use std::fs;
use std::path::Path;

use lexongraph_block::{BlockError, SerializedBlock, deserialize_block};
use lexongraph_block_store::BlockStoreError;
use lexongraph_indexer::{Indexer, IndexerError};
use thiserror::Error;

use crate::block_store::ConfiguredBlockStore;
use crate::config::{BatchItemConfig, BatchRequest, BatchSummary, ConfigError};
use crate::embedding::{ConfiguredEmbeddingProvider, ConfiguredEmbeddingProviderError};
use crate::mailbox::{MailboxExpansionError, expand_mailbox_item_with_stats};
use crate::paths::resolve_path;
use crate::resolver::LocalFilesystemContentResolver;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("failed to read request file {path}: {source}")]
    ReadRequest {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse request file {path}: {source}")]
    ParseRequest {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Provider(#[from] ConfiguredEmbeddingProviderError),
    #[error(transparent)]
    Mailbox(#[from] MailboxExpansionError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error("failed to deserialize staged block {block_id}: {source}")]
    DeserializeStagedBlock {
        block_id: String,
        #[source]
        source: BlockError,
    },
    #[error("staged block hash mismatch: expected {expected}, store returned {actual}")]
    StagedBlockHashMismatch { expected: String, actual: String },
    #[error(transparent)]
    Indexer(#[from] IndexerError),
    #[error("delegated indexing produced no blocks")]
    EmptyDelegatedOutput,
    #[error("failed to write batch summary {path}: {source}")]
    WriteSummary {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to render batch summary: {0}")]
    RenderSummary(#[from] serde_json::Error),
}

pub async fn run_request_file(request_path: &Path) -> Result<BatchSummary, RuntimeError> {
    let bytes = fs::read(request_path).map_err(|source| RuntimeError::ReadRequest {
        path: request_path.display().to_string(),
        source,
    })?;
    let request: BatchRequest =
        serde_json::from_slice(&bytes).map_err(|source| RuntimeError::ParseRequest {
            path: request_path.display().to_string(),
            source,
        })?;
    let request_dir = request_path.parent().unwrap_or_else(|| Path::new("."));

    run_request(request_dir, request).await
}

pub async fn run_request(
    request_dir: &Path,
    request: BatchRequest,
) -> Result<BatchSummary, RuntimeError> {
    run_request_with_progress(request_dir, request, |message| eprintln!("{message}")).await
}

async fn run_request_with_progress<F>(
    request_dir: &Path,
    request: BatchRequest,
    mut progress: F,
) -> Result<BatchSummary, RuntimeError>
where
    F: FnMut(String),
{
    request.validate()?;
    request.environment.local_embedding()?;
    let embedding_provider = ConfiguredEmbeddingProvider::from_environment(&request.environment)?;
    let block_store = ConfiguredBlockStore::from_environment(request_dir, &request.environment)?;
    let indexer = Indexer::with_defaults(LocalFilesystemContentResolver, embedding_provider);
    let embedding_spec = request.to_embedding_spec();
    let document_items = request.to_document_index_items(request_dir);
    let mut staged_blocks = Vec::new();
    let mut staged_block_ids = Vec::new();

    if !document_items.is_empty() {
        progress(format!(
            "Indexing {} document item(s)",
            document_items.len()
        ));
        let staged = indexer
            .build_leaf_blocks(&document_items, embedding_spec.clone())
            .await?;
        persist_staged_blocks(&staged.blocks, &block_store)?;
        progress(format!(
            "Indexed {} document item(s) into {} leaf block(s)",
            document_items.len(),
            staged.blocks.len()
        ));
        staged_block_ids.extend(staged.block_ids.iter().copied());
        staged_blocks.extend(staged.blocks);
    }

    for item in &request.items {
        if let BatchItemConfig::Mailbox { path, metadata } = item {
            let resolved = resolve_path(request_dir, path);
            progress(format!("Processing mailbox {}", resolved.display()));
            let expansion = expand_mailbox_item_with_stats(&resolved, metadata, &block_store)?;
            progress(format!(
                "Processed mailbox {}: {} message(s), {} delegated item(s)",
                resolved.display(),
                expansion.message_count,
                expansion.items.len()
            ));
            let staged = indexer
                .build_leaf_blocks(&expansion.items, embedding_spec.clone())
                .await?;
            persist_staged_blocks(&staged.blocks, &block_store)?;
            progress(format!(
                "Indexed {} delegated item(s) from mailbox {} into {} leaf block(s)",
                expansion.items.len(),
                resolved.display(),
                staged.blocks.len()
            ));
            staged_block_ids.extend(staged.block_ids.iter().copied());
            staged_blocks.extend(staged.blocks);
        }
    }

    let mut current_layer = unique_serialized_blocks_by_hash(staged_blocks);
    while current_layer.len() > 1 {
        let input_count = current_layer.len();
        let staged = indexer.build_parent_blocks(
            &current_layer,
            embedding_spec.clone(),
            request.block_size_target,
        )?;
        persist_staged_blocks(&staged.blocks, &block_store)?;
        let blocks_produced = staged.blocks.len();
        let next_layer = unique_serialized_blocks_by_hash(staged.blocks);
        progress(format!(
            "Indexed {} staged block(s) into {} parent block(s); {} staged block(s) remain",
            input_count,
            blocks_produced,
            next_layer.len()
        ));
        staged_block_ids.extend(staged.block_ids.iter().copied());
        current_layer = next_layer;
    }
    let root = current_layer
        .first()
        .ok_or(RuntimeError::EmptyDelegatedOutput)?
        .hash;
    staged_block_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    staged_block_ids.dedup_by(|left, right| left.as_bytes() == right.as_bytes());

    Ok(BatchSummary {
        root_id: root.to_string(),
        block_count: staged_block_ids.len(),
        block_ids: staged_block_ids
            .into_iter()
            .map(|block_id| block_id.to_string())
            .collect(),
    })
}

fn persist_staged_blocks(
    blocks: &[SerializedBlock],
    store: &dyn lexongraph_block_store::BlockStore,
) -> Result<(), RuntimeError> {
    for block in blocks {
        let validated = deserialize_block(&block.bytes, &block.hash).map_err(|source| {
            RuntimeError::DeserializeStagedBlock {
                block_id: block.hash.to_string(),
                source,
            }
        })?;
        let persisted = store.put(&validated.block)?;
        if persisted != block.hash {
            return Err(RuntimeError::StagedBlockHashMismatch {
                expected: block.hash.to_string(),
                actual: persisted.to_string(),
            });
        }
    }
    Ok(())
}

fn unique_serialized_blocks_by_hash(mut blocks: Vec<SerializedBlock>) -> Vec<SerializedBlock> {
    blocks.sort_by(|left, right| left.hash.as_bytes().cmp(right.hash.as_bytes()));
    blocks.dedup_by(|left, right| left.hash == right.hash);
    blocks
}

pub fn write_summary_file(path: &Path, summary: &BatchSummary) -> Result<(), RuntimeError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| RuntimeError::WriteSummary {
            path: path.display().to_string(),
            source,
        })?;
    }
    let rendered = serde_json::to_vec_pretty(summary)?;
    fs::write(path, rendered).map_err(|source| RuntimeError::WriteSummary {
        path: path.display().to_string(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::thread;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;

    use crate::config::{
        BatchItemConfig, EmbeddingSpecConfig, EnvironmentConfig, LocalEmbeddingConfig,
    };

    use super::*;

    #[tokio::test]
    async fn repeated_runs_are_idempotent_for_unchanged_content() {
        let temp = tempdir().unwrap();
        let mailbox_path = temp.path().join("2026-01.mbox");
        let document_path = temp.path().join("readme.txt");
        fs::write(
            &mailbox_path,
            b"From user@example.com Sat Jan 01 00:00:00 2026\nSubject: Hello\n\nBody\n",
        )
        .unwrap();
        fs::write(&document_path, b"document body\n").unwrap();

        let server = spawn_embedding_server(4);
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            items: vec![
                BatchItemConfig::Mailbox {
                    path: mailbox_path
                        .strip_prefix(temp.path())
                        .unwrap()
                        .to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Document {
                    path: document_path
                        .strip_prefix(temp.path())
                        .unwrap()
                        .to_path_buf(),
                    metadata: BTreeMap::new(),
                },
            ],
        };

        let first = run_request(temp.path(), request.clone()).await.unwrap();
        let stored_block_count_after_first = count_files_recursively(&temp.path().join("blocks"));
        let second = run_request(temp.path(), request).await.unwrap();
        let stored_block_count_after_second = count_files_recursively(&temp.path().join("blocks"));

        assert_eq!(first.root_id, second.root_id);
        assert_eq!(first.block_ids, second.block_ids);
        assert_eq!(
            stored_block_count_after_first,
            stored_block_count_after_second
        );
        assert!(stored_block_count_after_second > first.block_count);
        server.join();
    }

    #[tokio::test]
    async fn empty_local_embedding_base_url_is_rejected_as_config_error() {
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            items: vec![BatchItemConfig::Document {
                path: Path::new("doc.txt").to_path_buf(),
                metadata: BTreeMap::new(),
            }],
        };

        let error = run_request(Path::new("C:\\request-root"), request)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            RuntimeError::Config(ConfigError::MissingLocalEmbeddingBaseUrl)
        ));
    }

    #[tokio::test]
    async fn run_request_reports_progress_for_mailbox_processing_and_indexing() {
        let temp = tempdir().unwrap();
        let mailbox_path = temp.path().join("2026-04.mbox");
        let document_path = temp.path().join("notes.txt");
        fs::write(
            &mailbox_path,
            b"From user@example.com Sat Jan 01 00:00:00 2026\nSubject: Progress\n\nBody\n",
        )
        .unwrap();
        fs::write(&document_path, b"document body\n").unwrap();

        let server = spawn_embedding_server(2);
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            items: vec![
                BatchItemConfig::Mailbox {
                    path: mailbox_path
                        .strip_prefix(temp.path())
                        .unwrap()
                        .to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Document {
                    path: document_path
                        .strip_prefix(temp.path())
                        .unwrap()
                        .to_path_buf(),
                    metadata: BTreeMap::new(),
                },
            ],
        };

        let mut progress = Vec::new();
        let summary =
            run_request_with_progress(temp.path(), request, |message| progress.push(message))
                .await
                .unwrap();

        assert!(!summary.block_ids.is_empty());
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Indexing 1 document item(s)"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Processing mailbox"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Processed mailbox"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Indexed 1 delegated item(s) from mailbox"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("parent block(s); 1 staged block(s) remain"))
        );
        server.join();
    }

    struct TestServer {
        base_url: String,
        handle: thread::JoinHandle<()>,
    }

    impl TestServer {
        fn join(self) {
            self.handle.join().unwrap();
        }
    }

    fn request_is_complete(request: &[u8]) -> bool {
        let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            return false;
        };
        let body_start = header_end + 4;
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        });

        match content_length {
            Some(length) => request.len() >= body_start + length,
            None => true,
        }
    }

    fn count_files_recursively(root: &Path) -> usize {
        fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .map(|path| {
                if path.is_dir() {
                    count_files_recursively(&path)
                } else {
                    1
                }
            })
            .sum()
    }

    fn spawn_embedding_server(expected_requests: usize) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(AtomicUsize::new(0));
        let seen_for_thread = Arc::clone(&seen);
        let (ready_tx, ready_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let idle_after_expected = Duration::from_millis(200);
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut last_activity = Instant::now();
            while Instant::now() < deadline {
                if seen_for_thread.load(Ordering::SeqCst) >= expected_requests
                    && Instant::now().duration_since(last_activity) >= idle_after_expected
                {
                    break;
                }
                let (mut stream, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    Err(error) => panic!("failed to accept runtime test connection: {error}"),
                };
                last_activity = Instant::now();
                stream.set_nonblocking(true).unwrap();
                let mut request = Vec::new();
                let mut buffer = [0u8; 1024];
                let request_deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    if request_is_complete(&request) {
                        break;
                    }
                    if Instant::now() >= request_deadline {
                        panic!("timed out waiting for runtime test request body");
                    }
                    match stream.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(read) => {
                            request.extend_from_slice(&buffer[..read]);
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(error) if error.kind() == std::io::ErrorKind::TimedOut => break,
                        Err(error) => panic!("failed to read runtime test request: {error}"),
                    }
                }
                let body = r#"{"data":[{"embedding":[0.25,0.75]}]}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).unwrap();
                stream.flush().unwrap();
                seen_for_thread.fetch_add(1, Ordering::SeqCst);
            }
        });
        ready_rx.recv().unwrap();

        TestServer {
            base_url: format!("http://{}", address),
            handle,
        }
    }
}
