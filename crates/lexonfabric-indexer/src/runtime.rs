use std::fs;
use std::path::Path;

use lexongraph_indexer::{Indexer, IndexerError};
use thiserror::Error;

use crate::block_store::ConfiguredBlockStore;
use crate::config::{BatchRequest, BatchSummary, ConfigError};
use crate::embedding::{ConfiguredEmbeddingProvider, ConfiguredEmbeddingProviderError};
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
    Indexer(#[from] IndexerError),
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
    let items = request.to_index_items(request_dir)?;
    request.environment.local_embedding()?;
    let embedding_provider = ConfiguredEmbeddingProvider::from_environment(&request.environment)?;
    let block_store = ConfiguredBlockStore::from_environment(request_dir, &request.environment);
    let indexer = Indexer::with_defaults(LocalFilesystemContentResolver, embedding_provider);
    let result = indexer
        .index(
            &items,
            request.to_embedding_spec(),
            request.block_size_target,
            &block_store,
        )
        .await?;

    Ok(BatchSummary {
        root_id: result.root_id.to_string(),
        block_count: result.block_ids.len(),
        block_ids: result
            .block_ids
            .into_iter()
            .map(|block_id| block_id.to_string())
            .collect(),
    })
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
        let second = run_request(temp.path(), request).await.unwrap();

        assert_eq!(first.root_id, second.root_id);
        assert_eq!(first.block_ids, second.block_ids);
        assert_eq!(
            fs::read_dir(temp.path().join("blocks")).unwrap().count(),
            first.block_count
        );
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

    struct TestServer {
        base_url: String,
        handle: thread::JoinHandle<()>,
    }

    impl TestServer {
        fn join(self) {
            self.handle.join().unwrap();
        }
    }

    fn spawn_embedding_server(expected_requests: usize) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(AtomicUsize::new(0));
        let seen_for_thread = Arc::clone(&seen);
        let handle = thread::spawn(move || {
            let idle_after_expected = Duration::from_millis(200);
            let deadline = Instant::now() + Duration::from_secs(5);
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
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut buffer = [0u8; 1024];
                let _ = stream.read(&mut buffer);
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

        TestServer {
            base_url: format!("http://{}", address),
            handle,
        }
    }
}
