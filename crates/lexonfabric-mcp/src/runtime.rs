use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use ciborium::Value;
use lexonfabric_indexer::BatchSummary;
use lexonfabric_indexer::block_store::ConfiguredBlockStore;
use lexonfabric_indexer::config::ConfigError as IndexerConfigError;
use lexonfabric_indexer::embedding::ConfiguredEmbeddingProvider;
use lexongraph_block::{BlockHash, EmbeddingSpec};
use lexongraph_block_store::BlockStoreError;
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_search::{
    DefaultCandidateScorer, DefaultEmbeddingCompatibility, EncodedTargetEmbedding, SearchError,
    SearchResult, Searcher,
};
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::{ConfigError, McpConfig};

#[derive(Clone, Debug)]
pub struct McpRuntime {
    request_dir: PathBuf,
    config: McpConfig,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct SearchChunksRequest {
    pub query: String,
    #[serde(default)]
    pub top_k: Option<usize>,
    #[serde(default)]
    pub traversal_width: Option<usize>,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct SearchChunksResponse {
    pub root_id: String,
    pub top_k: usize,
    pub traversal_width: usize,
    pub results: Vec<SearchChunkHit>,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct SearchChunkHit {
    pub position: usize,
    pub leaf_block_id: String,
    pub media_type: String,
    pub text: String,
    pub metadata: BTreeMap<String, String>,
    pub source_kind: Option<String>,
    pub source_path: Option<String>,
    pub source_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct NamedRetrievalRequest {
    pub name: String,
}

#[derive(Clone, Copy, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum NamedItemKind {
    Document,
    Email,
    Thread,
}

#[derive(Clone, Copy, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum NamedRetrievalStatus {
    Unsupported,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct NamedRetrievalResponse {
    pub kind: NamedItemKind,
    pub name: String,
    pub status: NamedRetrievalStatus,
    pub message: String,
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("failed to read MCP config {path}: {source}")]
    ReadConfig {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse MCP config {path}: {source}")]
    ParseConfig {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    IndexerConfig(#[from] IndexerConfigError),
    #[error("top_k must be at least 1")]
    InvalidTopK,
    #[error("traversal_width must be at least 1")]
    InvalidTraversalWidth,
    #[error("failed to read index summary {path}: {source}")]
    ReadSummary {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse index summary {path}: {source}")]
    ParseSummary {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to parse root_id {value}")]
    InvalidRootId { value: String },
    #[error(transparent)]
    Provider(#[from] lexonfabric_indexer::embedding::ConfiguredEmbeddingProviderError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    Search(#[from] SearchError),
}

impl McpRuntime {
    pub fn from_config_file(config_path: &Path) -> Result<Self, RuntimeError> {
        let bytes = fs::read(config_path).map_err(|source| RuntimeError::ReadConfig {
            path: config_path.display().to_string(),
            source,
        })?;
        let config: McpConfig =
            serde_json::from_slice(&bytes).map_err(|source| RuntimeError::ParseConfig {
                path: config_path.display().to_string(),
                source,
            })?;
        let request_dir = config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();

        Self::new(request_dir, config)
    }

    pub fn new(request_dir: PathBuf, config: McpConfig) -> Result<Self, RuntimeError> {
        config.validate()?;
        config.environment.local_embedding()?;
        Ok(Self {
            request_dir,
            config,
        })
    }

    pub async fn search_chunks(
        &self,
        request: SearchChunksRequest,
    ) -> Result<SearchChunksResponse, RuntimeError> {
        let top_k = request.top_k.unwrap_or(self.config.top_k);
        if top_k == 0 {
            return Err(RuntimeError::InvalidTopK);
        }
        let traversal_width = request
            .traversal_width
            .unwrap_or(self.config.traversal_width);
        if traversal_width == 0 {
            return Err(RuntimeError::InvalidTraversalWidth);
        }

        let root_id = self.resolve_root_id()?;
        let embedding_spec: EmbeddingSpec = (&self.config.embedding_spec).into();
        let embedding_provider =
            ConfiguredEmbeddingProvider::from_environment(&self.config.environment)?;
        let block_store =
            ConfiguredBlockStore::from_environment(&self.request_dir, &self.config.environment)?;
        let target_embedding = embedding_provider
            .embed(
                &EmbeddingInput {
                    media_type: "text/plain".into(),
                    body: request.query.into_bytes(),
                },
                &embedding_spec,
            )
            .await?;
        let target = EncodedTargetEmbedding::new(target_embedding, embedding_spec);
        let searcher = Searcher::new(DefaultEmbeddingCompatibility, DefaultCandidateScorer);
        let result = search_with_partial_retry(
            &searcher,
            &root_id,
            &target,
            traversal_width,
            top_k,
            &block_store,
        )?;

        Ok(SearchChunksResponse {
            root_id: root_id.to_string(),
            top_k,
            traversal_width,
            results: result
                .leaves
                .into_iter()
                .map(|leaf| {
                    let metadata = metadata_to_text_map(&leaf.entry.metadata);
                    SearchChunkHit {
                        position: leaf.position,
                        leaf_block_id: leaf.leaf_block_id.to_string(),
                        media_type: leaf.entry.content.media_type,
                        text: String::from_utf8_lossy(&leaf.entry.content.body).into_owned(),
                        source_kind: metadata.get("source_kind").cloned(),
                        source_path: metadata.get("source_path").cloned(),
                        source_name: source_name_from_metadata(&metadata),
                        metadata,
                    }
                })
                .collect(),
        })
    }

    pub fn get_document(&self, request: NamedRetrievalRequest) -> NamedRetrievalResponse {
        unsupported_named_retrieval(NamedItemKind::Document, request.name)
    }

    pub fn get_email(&self, request: NamedRetrievalRequest) -> NamedRetrievalResponse {
        unsupported_named_retrieval(NamedItemKind::Email, request.name)
    }

    pub fn get_thread(&self, request: NamedRetrievalRequest) -> NamedRetrievalResponse {
        unsupported_named_retrieval(NamedItemKind::Thread, request.name)
    }

    fn resolve_root_id(&self) -> Result<BlockHash, RuntimeError> {
        let root_literal = if let Some(root_id) = self.config.root_id_literal() {
            root_id.to_string()
        } else {
            let summary_path = self
                .config
                .resolve_summary_path(&self.request_dir)
                .expect("summary path must exist when root_id literal is absent");
            let bytes = fs::read(&summary_path).map_err(|source| RuntimeError::ReadSummary {
                path: summary_path.display().to_string(),
                source,
            })?;
            let summary: BatchSummary =
                serde_json::from_slice(&bytes).map_err(|source| RuntimeError::ParseSummary {
                    path: summary_path.display().to_string(),
                    source,
                })?;
            summary.root_id
        };

        parse_block_hash(&root_literal).map_err(|_| RuntimeError::InvalidRootId {
            value: root_literal,
        })
    }
}

fn search_with_partial_retry(
    searcher: &Searcher<DefaultEmbeddingCompatibility, DefaultCandidateScorer>,
    root_id: &BlockHash,
    target: &EncodedTargetEmbedding,
    traversal_width: usize,
    top_k: usize,
    store: &dyn lexongraph_block_store::BlockStore,
) -> Result<SearchResult, SearchError> {
    match searcher.search(root_id, target, traversal_width, top_k, store) {
        Ok(result) => Ok(result),
        Err(SearchError::Exhausted {
            reachable_leaves, ..
        }) if reachable_leaves > 0 => {
            searcher.search(root_id, target, traversal_width, reachable_leaves, store)
        }
        Err(error) => Err(error),
    }
}

fn metadata_to_text_map(metadata: &[(Value, Value)]) -> BTreeMap<String, String> {
    metadata
        .iter()
        .filter_map(|(key, value)| match (key, value) {
            (Value::Text(key), Value::Text(value)) => Some((key.clone(), value.clone())),
            _ => None,
        })
        .collect()
}

fn source_name_from_metadata(metadata: &BTreeMap<String, String>) -> Option<String> {
    [
        "source_name",
        "document_name",
        "email_name",
        "thread_name",
        "name",
    ]
    .iter()
    .find_map(|key| metadata.get(*key).cloned())
}

fn unsupported_named_retrieval(kind: NamedItemKind, name: String) -> NamedRetrievalResponse {
    NamedRetrievalResponse {
        kind,
        name,
        status: NamedRetrievalStatus::Unsupported,
        message: "Named retrieval remains unavailable in the first MVP because the delegated LexonGraph retrieval-by-name contract is not yet implemented.".into(),
    }
}

fn parse_block_hash(value: &str) -> Result<BlockHash, ()> {
    if value.len() != BlockHash::LEN * 2 {
        return Err(());
    }

    let mut bytes = [0u8; BlockHash::LEN];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(chunk[0])?;
        let low = decode_hex_nibble(chunk[1])?;
        bytes[index] = (high << 4) | low;
    }

    Ok(BlockHash::from_bytes(bytes))
}

fn decode_hex_nibble(byte: u8) -> Result<u8, ()> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(()),
    }
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

    use lexonfabric_indexer::config::{
        BatchItemConfig, BatchRequest, EmbeddingSpecConfig, EnvironmentConfig, LocalEmbeddingConfig,
    };
    use lexonfabric_indexer::{run_request, write_summary_file};
    use tempfile::tempdir;

    use super::*;
    use crate::config::IndexConfig;

    #[tokio::test]
    async fn search_chunks_returns_indexed_chunk_content_from_local_profile() {
        let temp = tempdir().unwrap();
        let document_path = temp.path().join("overview.txt");
        fs::write(&document_path, b"LexonFabric MCP runtime document body\n").unwrap();

        let server = spawn_embedding_server(2);
        let batch_request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: PathBuf::from("block-store"),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 5,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            items: vec![BatchItemConfig::Document {
                path: document_path
                    .strip_prefix(temp.path())
                    .unwrap()
                    .to_path_buf(),
                metadata: BTreeMap::from([("collection".into(), "docs".into())]),
            }],
        };
        let summary = run_request(temp.path(), batch_request).await.unwrap();
        let summary_path = temp.path().join("summary.json");
        write_summary_file(&summary_path, &summary).unwrap();

        let runtime = McpRuntime::new(
            temp.path().to_path_buf(),
            McpConfig {
                environment: EnvironmentConfig::Local {
                    block_store_root: PathBuf::from("block-store"),
                    embedding: LocalEmbeddingConfig {
                        base_url: server.base_url.clone(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 5,
                        retry_delay_ms: 1,
                    },
                },
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                index: IndexConfig::SummaryFile {
                    path: PathBuf::from("summary.json"),
                },
                top_k: 5,
                traversal_width: 3,
            },
        )
        .unwrap();

        let response = runtime
            .search_chunks(SearchChunksRequest {
                query: "runtime document".into(),
                top_k: None,
                traversal_width: None,
            })
            .await
            .unwrap();

        assert_eq!(response.root_id, summary.root_id);
        assert_eq!(response.top_k, 5);
        assert!(!response.results.is_empty());
        assert!(
            response
                .results
                .iter()
                .any(|hit| hit.text.contains("LexonFabric MCP runtime document body"))
        );
        assert!(response.results.iter().any(|hit| {
            hit.source_path
                .as_deref()
                .is_some_and(|path| path.ends_with("overview.txt"))
        }));
        server.join();
    }

    #[tokio::test]
    async fn search_chunks_surfaces_email_chunk_provenance_metadata() {
        let temp = tempdir().unwrap();
        let mailbox_path = temp.path().join("2026-01.mbox");
        fs::write(
            &mailbox_path,
            concat!(
                "From alan@example.com Sat Jan 03 10:00:00 2026\n",
                "Subject: LexonFabric mail chunk\n",
                "From: Alan Example <alan@example.com>\n",
                "To: team@example.com\n",
                "Message-ID: <chunk-1@example.com>\n",
                "\n",
                "This searchable email body should surface provenance metadata.\n"
            ),
        )
        .unwrap();

        let server = spawn_embedding_server(2);
        let batch_request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: PathBuf::from("block-store"),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 5,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            items: vec![BatchItemConfig::Mailbox {
                path: mailbox_path
                    .strip_prefix(temp.path())
                    .unwrap()
                    .to_path_buf(),
                metadata: BTreeMap::from([("month".into(), "2026-01".into())]),
            }],
        };
        let summary = run_request(temp.path(), batch_request).await.unwrap();
        let summary_path = temp.path().join("summary.json");
        write_summary_file(&summary_path, &summary).unwrap();

        let runtime = McpRuntime::new(
            temp.path().to_path_buf(),
            McpConfig {
                environment: EnvironmentConfig::Local {
                    block_store_root: PathBuf::from("block-store"),
                    embedding: LocalEmbeddingConfig {
                        base_url: server.base_url.clone(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 5,
                        retry_delay_ms: 1,
                    },
                },
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                index: IndexConfig::SummaryFile {
                    path: PathBuf::from("summary.json"),
                },
                top_k: 3,
                traversal_width: 2,
            },
        )
        .unwrap();

        let response = runtime
            .search_chunks(SearchChunksRequest {
                query: "searchable provenance".into(),
                top_k: None,
                traversal_width: None,
            })
            .await
            .unwrap();

        assert_eq!(response.root_id, summary.root_id);
        let hit = response
            .results
            .iter()
            .find(|hit| hit.text.contains("searchable email body"))
            .expect("expected mailbox-derived chunk hit");
        assert_eq!(hit.source_kind.as_deref(), Some("email"));
        assert!(hit.metadata.contains_key("email_artifact_ref"));
        assert!(hit.metadata.contains_key("mailbox_artifact_ref"));
        assert!(hit.metadata.contains_key("chunk_locator"));
        assert_eq!(
            hit.metadata.get("email_subject"),
            Some(&"LexonFabric mail chunk".to_string())
        );
        server.join();
    }

    #[test]
    fn named_retrieval_operations_return_explicit_unsupported_outcome() {
        let runtime = McpRuntime::new(
            PathBuf::from("C:\\request-root"),
            McpConfig {
                environment: EnvironmentConfig::Local {
                    block_store_root: PathBuf::from("block-store"),
                    embedding: LocalEmbeddingConfig {
                        base_url: "http://localhost:8080".into(),
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
                index: IndexConfig::RootId {
                    root_id: "4c33a6fc7cac4679c0a1f57d40203a28e997c3a92783abf4dc0f7162d36f856e"
                        .into(),
                },
                top_k: 1,
                traversal_width: 1,
            },
        )
        .unwrap();

        let document = runtime.get_document(NamedRetrievalRequest {
            name: "overview.txt".into(),
        });
        let email = runtime.get_email(NamedRetrievalRequest {
            name: "hello@example.com".into(),
        });
        let thread = runtime.get_thread(NamedRetrievalRequest {
            name: "thread-1".into(),
        });

        assert!(matches!(document.status, NamedRetrievalStatus::Unsupported));
        assert!(matches!(email.status, NamedRetrievalStatus::Unsupported));
        assert!(matches!(thread.status, NamedRetrievalStatus::Unsupported));
    }

    #[test]
    fn empty_local_embedding_base_url_is_rejected_at_runtime_construction() {
        let error = McpRuntime::new(
            PathBuf::from("C:\\request-root"),
            McpConfig {
                environment: EnvironmentConfig::Local {
                    block_store_root: PathBuf::from("block-store"),
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
                index: IndexConfig::RootId {
                    root_id: "4c33a6fc7cac4679c0a1f57d40203a28e997c3a92783abf4dc0f7162d36f856e"
                        .into(),
                },
                top_k: 1,
                traversal_width: 1,
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            RuntimeError::IndexerConfig(IndexerConfigError::MissingLocalEmbeddingBaseUrl)
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
                    Err(error) => panic!("failed to accept MCP runtime test connection: {error}"),
                };
                last_activity = Instant::now();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0u8; 1024];
                loop {
                    match stream.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(read) => {
                            request.extend_from_slice(&buffer[..read]);
                        }
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                            ) =>
                        {
                            break;
                        }
                        Err(error) => panic!("failed to read MCP runtime test request: {error}"),
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

        TestServer {
            base_url: format!("http://{}", address),
            handle,
        }
    }
}
