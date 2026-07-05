// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};

use lexonarchivebuilder_indexer::BatchSummary;
use lexonarchivebuilder_indexer::INGESTION_ONLY_ROOT_ID_PLACEHOLDER;
use lexonarchivebuilder_indexer::block_store::ConfiguredBlockStore;
use lexonarchivebuilder_indexer::embedding::ConfiguredEmbeddingProvider;
use lexonarchivebuilder_indexer::tree_tools::{
    metadata_values_to_text_map, parse_block_hash, search_with_partial_retry,
    source_name_from_metadata,
};
use lexongraph_block::{BlockHash, EmbeddingSpec};
use lexongraph_block_store::BlockStoreError;
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_search::{
    DefaultCandidateScorer, DefaultEmbeddingCompatibility, EncodedTargetEmbedding, SearchError,
    Searcher,
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
    #[error(
        "index summary {path} was produced by ingestion-only execution and does not contain a searchable root"
    )]
    IngestionOnlySummary { path: String },
    #[error(transparent)]
    Provider(#[from] lexonarchivebuilder_indexer::embedding::ConfiguredEmbeddingProviderError),
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
        Ok(Self {
            request_dir,
            config,
        })
    }

    pub async fn search_chunks(
        &self,
        request: SearchChunksRequest,
    ) -> Result<SearchChunksResponse, RuntimeError> {
        Self::search_chunks_with_context(self.request_dir.clone(), self.config.clone(), request)
            .await
    }

    pub(crate) fn search_chunks_blocking(
        &self,
        request: SearchChunksRequest,
    ) -> Result<SearchChunksResponse, RuntimeError> {
        let request_dir = self.request_dir.clone();
        let config = self.config.clone();
        Self::block_on_search_future(move || {
            Self::search_chunks_with_context(request_dir, config, request)
        })
    }

    async fn search_chunks_with_context(
        request_dir: PathBuf,
        config: McpConfig,
        request: SearchChunksRequest,
    ) -> Result<SearchChunksResponse, RuntimeError> {
        let top_k = request.top_k.unwrap_or(config.top_k);
        if top_k == 0 {
            return Err(RuntimeError::InvalidTopK);
        }
        let traversal_width = request.traversal_width.unwrap_or(config.traversal_width);
        if traversal_width == 0 {
            return Err(RuntimeError::InvalidTraversalWidth);
        }

        let root_id = resolve_root_id_async(&request_dir, &config).await?;
        let embedding_spec: EmbeddingSpec = (&config.embedding_spec).into();
        let embedding_provider =
            ConfiguredEmbeddingProvider::from_environment(&config.environment)?;
        let block_store =
            ConfiguredBlockStore::from_environment(&request_dir, &config.environment)?;
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
        let root_id_text = root_id.to_string();
        let searcher = Searcher::new(DefaultEmbeddingCompatibility, DefaultCandidateScorer);
        let result = search_with_partial_retry(
            &searcher,
            &root_id,
            &target,
            traversal_width,
            top_k,
            &block_store,
        )
        .await?;

        Ok(SearchChunksResponse {
            root_id: root_id_text,
            top_k,
            traversal_width,
            results: result
                .leaves
                .into_iter()
                .map(|leaf| {
                    let metadata = metadata_values_to_text_map(&leaf.entry.metadata);
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

    fn block_on_search_future<F, Fut, T>(make_future: F) -> T
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = T>,
        T: Send,
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            match handle.runtime_flavor() {
                tokio::runtime::RuntimeFlavor::MultiThread => {
                    tokio::task::block_in_place(|| handle.block_on(make_future()))
                }
                tokio::runtime::RuntimeFlavor::CurrentThread => std::thread::scope(|scope| {
                    scope
                        .spawn(|| {
                            tokio::runtime::Builder::new_current_thread()
                                .enable_all()
                                .build()
                                .expect("failed to build tokio runtime for MCP search bridge")
                                .block_on(make_future())
                        })
                        .join()
                        .expect("MCP search bridge thread panicked")
                }),
                _ => unreachable!("unsupported tokio runtime flavor"),
            }
        } else {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime for MCP search bridge")
                .block_on(make_future())
        }
    }

    pub fn get_email(&self, request: NamedRetrievalRequest) -> NamedRetrievalResponse {
        unsupported_named_retrieval(NamedItemKind::Email, request.name)
    }

    pub fn get_thread(&self, request: NamedRetrievalRequest) -> NamedRetrievalResponse {
        unsupported_named_retrieval(NamedItemKind::Thread, request.name)
    }
}

async fn resolve_root_id_async(
    request_dir: &Path,
    config: &McpConfig,
) -> Result<BlockHash, RuntimeError> {
    let root_literal = if let Some(root_id) = config.root_id_literal() {
        root_id.to_string()
    } else {
        let summary_path = config
            .resolve_summary_path(request_dir)
            .expect("summary path must exist when root_id literal is absent");
        let bytes =
            tokio::fs::read(&summary_path)
                .await
                .map_err(|source| RuntimeError::ReadSummary {
                    path: summary_path.display().to_string(),
                    source,
                })?;
        let summary: BatchSummary =
            serde_json::from_slice(&bytes).map_err(|source| RuntimeError::ParseSummary {
                path: summary_path.display().to_string(),
                source,
            })?;
        if summary.root_id == INGESTION_ONLY_ROOT_ID_PLACEHOLDER {
            return Err(RuntimeError::IngestionOnlySummary {
                path: summary_path.display().to_string(),
            });
        }
        summary.root_id
    };

    parse_block_hash(&root_literal).map_err(|_| RuntimeError::InvalidRootId {
        value: root_literal,
    })
}

fn unsupported_named_retrieval(kind: NamedItemKind, name: String) -> NamedRetrievalResponse {
    NamedRetrievalResponse {
        kind,
        name,
        status: NamedRetrievalStatus::Unsupported,
        message: "Named retrieval remains unavailable in the first MVP because the delegated LexonGraph retrieval-by-name contract is not yet implemented.".into(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::thread;
    use std::time::{Duration, Instant};

    use lexonarchivebuilder_indexer::config::{
        BatchItemConfig, BatchRequest, EmbeddingSpecConfig, EnvironmentConfig, ExecutionStage,
        LocalEmbeddingConfig,
    };
    use lexonarchivebuilder_indexer::{run_request, write_summary_file};
    use tempfile::tempdir;

    use super::*;
    use crate::config::IndexConfig;

    #[tokio::test(flavor = "multi_thread")]
    async fn search_chunks_returns_indexed_chunk_content_from_local_profile() {
        let temp = tempdir().unwrap();
        let document_path = temp.path().join("overview.txt");
        fs::write(
            &document_path,
            b"LexonArchiveBuilder MCP runtime document body\n",
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
            stage: ExecutionStage::FullPipeline,
            profile_version: lexonarchivebuilder_indexer::config::PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: None,
            ref_name: "test-branch".into(),
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
        assert!(response.results.iter().any(|hit| {
            hit.text
                .contains("LexonArchiveBuilder MCP runtime document body")
        }));
        assert!(response.results.iter().any(|hit| {
            hit.source_path
                .as_deref()
                .is_some_and(|path| path.ends_with("overview.txt"))
        }));
        server.join();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn search_chunks_surfaces_email_chunk_provenance_metadata() {
        let temp = tempdir().unwrap();
        let mailbox_path = temp.path().join("2026-01.mbox");
        fs::write(
            &mailbox_path,
            concat!(
                "From alan@example.com Sat Jan 03 10:00:00 2026\n",
                "Subject: LexonArchiveBuilder mail chunk\n",
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
            stage: ExecutionStage::FullPipeline,
            profile_version: lexonarchivebuilder_indexer::config::PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: None,
            ref_name: "test-branch".into(),
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
            Some(&"LexonArchiveBuilder mail chunk".to_string())
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
            RuntimeError::Config(crate::config::ConfigError::IndexerConfig(
                lexonarchivebuilder_indexer::config::ConfigError::MissingLocalEmbeddingBaseUrl
            ))
        ));
    }

    #[test]
    fn ingestion_only_summary_file_is_rejected_explicitly() {
        let temp = tempdir().unwrap();
        let summary_path = temp.path().join("summary.json");
        write_summary_file(
            &summary_path,
            &BatchSummary {
                root_id: lexonarchivebuilder_indexer::INGESTION_ONLY_ROOT_ID_PLACEHOLDER.into(),
                block_ids: vec![],
                block_count: 0,
            },
        )
        .unwrap();

        let runtime = McpRuntime::new(
            temp.path().to_path_buf(),
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
                index: IndexConfig::SummaryFile {
                    path: PathBuf::from("summary.json"),
                },
                top_k: 1,
                traversal_width: 1,
            },
        )
        .unwrap();

        assert!(matches!(
            McpRuntime::block_on_search_future(|| resolve_root_id_async(
                &runtime.request_dir,
                &runtime.config
            )),
            Err(RuntimeError::IngestionOnlySummary { .. })
        ));
    }

    struct TestServer {
        base_url: String,
        expected_requests: usize,
        seen: Arc<AtomicUsize>,
        shutdown: Arc<AtomicBool>,
        handle: thread::JoinHandle<()>,
    }

    impl TestServer {
        fn join(self) {
            self.shutdown.store(true, Ordering::SeqCst);
            self.handle.join().unwrap();
            assert!(
                self.seen.load(Ordering::SeqCst) >= self.expected_requests,
                "expected at least {} embedding request(s), saw {}",
                self.expected_requests,
                self.seen.load(Ordering::SeqCst)
            );
        }
    }

    fn spawn_embedding_server(expected_requests: usize) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let seen_for_thread = Arc::clone(&seen);
        let shutdown_for_thread = Arc::clone(&shutdown);
        let handle = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(15);
            while !shutdown_for_thread.load(Ordering::SeqCst) && Instant::now() < deadline {
                let (mut stream, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    Err(error) => panic!("failed to accept MCP runtime test connection: {error}"),
                };
                stream
                    .set_read_timeout(Some(Duration::from_millis(200)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0u8; 1024];
                let expected_len = loop {
                    match stream.read(&mut buffer) {
                        Ok(0) => break None,
                        Ok(read) => {
                            request.extend_from_slice(&buffer[..read]);
                            if let Some(header_end) = request
                                .windows(4)
                                .position(|window| window == b"\r\n\r\n")
                                .map(|index| index + 4)
                            {
                                let header_text =
                                    String::from_utf8_lossy(&request[..header_end]).to_lowercase();
                                let content_length = header_text
                                    .lines()
                                    .find_map(|line| {
                                        line.strip_prefix("content-length:")
                                            .and_then(|value| value.trim().parse::<usize>().ok())
                                    })
                                    .unwrap_or(0);
                                break Some(header_end + content_length);
                            }
                        }
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                            ) =>
                        {
                            break None;
                        }
                        Err(error) => panic!("failed to read MCP runtime test request: {error}"),
                    }
                };
                let Some(expected_len) = expected_len else {
                    continue;
                };
                while request.len() < expected_len {
                    match stream.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(read) => request.extend_from_slice(&buffer[..read]),
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                            ) =>
                        {
                            panic!("timed out before receiving the full MCP runtime test request")
                        }
                        Err(error) => panic!("failed to read MCP runtime test request: {error}"),
                    }
                }
                if request.len() < expected_len {
                    break;
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
            expected_requests,
            seen,
            shutdown,
            handle,
        }
    }
}
