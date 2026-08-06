// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};

use lexonarchivebuilder_indexer::BatchSummary;
use lexonarchivebuilder_indexer::INGESTION_ONLY_ROOT_ID_PLACEHOLDER;
use lexonarchivebuilder_indexer::block_store::ConfiguredBlockStore;
use lexonarchivebuilder_indexer::embedding::{
    ConfiguredEmbeddingProvider, decode_logical_f32_embedding, logical_f32_embedding_spec,
};
use lexonarchivebuilder_indexer::tree_tools::{
    metadata_values_to_text_map, parse_block_hash, search_with_partial_retry,
    source_name_from_metadata,
};
use lexongraph_block::{Block, BlockHash, LeafEntry};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_search::{
    DefaultCandidateScorer, DefaultEmbeddingCompatibility, SearchError, Searcher,
    prepare_target_embedding,
};
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::{
    ConfigError, DEFAULT_GATEWAY_HTTP3_FS_CACHE_ROOT, McpConfig, McpEnvironmentConfig,
};

#[derive(Clone, Debug)]
pub struct McpRuntime {
    request_dir: PathBuf,
    config: McpConfig,
    block_store: ConfiguredBlockStore,
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

#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct EmailRetrievalResponse {
    pub leaf_block_id: String,
    pub entry: SearchChunkHit,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
pub(crate) struct CacheStats {
    pub layers: Vec<CacheStatsLayer>,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
pub(crate) struct CacheStatsLayer {
    pub layer_index: usize,
    pub role: String,
    pub hits: u64,
    pub misses: u64,
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
    #[error("failed to parse email leaf_block_id {value}")]
    InvalidEmailLeafBlockId { value: String },
    #[error(
        "index summary {path} was produced by ingestion-only execution and does not contain a searchable root"
    )]
    IngestionOnlySummary { path: String },
    #[error(transparent)]
    Provider(#[from] lexonarchivebuilder_indexer::embedding::ConfiguredEmbeddingProviderError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error("root block {root_id} was not found")]
    MissingRootBlock { root_id: String },
    #[error("root block {root_id} is a leaf and cannot be searched by the format-neutral MCP path")]
    LeafRoot { root_id: String },
    #[error("email leaf block {leaf_block_id} was not found")]
    MissingEmailLeafBlock { leaf_block_id: String },
    #[error("email block {block_id} is not a leaf")]
    EmailBlockIsNotLeaf { block_id: String },
    #[error("email leaf block {leaf_block_id} must contain exactly one entry, found {entry_count}")]
    EmailLeafEntryCount {
        leaf_block_id: String,
        entry_count: usize,
    },
    #[error("email leaf block {leaf_block_id} contains no email entries")]
    EmailLeafContainsNoEmailEntries { leaf_block_id: String },
    #[error("failed to prepare rooted search target: {message}")]
    TargetPreparation { message: String },
    #[error("cache statistics layer layout changed during an MCP operation")]
    CacheStatisticsLayoutChanged,
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
        let block_store = configured_block_store(&request_dir, &config.environment)?;
        Ok(Self {
            request_dir,
            config,
            block_store,
        })
    }

    pub(crate) fn tool_description(&self, tool_name: &str) -> &str {
        self.config.tool_description(tool_name)
    }

    pub(crate) fn cache_stats(&self) -> Option<CacheStats> {
        self.block_store.cache_stats().map(|stats| CacheStats {
            layers: stats
                .layers
                .into_iter()
                .map(|layer| CacheStatsLayer {
                    layer_index: layer.layer_index,
                    role: layer.role,
                    hits: layer.hits,
                    misses: layer.misses,
                })
                .collect(),
        })
    }

    pub(crate) fn cache_stats_delta(
        &self,
        before: Option<CacheStats>,
    ) -> Result<Option<CacheStats>, RuntimeError> {
        let Some(before) = before else {
            return Ok(None);
        };
        let Some(after) = self.cache_stats() else {
            return Err(RuntimeError::CacheStatisticsLayoutChanged);
        };
        if before.layers.len() != after.layers.len() {
            return Err(RuntimeError::CacheStatisticsLayoutChanged);
        }

        let mut layers = Vec::with_capacity(after.layers.len());
        for (before_layer, after_layer) in before.layers.into_iter().zip(after.layers) {
            if before_layer.layer_index != after_layer.layer_index
                || before_layer.role != after_layer.role
            {
                return Err(RuntimeError::CacheStatisticsLayoutChanged);
            }
            let Some(hits) = after_layer.hits.checked_sub(before_layer.hits) else {
                return Err(RuntimeError::CacheStatisticsLayoutChanged);
            };
            let Some(misses) = after_layer.misses.checked_sub(before_layer.misses) else {
                return Err(RuntimeError::CacheStatisticsLayoutChanged);
            };
            layers.push(CacheStatsLayer {
                layer_index: after_layer.layer_index,
                role: after_layer.role,
                hits,
                misses,
            });
        }
        Ok(Some(CacheStats { layers }))
    }

    pub async fn search_chunks(
        &self,
        request: SearchChunksRequest,
    ) -> Result<SearchChunksResponse, RuntimeError> {
        Self::search_chunks_with_context(
            self.request_dir.clone(),
            self.config.clone(),
            self.block_store.clone(),
            request,
        )
        .await
    }

    pub(crate) fn search_chunks_blocking(
        &self,
        request: SearchChunksRequest,
    ) -> Result<SearchChunksResponse, RuntimeError> {
        let request_dir = self.request_dir.clone();
        let config = self.config.clone();
        let block_store = self.block_store.clone();
        Self::block_on_search_future(move || {
            Self::search_chunks_with_context(request_dir, config, block_store, request)
        })
    }

    async fn search_chunks_with_context(
        request_dir: PathBuf,
        config: McpConfig,
        block_store: ConfiguredBlockStore,
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
        let Some(root) = block_store.get(&root_id).await? else {
            return Err(RuntimeError::MissingRootBlock {
                root_id: root_id.to_string(),
            });
        };
        let Block::Branch(branch) = &root.block else {
            return Err(RuntimeError::LeafRoot {
                root_id: root_id.to_string(),
            });
        };
        let embedding_provider = configured_embedding_provider(&config.environment)?;
        let provider_spec = logical_f32_embedding_spec(branch.embedding_spec.dims);
        let target_embedding = embedding_provider
            .embed(
                &EmbeddingInput {
                    media_type: "text/plain".into(),
                    body: request.query.into_bytes(),
                },
                &provider_spec,
            )
            .await?;
        let logical_embedding =
            decode_logical_f32_embedding(&target_embedding, provider_spec.dims)?;
        let target = prepare_target_embedding(&root, &logical_embedding)
            .map_err(|error| RuntimeError::TargetPreparation {
                message: error.to_string(),
            })?
            .target;
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
                    project_leaf_entry(&leaf.leaf_block_id, leaf.position, &leaf.entry, metadata)
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

    pub async fn get_email(
        &self,
        request: NamedRetrievalRequest,
    ) -> Result<EmailRetrievalResponse, RuntimeError> {
        let leaf_block_id =
            parse_block_hash(&request.name).map_err(|_| RuntimeError::InvalidEmailLeafBlockId {
                value: request.name.clone(),
            })?;
        let Some(block) = self.block_store.get(&leaf_block_id).await? else {
            return Err(RuntimeError::MissingEmailLeafBlock {
                leaf_block_id: leaf_block_id.to_string(),
            });
        };
        let Block::Leaf(leaf) = block.block else {
            return Err(RuntimeError::EmailBlockIsNotLeaf {
                block_id: leaf_block_id.to_string(),
            });
        };
        let leaf_block_id_text = leaf_block_id.to_string();
        let entry = sole_email_leaf_entry(&leaf_block_id_text, &leaf.entries)?;
        let metadata = metadata_values_to_text_map(&entry.metadata);
        if metadata.get("source_kind").map(String::as_str) != Some("email") {
            return Err(RuntimeError::EmailLeafContainsNoEmailEntries {
                leaf_block_id: leaf_block_id_text,
            });
        }

        Ok(EmailRetrievalResponse {
            leaf_block_id: leaf_block_id_text,
            entry: project_leaf_entry(&leaf_block_id, 0, entry, metadata),
        })
    }

    pub fn get_thread(&self, request: NamedRetrievalRequest) -> NamedRetrievalResponse {
        unsupported_named_retrieval(NamedItemKind::Thread, request.name)
    }
}

fn sole_email_leaf_entry<'a>(
    leaf_block_id: &str,
    entries: &'a [LeafEntry],
) -> Result<&'a LeafEntry, RuntimeError> {
    let [entry] = entries else {
        return Err(RuntimeError::EmailLeafEntryCount {
            leaf_block_id: leaf_block_id.into(),
            entry_count: entries.len(),
        });
    };
    Ok(entry)
}

fn project_leaf_entry(
    leaf_block_id: &BlockHash,
    position: usize,
    entry: &LeafEntry,
    metadata: BTreeMap<String, String>,
) -> SearchChunkHit {
    SearchChunkHit {
        position,
        leaf_block_id: leaf_block_id.to_string(),
        media_type: entry.content.media_type.clone(),
        text: String::from_utf8_lossy(&entry.content.body).into_owned(),
        source_kind: metadata.get("source_kind").cloned(),
        source_path: metadata.get("source_path").cloned(),
        source_name: source_name_from_metadata(&metadata),
        metadata,
    }
}

fn configured_block_store(
    request_dir: &Path,
    environment: &McpEnvironmentConfig,
) -> Result<ConfiguredBlockStore, RuntimeError> {
    match environment {
        McpEnvironmentConfig::Shared(environment) => {
            Ok(ConfiguredBlockStore::from_environment_with_redb_read_only(
                request_dir,
                environment,
                None,
            )?)
        }
        McpEnvironmentConfig::GatewayHttp3(gateway) => Ok(
            ConfiguredBlockStore::gateway_http3_store(&gateway.gateway_dns_name)?,
        ),
        McpEnvironmentConfig::GatewayHttp3FilesystemCache(gateway) => {
            let cache_root = gateway
                .block_cache_root
                .as_deref()
                .unwrap_or_else(|| Path::new(DEFAULT_GATEWAY_HTTP3_FS_CACHE_ROOT));
            Ok(
                ConfiguredBlockStore::gateway_http3_filesystem_overlay_store(
                    request_dir,
                    cache_root,
                    gateway.memory_cache_max_resident_blocks,
                    &gateway.gateway_dns_name,
                )?,
            )
        }
    }
}

fn configured_embedding_provider(
    environment: &McpEnvironmentConfig,
) -> Result<ConfiguredEmbeddingProvider, RuntimeError> {
    match environment {
        McpEnvironmentConfig::Shared(environment) => {
            Ok(ConfiguredEmbeddingProvider::from_environment(environment)?)
        }
        McpEnvironmentConfig::GatewayHttp3(gateway) => {
            Ok(ConfiguredEmbeddingProvider::gateway_http3(
                &gateway.gateway_dns_name,
                gateway.model.clone(),
                gateway.max_retries,
                gateway.retry_delay_ms,
                gateway.request_timeout_secs,
            )?)
        }
        McpEnvironmentConfig::GatewayHttp3FilesystemCache(gateway) => {
            Ok(ConfiguredEmbeddingProvider::gateway_http3(
                &gateway.gateway_dns_name,
                gateway.model.clone(),
                gateway.max_retries,
                gateway.retry_delay_ms,
                gateway.request_timeout_secs,
            )?)
        }
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

    use ciborium::Value;
    use lexonarchivebuilder_indexer::block_store::ConfiguredBlockStore;
    use lexonarchivebuilder_indexer::config::{
        BatchItemConfig, BatchRequest, EmbeddingSpecConfig, EnvironmentConfig, ExecutionStage,
        LocalEmbeddingConfig,
    };
    use lexonarchivebuilder_indexer::{run_request, write_summary_file};
    use lexongraph_block::{
        Block, BranchBlock, BranchEntry, Content, EmbeddingSpec, LeafBlock, LeafEntry, VERSION_1,
    };
    use lexongraph_block_store::BlockStore;
    use tempfile::tempdir;

    use super::*;
    use crate::config::{
        GatewayHttp3FilesystemCacheKind, GatewayHttp3FilesystemCacheMcpEnvironmentConfig,
        GatewayHttp3Kind, GatewayHttp3McpEnvironmentConfig, IndexConfig, McpEnvironmentConfig,
        ToolDescriptionsConfig,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn gateway_environment_selects_gateway_dependencies() {
        let environment = McpEnvironmentConfig::GatewayHttp3(GatewayHttp3McpEnvironmentConfig {
            kind: GatewayHttp3Kind::GatewayHttp3,
            gateway_dns_name: "gateway.example.test".into(),
            model: "gateway-model".into(),
            request_timeout_secs: 1,
            max_retries: 0,
            retry_delay_ms: 1,
        });

        let block_store = configured_block_store(Path::new("."), &environment).unwrap();
        let embedding_provider = configured_embedding_provider(&environment).unwrap();

        assert!(matches!(block_store, ConfiguredBlockStore::GatewayHttp3(_)));
        assert!(matches!(
            embedding_provider,
            ConfiguredEmbeddingProvider::GatewayHttp3(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn filesystem_cache_gateway_selects_overlay_and_gateway_embedding() {
        let temp = tempdir().unwrap();
        let environment = McpEnvironmentConfig::GatewayHttp3FilesystemCache(
            GatewayHttp3FilesystemCacheMcpEnvironmentConfig {
                kind: GatewayHttp3FilesystemCacheKind::GatewayHttp3FsCache,
                gateway_dns_name: "gateway.example.test".into(),
                block_cache_root: Some(PathBuf::from("cache")),
                memory_cache_max_resident_blocks: 256,
                model: "gateway-model".into(),
                request_timeout_secs: 1,
                max_retries: 0,
                retry_delay_ms: 1,
            },
        );

        let block_store = configured_block_store(temp.path(), &environment).unwrap();
        let embedding_provider = configured_embedding_provider(&environment).unwrap();

        assert!(matches!(block_store, ConfiguredBlockStore::Overlay(_)));
        assert!(matches!(
            embedding_provider,
            ConfiguredEmbeddingProvider::GatewayHttp3(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runtime_constructs_filesystem_cache_overlay_once() {
        let temp = tempdir().unwrap();
        let config = McpConfig {
            environment: McpEnvironmentConfig::GatewayHttp3FilesystemCache(
                GatewayHttp3FilesystemCacheMcpEnvironmentConfig {
                    kind: GatewayHttp3FilesystemCacheKind::GatewayHttp3FsCache,
                    gateway_dns_name: "gateway.example.test".into(),
                    block_cache_root: Some(PathBuf::from("cache")),
                    memory_cache_max_resident_blocks: 256,
                    model: "gateway-model".into(),
                    request_timeout_secs: 1,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            ),
            embedding_spec: EmbeddingSpecConfig {
                dims: 384,
                encoding: "f32le".into(),
            },
            index: IndexConfig::RootId {
                root_id: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff".into(),
            },
            tool_descriptions: ToolDescriptionsConfig::default(),
            top_k: 5,
            traversal_width: 3,
        };

        let runtime = McpRuntime::new(temp.path().to_path_buf(), config).unwrap();
        let ConfiguredBlockStore::Overlay(first) = &runtime.block_store else {
            panic!("expected runtime-owned overlay");
        };
        let ConfiguredBlockStore::Overlay(second) = runtime.block_store.clone() else {
            panic!("expected cloned runtime overlay");
        };
        assert!(Arc::ptr_eq(first, &second));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn filesystem_cache_runtime_exposes_request_local_stats_deltas() {
        let temp = tempdir().unwrap();
        let runtime = McpRuntime::new(
            temp.path().to_path_buf(),
            McpConfig {
                environment: McpEnvironmentConfig::GatewayHttp3FilesystemCache(
                    GatewayHttp3FilesystemCacheMcpEnvironmentConfig {
                        kind: GatewayHttp3FilesystemCacheKind::GatewayHttp3FsCache,
                        gateway_dns_name: "gateway.example.test".into(),
                        block_cache_root: Some(PathBuf::from("cache")),
                        memory_cache_max_resident_blocks: 256,
                        model: "gateway-model".into(),
                        request_timeout_secs: 1,
                        max_retries: 0,
                        retry_delay_ms: 1,
                    },
                ),
                embedding_spec: EmbeddingSpecConfig {
                    dims: 384,
                    encoding: "f32le".into(),
                },
                index: IndexConfig::RootId {
                    root_id: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                        .into(),
                },
                tool_descriptions: ToolDescriptionsConfig::default(),
                top_k: 5,
                traversal_width: 3,
            },
        )
        .unwrap();

        let before = runtime.cache_stats().expect("expected overlay stats");
        let delta = runtime
            .cache_stats_delta(Some(before))
            .unwrap()
            .expect("expected overlay stats delta");
        assert_eq!(
            delta
                .layers
                .iter()
                .map(|layer| layer.role.as_str())
                .collect::<Vec<_>>(),
            vec!["cache", "cache", "read-only"]
        );
        assert!(
            delta
                .layers
                .iter()
                .all(|layer| layer.hits == 0 && layer.misses == 0)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn search_chunks_uses_upstream_target_preparation_for_branch_roots() {
        let temp = tempdir().unwrap();
        let server = spawn_embedding_server(1);
        let environment = EnvironmentConfig::Local {
            block_store_root: PathBuf::from("block-store"),
            embedding: LocalEmbeddingConfig {
                base_url: server.base_url.clone(),
                model: "all-MiniLM-L6-v2".into(),
                api_key_env: None,
                request_timeout_secs: 5,
                max_retries: 0,
                retry_delay_ms: 1,
            },
        };
        let store = ConfiguredBlockStore::from_environment(temp.path(), &environment).unwrap();
        let leaf = store
            .put(&Block::Leaf(LeafBlock {
                version: VERSION_1,
                level: 0,
                embedding_spec: EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                entries: vec![LeafEntry {
                    embedding: f32_bytes(&[1.0, 0.0]),
                    metadata: Vec::new(),
                    content: Content {
                        media_type: "text/plain".into(),
                        body: b"branch-root result".to_vec(),
                    },
                }],
                ext: None,
            }))
            .await
            .unwrap();
        let root = store
            .put(&Block::Branch(BranchBlock {
                version: VERSION_1,
                level: 1,
                embedding_spec: EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                entries: vec![BranchEntry {
                    embedding: f32_bytes(&[1.0, 0.0]),
                    child: leaf,
                }],
                ext: None,
            }))
            .await
            .unwrap();
        let runtime = McpRuntime::new(
            temp.path().to_path_buf(),
            McpConfig {
                environment: McpEnvironmentConfig::Shared(environment),
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                index: IndexConfig::RootId {
                    root_id: root.to_string(),
                },
                tool_descriptions: ToolDescriptionsConfig::default(),
                top_k: 1,
                traversal_width: 1,
            },
        )
        .unwrap();

        let response = runtime
            .search_chunks(SearchChunksRequest {
                query: "branch".into(),
                top_k: None,
                traversal_width: None,
            })
            .await
            .unwrap();

        assert_eq!(response.results[0].text, "branch-root result");
        server.join();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn search_chunks_rejects_leaf_root_from_local_profile() {
        let temp = tempdir().unwrap();
        let document_path = temp.path().join("overview.txt");
        fs::write(
            &document_path,
            b"LexonArchiveBuilder MCP runtime document body\n",
        )
        .unwrap();

        let server = spawn_embedding_server(1);
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
            replay_batch_size: None,
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
                environment: McpEnvironmentConfig::Shared(EnvironmentConfig::Local {
                    block_store_root: PathBuf::from("block-store"),
                    embedding: LocalEmbeddingConfig {
                        base_url: server.base_url.clone(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 5,
                        retry_delay_ms: 1,
                    },
                }),
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                index: IndexConfig::SummaryFile {
                    path: PathBuf::from("summary.json"),
                },
                tool_descriptions: ToolDescriptionsConfig::default(),
                top_k: 5,
                traversal_width: 3,
            },
        )
        .unwrap();

        let error = runtime
            .search_chunks(SearchChunksRequest {
                query: "runtime document".into(),
                top_k: None,
                traversal_width: None,
            })
            .await
            .unwrap_err();

        assert!(matches!(error, RuntimeError::LeafRoot { .. }));
        server.join();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn search_chunks_rejects_leaf_root_from_local_redb_profile() {
        let temp = tempdir().unwrap();
        let document_path = temp.path().join("overview.txt");
        fs::write(
            &document_path,
            b"LexonArchiveBuilder MCP runtime redb document body\n",
        )
        .unwrap();

        let server = spawn_embedding_server(1);
        let batch_request = BatchRequest {
            environment: EnvironmentConfig::LocalRedb {
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
            replay_batch_size: None,
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
                environment: McpEnvironmentConfig::Shared(EnvironmentConfig::LocalRedb {
                    block_store_root: PathBuf::from("block-store"),
                    embedding: LocalEmbeddingConfig {
                        base_url: server.base_url.clone(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 5,
                        retry_delay_ms: 1,
                    },
                }),
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                index: IndexConfig::SummaryFile {
                    path: PathBuf::from("summary.json"),
                },
                tool_descriptions: ToolDescriptionsConfig::default(),
                top_k: 5,
                traversal_width: 3,
            },
        )
        .unwrap();

        let error = runtime
            .search_chunks(SearchChunksRequest {
                query: "runtime redb document".into(),
                top_k: None,
                traversal_width: None,
            })
            .await
            .unwrap_err();

        assert!(matches!(error, RuntimeError::LeafRoot { .. }));
        server.join();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn search_chunks_rejects_leaf_root_from_mailbox_profile() {
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

        let server = spawn_embedding_server(1);
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
            replay_batch_size: None,
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
                environment: McpEnvironmentConfig::Shared(EnvironmentConfig::Local {
                    block_store_root: PathBuf::from("block-store"),
                    embedding: LocalEmbeddingConfig {
                        base_url: server.base_url.clone(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 5,
                        retry_delay_ms: 1,
                    },
                }),
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                index: IndexConfig::SummaryFile {
                    path: PathBuf::from("summary.json"),
                },
                tool_descriptions: ToolDescriptionsConfig::default(),
                top_k: 3,
                traversal_width: 2,
            },
        )
        .unwrap();

        let error = runtime
            .search_chunks(SearchChunksRequest {
                query: "searchable provenance".into(),
                top_k: None,
                traversal_width: None,
            })
            .await
            .unwrap_err();

        assert!(matches!(error, RuntimeError::LeafRoot { .. }));
        server.join();
    }

    #[test]
    fn document_and_thread_retrieval_return_explicit_unsupported_outcome() {
        let runtime = McpRuntime::new(
            PathBuf::from("C:\\request-root"),
            McpConfig {
                environment: McpEnvironmentConfig::Shared(EnvironmentConfig::Local {
                    block_store_root: PathBuf::from("block-store"),
                    embedding: LocalEmbeddingConfig {
                        base_url: "http://localhost:8080".into(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 0,
                        retry_delay_ms: 1,
                    },
                }),
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                index: IndexConfig::RootId {
                    root_id: "4c33a6fc7cac4679c0a1f57d40203a28e997c3a92783abf4dc0f7162d36f856e"
                        .into(),
                },
                tool_descriptions: ToolDescriptionsConfig::default(),
                top_k: 1,
                traversal_width: 1,
            },
        )
        .unwrap();

        let document = runtime.get_document(NamedRetrievalRequest {
            name: "overview.txt".into(),
        });
        let thread = runtime.get_thread(NamedRetrievalRequest {
            name: "thread-1".into(),
        });

        assert!(matches!(document.status, NamedRetrievalStatus::Unsupported));
        assert!(matches!(thread.status, NamedRetrievalStatus::Unsupported));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_email_returns_the_selected_leaf_email_entry() {
        let temp = tempdir().unwrap();
        let environment = local_environment();
        let store = ConfiguredBlockStore::from_environment(temp.path(), &environment).unwrap();
        let leaf_block_id = store
            .put(&Block::Leaf(LeafBlock {
                version: VERSION_1,
                level: 0,
                embedding_spec: EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                entries: vec![test_leaf_entry("email", b"email")],
                ext: None,
            }))
            .await
            .unwrap();
        let runtime = local_runtime(temp.path(), environment);

        let response = runtime
            .get_email(NamedRetrievalRequest {
                name: leaf_block_id.to_string(),
            })
            .await
            .unwrap();

        assert_eq!(response.leaf_block_id, leaf_block_id.to_string());
        assert_eq!(response.entry.position, 0);
        assert_eq!(response.entry.text, "email");
        assert_eq!(response.entry.source_kind.as_deref(), Some("email"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_email_rejects_invalid_missing_branch_and_non_email_leaves() {
        let temp = tempdir().unwrap();
        let environment = local_environment();
        let store = ConfiguredBlockStore::from_environment(temp.path(), &environment).unwrap();
        let non_email_leaf = store
            .put(&Block::Leaf(LeafBlock {
                version: VERSION_1,
                level: 0,
                embedding_spec: EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                entries: vec![test_leaf_entry("document", b"document")],
                ext: None,
            }))
            .await
            .unwrap();
        let branch = store
            .put(&Block::Branch(BranchBlock {
                version: VERSION_1,
                level: 1,
                embedding_spec: EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                entries: vec![BranchEntry {
                    embedding: f32_bytes(&[1.0, 0.0]),
                    child: non_email_leaf,
                }],
                ext: None,
            }))
            .await
            .unwrap();
        let runtime = local_runtime(temp.path(), environment);

        let invalid = runtime
            .get_email(NamedRetrievalRequest {
                name: "not-a-block-id".into(),
            })
            .await
            .unwrap_err();
        let missing = runtime
            .get_email(NamedRetrievalRequest {
                name: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff".into(),
            })
            .await
            .unwrap_err();
        let non_leaf = runtime
            .get_email(NamedRetrievalRequest {
                name: branch.to_string(),
            })
            .await
            .unwrap_err();
        let no_email_entries = runtime
            .get_email(NamedRetrievalRequest {
                name: non_email_leaf.to_string(),
            })
            .await
            .unwrap_err();

        assert!(matches!(
            invalid,
            RuntimeError::InvalidEmailLeafBlockId { .. }
        ));
        assert!(matches!(
            missing,
            RuntimeError::MissingEmailLeafBlock { .. }
        ));
        assert!(matches!(non_leaf, RuntimeError::EmailBlockIsNotLeaf { .. }));
        assert!(matches!(
            no_email_entries,
            RuntimeError::EmailLeafContainsNoEmailEntries { .. }
        ));
    }

    #[test]
    fn email_leaf_requires_exactly_one_entry() {
        let entries = vec![
            test_leaf_entry("email", b"first"),
            test_leaf_entry("email", b"second"),
        ];

        let error = sole_email_leaf_entry("leaf", &entries).unwrap_err();

        assert!(matches!(
            error,
            RuntimeError::EmailLeafEntryCount { entry_count: 2, .. }
        ));
    }

    fn local_environment() -> EnvironmentConfig {
        EnvironmentConfig::Local {
            block_store_root: PathBuf::from("block-store"),
            embedding: LocalEmbeddingConfig {
                base_url: "http://localhost:8080".into(),
                model: "all-MiniLM-L6-v2".into(),
                api_key_env: None,
                request_timeout_secs: 5,
                max_retries: 0,
                retry_delay_ms: 1,
            },
        }
    }

    fn local_runtime(request_dir: &Path, environment: EnvironmentConfig) -> McpRuntime {
        McpRuntime::new(
            request_dir.to_path_buf(),
            McpConfig {
                environment: McpEnvironmentConfig::Shared(environment),
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                index: IndexConfig::RootId {
                    root_id: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                        .into(),
                },
                tool_descriptions: ToolDescriptionsConfig::default(),
                top_k: 1,
                traversal_width: 1,
            },
        )
        .unwrap()
    }

    fn test_leaf_entry(source_kind: &str, body: &[u8]) -> LeafEntry {
        LeafEntry {
            embedding: f32_bytes(&[1.0, 0.0]),
            metadata: vec![
                ("source_kind".into(), Value::Text(source_kind.into())),
                ("source_path".into(), Value::Text("source-path".into())),
                ("email_name".into(), Value::Text("email-name".into())),
            ],
            content: Content {
                media_type: "text/plain".into(),
                body: body.to_vec(),
            },
        }
    }

    fn f32_bytes(values: &[f32]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }

    #[test]
    fn empty_local_embedding_base_url_is_rejected_at_runtime_construction() {
        let error = McpRuntime::new(
            PathBuf::from("C:\\request-root"),
            McpConfig {
                environment: McpEnvironmentConfig::Shared(EnvironmentConfig::Local {
                    block_store_root: PathBuf::from("block-store"),
                    embedding: LocalEmbeddingConfig {
                        base_url: String::new(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 0,
                        retry_delay_ms: 1,
                    },
                }),
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                index: IndexConfig::RootId {
                    root_id: "4c33a6fc7cac4679c0a1f57d40203a28e997c3a92783abf4dc0f7162d36f856e"
                        .into(),
                },
                tool_descriptions: ToolDescriptionsConfig::default(),
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
                environment: McpEnvironmentConfig::Shared(EnvironmentConfig::Local {
                    block_store_root: PathBuf::from("block-store"),
                    embedding: LocalEmbeddingConfig {
                        base_url: "http://localhost:8080".into(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 0,
                        retry_delay_ms: 1,
                    },
                }),
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                index: IndexConfig::SummaryFile {
                    path: PathBuf::from("summary.json"),
                },
                tool_descriptions: ToolDescriptionsConfig::default(),
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
