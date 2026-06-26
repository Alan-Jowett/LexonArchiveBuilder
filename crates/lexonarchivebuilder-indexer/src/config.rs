use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ciborium::Value;
use clap::{Args, ValueEnum};
use lexongraph_block::EmbeddingSpec;
use lexongraph_streaming_indexer::{IndexItem, Metadata};
pub use lexongraph_streaming_indexer::{PUBLISHED_PROFILE_V0_1_0, PublishedProfileVersion};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::paths::resolve_path;
use crate::resolver::ContentRef;
use crate::tree_tools::metadata_values_to_text_map;

const DEFAULT_BLOCK_SIZE_TARGET: usize = 65_536;
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_MAX_RETRIES: u32 = 5;
const DEFAULT_RETRY_DELAY_MS: u64 = 1_000;
const MIN_MAX_CONCURRENCY: usize = 1;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ExecutionStage {
    #[default]
    FullPipeline,
    IngestionAndEmbedding,
    ClusteringAndBlockAssembly,
}

impl ExecutionStage {
    pub fn includes_ingestion(self) -> bool {
        matches!(self, Self::FullPipeline | Self::IngestionAndEmbedding)
    }

    pub fn includes_clustering(self) -> bool {
        matches!(self, Self::FullPipeline | Self::ClusteringAndBlockAssembly)
    }
}

#[derive(Args, Clone, Debug, Default, PartialEq)]
pub struct ClusteringConfigOverrides {
    #[arg(
        long,
        value_name = "MAJOR.MINOR.PATCH",
        value_parser = parse_published_profile_version
    )]
    pub profile_version: Option<PublishedProfileVersion>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConfiguredClustering {
    pub profile_version: PublishedProfileVersion,
}

impl ClusteringConfigOverrides {
    pub fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }

    pub(crate) fn to_configured_clustering(
        &self,
        request_profile_version: PublishedProfileVersion,
    ) -> Result<ConfiguredClustering, ConfigError> {
        self.validate()?;
        Ok(ConfiguredClustering {
            profile_version: self.profile_version.unwrap_or(request_profile_version),
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct BatchRequest {
    pub environment: EnvironmentConfig,
    pub embedding_spec: EmbeddingSpecConfig,
    #[serde(default = "default_block_size_target")]
    pub block_size_target: usize,
    #[serde(default)]
    pub stage: ExecutionStage,
    #[serde(
        default = "default_profile_version",
        deserialize_with = "deserialize_published_profile_version"
    )]
    pub profile_version: PublishedProfileVersion,
    #[serde(default)]
    pub max_concurrency: Option<usize>,
    #[serde(default)]
    pub items: Vec<BatchItemConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum EnvironmentConfig {
    Local {
        block_store_root: PathBuf,
        embedding: LocalEmbeddingConfig,
    },
    Production {
        block_store: ProductionBlockStoreConfig,
        embedding: ProductionEmbeddingConfig,
    },
}

#[derive(Clone, Debug, Deserialize)]
pub struct LocalEmbeddingConfig {
    pub base_url: String,
    #[serde(default = "default_local_model")]
    pub model: String,
    #[serde(default)]
    pub api_key_env: Option<String>,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProductionBlockStoreConfig {
    pub container_sas_url: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub filesystem_cache_root: Option<PathBuf>,
    #[serde(default)]
    pub memory_cache_max_resident_blocks: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProductionEmbeddingConfig {
    pub endpoint: String,
    pub deployment: String,
    #[serde(default = "default_azure_api_version")]
    pub api_version: String,
    #[serde(default)]
    pub api_key_env: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EmbeddingSpecConfig {
    pub dims: u64,
    pub encoding: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum BatchItemConfig {
    Mailbox {
        path: PathBuf,
        #[serde(default)]
        metadata: BTreeMap<String, String>,
    },
    Document {
        path: PathBuf,
        #[serde(default)]
        metadata: BTreeMap<String, String>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BatchSummary {
    pub root_id: String,
    pub block_ids: Vec<String>,
    pub block_count: usize,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("batch request must contain at least one item for the selected stage")]
    EmptyItems,
    #[error("max_concurrency must be at least 1 when specified")]
    InvalidMaxConcurrency,
    #[error("local embedding base_url must not be empty")]
    MissingLocalEmbeddingBaseUrl,
    #[error("production block_store.container_sas_url must not be empty")]
    MissingProductionContainerSasUrl,
    #[error("production block_store.prefix is not supported by the Azure Blob block store")]
    UnsupportedProductionBlockStorePrefix,
    #[error(
        "production block_store.filesystem_cache_root is required for the production overlay block store"
    )]
    MissingProductionFilesystemCacheRoot,
    #[error(
        "production block_store.memory_cache_max_resident_blocks is required for the production overlay block store"
    )]
    MissingProductionMemoryCacheMaxResidentBlocks,
    #[error("production block_store.memory_cache_max_resident_blocks must be at least 1")]
    InvalidProductionMemoryCacheMaxResidentBlocks,
}

impl BatchRequest {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.stage.includes_ingestion() && self.items.is_empty() {
            return Err(ConfigError::EmptyItems);
        }
        if matches!(self.max_concurrency, Some(0)) {
            return Err(ConfigError::InvalidMaxConcurrency);
        }
        self.environment.validate_for_stage(self.stage)?;
        Ok(())
    }

    pub fn to_document_index_items(&self, request_dir: &Path) -> Vec<IndexItem<ContentRef>> {
        self.items
            .iter()
            .filter_map(|item| item.to_document_index_item(request_dir))
            .collect::<Vec<_>>()
    }

    pub fn to_embedding_spec(&self) -> EmbeddingSpec {
        self.embedding_spec.clone().into()
    }

    pub fn effective_max_concurrency(&self) -> usize {
        self.max_concurrency.unwrap_or_else(default_max_concurrency)
    }
}

impl EnvironmentConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_for_stage(ExecutionStage::FullPipeline)
    }

    pub fn validate_for_stage(&self, stage: ExecutionStage) -> Result<(), ConfigError> {
        match self {
            Self::Local { .. } => {
                if stage.includes_ingestion() {
                    self.local_embedding()?;
                }
            }
            Self::Production { block_store, .. } => {
                block_store.validate()?;
            }
        }
        Ok(())
    }

    pub fn resolve_block_store_root(&self, request_dir: &Path) -> Option<PathBuf> {
        match self {
            Self::Local {
                block_store_root, ..
            } => Some(resolve_path(request_dir, block_store_root)),
            Self::Production { .. } => None,
        }
    }

    pub fn local_embedding(&self) -> Result<Option<LocalEmbeddingConfig>, ConfigError> {
        match self {
            Self::Local { embedding, .. } => {
                if embedding.base_url.trim().is_empty() {
                    Err(ConfigError::MissingLocalEmbeddingBaseUrl)
                } else {
                    Ok(Some(embedding.clone()))
                }
            }
            Self::Production { .. } => Ok(None),
        }
    }
}

impl ProductionBlockStoreConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.container_sas_url.trim().is_empty() {
            return Err(ConfigError::MissingProductionContainerSasUrl);
        }
        if self
            .prefix
            .as_deref()
            .is_some_and(|prefix| !prefix.trim().is_empty())
        {
            return Err(ConfigError::UnsupportedProductionBlockStorePrefix);
        }
        if self
            .filesystem_cache_root
            .as_ref()
            .is_none_or(|path| path.as_os_str().is_empty())
        {
            return Err(ConfigError::MissingProductionFilesystemCacheRoot);
        }
        match self.memory_cache_max_resident_blocks {
            Some(0) => {
                return Err(ConfigError::InvalidProductionMemoryCacheMaxResidentBlocks);
            }
            Some(_) => {}
            None => {
                return Err(ConfigError::MissingProductionMemoryCacheMaxResidentBlocks);
            }
        }
        Ok(())
    }
}

impl BatchItemConfig {
    fn to_document_index_item(&self, request_dir: &Path) -> Option<IndexItem<ContentRef>> {
        match self {
            Self::Document { path, metadata } => {
                let resolved = resolve_path(request_dir, path);
                Some(IndexItem {
                    metadata: metadata_to_lexongraph(metadata, "document", &resolved),
                    content_ref: ContentRef::Document { path: resolved },
                })
            }
            Self::Mailbox { .. } => None,
        }
    }
}

impl From<EmbeddingSpecConfig> for EmbeddingSpec {
    fn from(value: EmbeddingSpecConfig) -> Self {
        Self {
            dims: value.dims,
            encoding: value.encoding,
        }
    }
}

impl From<&EmbeddingSpecConfig> for EmbeddingSpec {
    fn from(value: &EmbeddingSpecConfig) -> Self {
        Self {
            dims: value.dims,
            encoding: value.encoding.clone(),
        }
    }
}

pub(crate) fn metadata_to_lexongraph(
    metadata: &BTreeMap<String, String>,
    source_kind: &str,
    path: &Path,
) -> Metadata {
    let mut result = Vec::with_capacity(metadata.len() + 2);
    result.push((
        Value::Text("source_kind".into()),
        Value::Text(source_kind.to_string()),
    ));
    result.push((
        Value::Text("source_path".into()),
        Value::Text(path.to_string_lossy().replace('\\', "/")),
    ));

    for (key, value) in metadata {
        result.push((Value::Text(key.clone()), Value::Text(value.clone())));
    }

    result
}

pub(crate) fn metadata_to_text_map(metadata: &Metadata) -> BTreeMap<String, String> {
    metadata_values_to_text_map(metadata)
}

fn default_block_size_target() -> usize {
    DEFAULT_BLOCK_SIZE_TARGET
}

fn default_profile_version() -> PublishedProfileVersion {
    PUBLISHED_PROFILE_V0_1_0
}

fn default_local_model() -> String {
    "all-MiniLM-L6-v2".to_string()
}

fn default_request_timeout_secs() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_SECS
}

fn default_max_retries() -> u32 {
    DEFAULT_MAX_RETRIES
}

fn default_retry_delay_ms() -> u64 {
    DEFAULT_RETRY_DELAY_MS
}

fn default_max_concurrency() -> usize {
    derive_default_max_concurrency(detected_cpu_count_for_default())
}

fn detected_cpu_count_for_default() -> usize {
    let physical = num_cpus::get_physical();
    if physical > 0 {
        return physical;
    }

    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(MIN_MAX_CONCURRENCY)
}

fn derive_default_max_concurrency(cpu_count: usize) -> usize {
    if cpu_count <= 1 {
        return MIN_MAX_CONCURRENCY;
    }

    (cpu_count / 2).max(MIN_MAX_CONCURRENCY)
}

fn default_azure_api_version() -> String {
    "2024-02-01".to_string()
}

fn deserialize_published_profile_version<'de, D>(
    deserializer: D,
) -> Result<PublishedProfileVersion, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_published_profile_version(&value).map_err(serde::de::Error::custom)
}

fn parse_published_profile_version(value: &str) -> Result<PublishedProfileVersion, String> {
    let trimmed = value.trim();
    let Some((major, remainder)) = trimmed.split_once('.') else {
        return Err(format!(
            "invalid published profile version '{trimmed}': expected <major>.<minor>.<patch>"
        ));
    };
    let Some((minor, patch)) = remainder.split_once('.') else {
        return Err(format!(
            "invalid published profile version '{trimmed}': expected <major>.<minor>.<patch>"
        ));
    };
    if major.is_empty() || minor.is_empty() || patch.is_empty() || patch.contains('.') {
        return Err(format!(
            "invalid published profile version '{trimmed}': expected <major>.<minor>.<patch>"
        ));
    }

    let major = major.parse::<u64>().map_err(|_| {
        format!("invalid published profile version '{trimmed}': major must be an unsigned integer")
    })?;
    let minor = minor.parse::<u64>().map_err(|_| {
        format!("invalid published profile version '{trimmed}': minor must be an unsigned integer")
    })?;
    let patch = patch.parse::<u64>().map_err(|_| {
        format!("invalid published profile version '{trimmed}': patch must be an unsigned integer")
    })?;

    Ok(PublishedProfileVersion::new(major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn relative_paths_are_resolved_against_request_directory() {
        let request_root = PathBuf::from("request-root");
        let relative_document_path = PathBuf::from("docs").join("sample.txt");
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: PathBuf::from("blocks"),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://localhost:8080".into(),
                    model: default_local_model(),
                    api_key_env: None,
                    request_timeout_secs: default_request_timeout_secs(),
                    max_retries: default_max_retries(),
                    retry_delay_ms: default_retry_delay_ms(),
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 384,
                encoding: "f32le".into(),
            },
            block_size_target: default_block_size_target(),
            stage: ExecutionStage::FullPipeline,
            profile_version: default_profile_version(),
            max_concurrency: None,
            items: vec![BatchItemConfig::Document {
                path: relative_document_path.clone(),
                metadata: BTreeMap::new(),
            }],
        };

        let items = request.to_document_index_items(&request_root);

        match &items[0].content_ref {
            ContentRef::Document { path } => {
                assert_eq!(path, &request_root.join(relative_document_path));
            }
            ContentRef::Inline { .. } => panic!("expected a document content ref"),
            ContentRef::EmailChunk { .. } => panic!("expected a document content ref"),
        }
    }

    #[test]
    fn explicit_max_concurrency_must_be_positive() {
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: PathBuf::from("blocks"),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://localhost:8080".into(),
                    model: default_local_model(),
                    api_key_env: None,
                    request_timeout_secs: default_request_timeout_secs(),
                    max_retries: default_max_retries(),
                    retry_delay_ms: default_retry_delay_ms(),
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 384,
                encoding: "f32le".into(),
            },
            block_size_target: default_block_size_target(),
            stage: ExecutionStage::FullPipeline,
            profile_version: default_profile_version(),
            max_concurrency: Some(0),
            items: vec![BatchItemConfig::Document {
                path: PathBuf::from("docs").join("sample.txt"),
                metadata: BTreeMap::new(),
            }],
        };

        assert!(matches!(
            request.validate(),
            Err(ConfigError::InvalidMaxConcurrency)
        ));
    }

    #[test]
    fn explicit_max_concurrency_overrides_default() {
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: PathBuf::from("blocks"),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://localhost:8080".into(),
                    model: default_local_model(),
                    api_key_env: None,
                    request_timeout_secs: default_request_timeout_secs(),
                    max_retries: default_max_retries(),
                    retry_delay_ms: default_retry_delay_ms(),
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 384,
                encoding: "f32le".into(),
            },
            block_size_target: default_block_size_target(),
            stage: ExecutionStage::FullPipeline,
            profile_version: default_profile_version(),
            max_concurrency: Some(7),
            items: vec![BatchItemConfig::Document {
                path: PathBuf::from("docs").join("sample.txt"),
                metadata: BTreeMap::new(),
            }],
        };

        assert_eq!(request.effective_max_concurrency(), 7);
    }

    #[test]
    fn derived_default_max_concurrency_uses_half_the_detected_cpu_count() {
        assert_eq!(derive_default_max_concurrency(0), 1);
        assert_eq!(derive_default_max_concurrency(1), 1);
        assert_eq!(derive_default_max_concurrency(2), 1);
        assert_eq!(derive_default_max_concurrency(3), 1);
        assert_eq!(derive_default_max_concurrency(4), 2);
        assert_eq!(derive_default_max_concurrency(8), 4);
    }

    #[test]
    fn stage_defaults_to_full_pipeline_when_omitted_from_request_json() {
        let request: BatchRequest = serde_json::from_value(json!({
            "environment": {
                "kind": "local",
                "block_store_root": "blocks",
                "embedding": {
                    "base_url": "http://localhost:8080"
                }
            },
            "embedding_spec": {
                "dims": 384,
                "encoding": "f32le"
            },
            "items": [{
                "kind": "document",
                "path": "docs/sample.txt"
            }]
        }))
        .unwrap();

        assert_eq!(request.stage, ExecutionStage::FullPipeline);
        assert_eq!(request.profile_version, PUBLISHED_PROFILE_V0_1_0);
    }

    #[test]
    fn clustering_only_stage_allows_empty_items() {
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: PathBuf::from("blocks"),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: default_local_model(),
                    api_key_env: None,
                    request_timeout_secs: default_request_timeout_secs(),
                    max_retries: default_max_retries(),
                    retry_delay_ms: default_retry_delay_ms(),
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 384,
                encoding: "f32le".into(),
            },
            block_size_target: default_block_size_target(),
            stage: ExecutionStage::ClusteringAndBlockAssembly,
            profile_version: default_profile_version(),
            max_concurrency: None,
            items: vec![],
        };

        assert!(request.validate().is_ok());
    }

    #[test]
    fn clustering_only_stage_may_reuse_request_items() {
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: PathBuf::from("blocks"),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://localhost:8080".into(),
                    model: default_local_model(),
                    api_key_env: None,
                    request_timeout_secs: default_request_timeout_secs(),
                    max_retries: default_max_retries(),
                    retry_delay_ms: default_retry_delay_ms(),
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 384,
                encoding: "f32le".into(),
            },
            block_size_target: default_block_size_target(),
            stage: ExecutionStage::ClusteringAndBlockAssembly,
            profile_version: default_profile_version(),
            max_concurrency: None,
            items: vec![BatchItemConfig::Document {
                path: PathBuf::from("docs").join("sample.txt"),
                metadata: BTreeMap::new(),
            }],
        };

        assert!(request.validate().is_ok());
    }

    #[test]
    fn production_request_requires_non_empty_container_sas_url() {
        let request = BatchRequest {
            environment: EnvironmentConfig::Production {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url: String::new(),
                    prefix: None,
                    filesystem_cache_root: None,
                    memory_cache_max_resident_blocks: None,
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://example.openai.azure.com".into(),
                    deployment: "embeddings".into(),
                    api_version: default_azure_api_version(),
                    api_key_env: None,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 384,
                encoding: "f32le".into(),
            },
            block_size_target: default_block_size_target(),
            stage: ExecutionStage::FullPipeline,
            profile_version: default_profile_version(),
            max_concurrency: None,
            items: vec![BatchItemConfig::Document {
                path: PathBuf::from("docs").join("sample.txt"),
                metadata: BTreeMap::new(),
            }],
        };

        assert!(matches!(
            request.validate(),
            Err(ConfigError::MissingProductionContainerSasUrl)
        ));
    }

    #[test]
    fn production_request_rejects_non_empty_prefix() {
        let request = BatchRequest {
            environment: EnvironmentConfig::Production {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.blob.core.windows.net/archive-sync?sig=test".into(),
                    prefix: Some("archive-sync".into()),
                    filesystem_cache_root: None,
                    memory_cache_max_resident_blocks: None,
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://example.openai.azure.com".into(),
                    deployment: "embeddings".into(),
                    api_version: default_azure_api_version(),
                    api_key_env: None,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 384,
                encoding: "f32le".into(),
            },
            block_size_target: default_block_size_target(),
            stage: ExecutionStage::FullPipeline,
            profile_version: default_profile_version(),
            max_concurrency: None,
            items: vec![BatchItemConfig::Document {
                path: PathBuf::from("docs").join("sample.txt"),
                metadata: BTreeMap::new(),
            }],
        };

        assert!(matches!(
            request.validate(),
            Err(ConfigError::UnsupportedProductionBlockStorePrefix)
        ));
    }

    #[test]
    fn production_request_requires_filesystem_cache_root_for_overlay_block_store() {
        let request: BatchRequest = serde_json::from_value(json!({
            "environment": {
                "kind": "production",
                "block_store": {
                    "container_sas_url": "https://example.blob.core.windows.net/archive-sync?sig=test"
                },
                "embedding": {
                    "endpoint": "https://example.openai.azure.com",
                    "deployment": "embeddings"
                }
            },
            "embedding_spec": {
                "dims": 384,
                "encoding": "f32le"
            },
            "profile_version": "0.5.0",
            "items": [{
                "kind": "document",
                "path": "docs/sample.txt"
            }]
        }))
        .unwrap();

        assert!(matches!(
            request.validate(),
            Err(ConfigError::MissingProductionFilesystemCacheRoot)
        ));
    }

    #[test]
    fn production_request_accepts_overlay_cache_layers() {
        let request: BatchRequest = serde_json::from_value(json!({
            "environment": {
                "kind": "production",
                "block_store": {
                    "container_sas_url": "https://example.blob.core.windows.net/archive-sync?sig=test",
                    "filesystem_cache_root": "cache",
                    "memory_cache_max_resident_blocks": 64
                },
                "embedding": {
                    "endpoint": "https://example.openai.azure.com",
                    "deployment": "embeddings"
                }
            },
            "embedding_spec": {
                "dims": 384,
                "encoding": "f32le"
            },
            "profile_version": "0.5.0",
            "items": [{
                "kind": "document",
                "path": "docs/sample.txt"
            }]
        }))
        .unwrap();

        assert!(request.validate().is_ok());
        match request.environment {
            EnvironmentConfig::Production { block_store, .. } => {
                assert_eq!(
                    block_store.filesystem_cache_root,
                    Some(PathBuf::from("cache"))
                );
                assert_eq!(block_store.memory_cache_max_resident_blocks, Some(64));
            }
            EnvironmentConfig::Local { .. } => panic!("expected production environment"),
        }
    }

    #[test]
    fn production_request_rejects_empty_filesystem_cache_root() {
        let request: BatchRequest = serde_json::from_value(json!({
            "environment": {
                "kind": "production",
                "block_store": {
                    "container_sas_url": "https://example.blob.core.windows.net/archive-sync?sig=test",
                    "filesystem_cache_root": "",
                    "memory_cache_max_resident_blocks": 64
                },
                "embedding": {
                    "endpoint": "https://example.openai.azure.com",
                    "deployment": "embeddings"
                }
            },
            "embedding_spec": {
                "dims": 384,
                "encoding": "f32le"
            },
            "profile_version": "0.5.0",
            "items": [{
                "kind": "document",
                "path": "docs/sample.txt"
            }]
        }))
        .unwrap();

        assert!(matches!(
            request.validate(),
            Err(ConfigError::MissingProductionFilesystemCacheRoot)
        ));
    }

    #[test]
    fn production_request_rejects_partial_overlay_cache_config() {
        let request = BatchRequest {
            environment: EnvironmentConfig::Production {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.blob.core.windows.net/archive-sync?sig=test".into(),
                    prefix: None,
                    filesystem_cache_root: Some("cache".into()),
                    memory_cache_max_resident_blocks: None,
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://example.openai.azure.com".into(),
                    deployment: "embeddings".into(),
                    api_version: default_azure_api_version(),
                    api_key_env: None,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 384,
                encoding: "f32le".into(),
            },
            block_size_target: default_block_size_target(),
            stage: ExecutionStage::FullPipeline,
            profile_version: default_profile_version(),
            max_concurrency: None,
            items: vec![BatchItemConfig::Document {
                path: PathBuf::from("docs").join("sample.txt"),
                metadata: BTreeMap::new(),
            }],
        };

        assert!(matches!(
            request.validate(),
            Err(ConfigError::MissingProductionMemoryCacheMaxResidentBlocks)
        ));
    }

    #[test]
    fn clustering_defaults_to_published_profile_v0_1_0() {
        let clustering = ClusteringConfigOverrides::default()
            .to_configured_clustering(default_profile_version())
            .unwrap();

        assert_eq!(clustering.profile_version, PUBLISHED_PROFILE_V0_1_0);
    }

    #[test]
    fn clustering_override_uses_request_profile_when_cli_omits_selector() {
        let clustering = ClusteringConfigOverrides::default()
            .to_configured_clustering(PublishedProfileVersion::new(0, 5, 0))
            .unwrap();

        assert_eq!(
            clustering.profile_version,
            PublishedProfileVersion::new(0, 5, 0)
        );
    }

    #[test]
    fn clustering_override_replaces_request_profile_when_cli_selects_profile() {
        let clustering = ClusteringConfigOverrides {
            profile_version: Some(PublishedProfileVersion::new(0, 5, 0)),
        }
        .to_configured_clustering(default_profile_version())
        .unwrap();

        assert_eq!(
            clustering.profile_version,
            PublishedProfileVersion::new(0, 5, 0)
        );
    }

    #[test]
    fn request_profile_version_deserializes_from_json_string() {
        let request: BatchRequest = serde_json::from_value(json!({
            "environment": {
                "kind": "local",
                "block_store_root": "blocks",
                "embedding": {
                    "base_url": "http://localhost:8080"
                }
            },
            "embedding_spec": {
                "dims": 384,
                "encoding": "f32le"
            },
            "profile_version": "0.5.0",
            "items": [{
                "kind": "document",
                "path": "docs/sample.txt"
            }]
        }))
        .unwrap();

        assert_eq!(
            request.profile_version,
            PublishedProfileVersion::new(0, 5, 0)
        );
    }

    #[test]
    fn request_profile_version_rejects_invalid_strings() {
        let error = serde_json::from_value::<BatchRequest>(json!({
            "environment": {
                "kind": "local",
                "block_store_root": "blocks",
                "embedding": {
                    "base_url": "http://localhost:8080"
                }
            },
            "embedding_spec": {
                "dims": 384,
                "encoding": "f32le"
            },
            "profile_version": "0.2",
            "items": [{
                "kind": "document",
                "path": "docs/sample.txt"
            }]
        }))
        .unwrap_err()
        .to_string();

        assert!(error.contains("expected <major>.<minor>.<patch>"));
    }
}
