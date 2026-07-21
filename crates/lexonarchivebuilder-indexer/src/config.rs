// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use ciborium::Value;
use clap::{Args, ValueEnum};
use lexongraph_block::EmbeddingSpec;
use lexongraph_streaming_indexer::{IndexItem, Metadata};
pub use lexongraph_streaming_indexer::{
    PUBLISHED_PROFILE_V0_1_0, PUBLISHED_PROFILE_V0_7_0, PublishedProfileVersion,
};
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
pub const MUTABLE_REF_ROOT_DIR: &str = "refs";

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
    #[arg(
        long,
        value_name = "COUNT",
        help = "Override published cluster_count for local/local-overlay ladder testing"
    )]
    pub local_testing_cluster_count: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConfiguredClustering {
    pub profile_version: PublishedProfileVersion,
    pub local_testing_cluster_count: Option<u32>,
}

impl ClusteringConfigOverrides {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if matches!(self.local_testing_cluster_count, Some(0)) {
            return Err(ConfigError::InvalidLocalTestingClusterCount);
        }
        Ok(())
    }

    pub(crate) fn to_configured_clustering(
        &self,
        request_profile_version: PublishedProfileVersion,
        environment: &EnvironmentConfig,
    ) -> Result<ConfiguredClustering, ConfigError> {
        self.validate()?;
        let profile_version = self.profile_version.unwrap_or(request_profile_version);
        if self.local_testing_cluster_count.is_some()
            && matches!(
                environment,
                EnvironmentConfig::Production { .. } | EnvironmentConfig::ProductionV2 { .. }
            )
        {
            return Err(ConfigError::LocalTestingClusterCountRequiresLocalEnvironment);
        }
        if self.local_testing_cluster_count.is_some() && profile_version == PUBLISHED_PROFILE_V0_7_0
        {
            return Err(ConfigError::LocalTestingClusterCountUnsupportedForPublishedProfileV0_7_0);
        }
        Ok(ConfiguredClustering {
            profile_version,
            local_testing_cluster_count: self.local_testing_cluster_count,
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
    pub ref_name: String,
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
    LocalOverlay {
        block_store: ProductionBlockStoreConfig,
        embedding: LocalEmbeddingConfig,
    },
    Production {
        block_store: ProductionBlockStoreConfig,
        embedding: ProductionEmbeddingConfig,
    },
    ProductionV2 {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MutableRefStoreLocation {
    LocalFile {
        path: PathBuf,
    },
    AzureBlob {
        url: String,
        display_path: String,
    },
    AzureTable {
        table_sas_url: String,
        display_path: String,
        partition_key: String,
        row_key: String,
    },
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("batch request must contain at least one item for the selected stage")]
    EmptyItems,
    #[error("max_concurrency must be at least 1 when specified")]
    InvalidMaxConcurrency,
    #[error("batch request ref_name must not be empty")]
    MissingRefName,
    #[error(
        "batch request ref_name must be a relative slash-separated ref name without empty, special, whitespace/control, or reserved URL/Windows-path segments"
    )]
    InvalidRefName,
    #[error("local embedding base_url must not be empty")]
    MissingLocalEmbeddingBaseUrl,
    #[error("overlay block_store.container_sas_url must not be empty")]
    MissingProductionContainerSasUrl,
    #[error("production-v2 block_store.container_sas_url must not be empty")]
    MissingProductionV2ContainerSasUrl,
    #[error("overlay block_store.prefix is not supported by the Azure Blob block store")]
    UnsupportedProductionBlockStorePrefix,
    #[error("production-v2 block_store.prefix is not supported by the Azure Table block store")]
    UnsupportedProductionV2BlockStorePrefix,
    #[error(
        "production-v2 block_store.filesystem_cache_root is not supported for the direct Azure Table block store"
    )]
    UnsupportedProductionV2FilesystemCacheRoot,
    #[error(
        "production-v2 block_store.memory_cache_max_resident_blocks is not supported for the direct Azure Table block store"
    )]
    UnsupportedProductionV2MemoryCacheMaxResidentBlocks,
    #[error(
        "overlay block_store.filesystem_cache_root is required for the overlay-backed block store"
    )]
    MissingProductionFilesystemCacheRoot,
    #[error(
        "overlay block_store.memory_cache_max_resident_blocks is required for the overlay-backed block store"
    )]
    MissingProductionMemoryCacheMaxResidentBlocks,
    #[error("overlay block_store.memory_cache_max_resident_blocks must be at least 1")]
    InvalidProductionMemoryCacheMaxResidentBlocks,
    #[error("local testing cluster_count override must be at least 1")]
    InvalidLocalTestingClusterCount,
    #[error(
        "local testing cluster_count override is only supported for local and local-overlay environments"
    )]
    LocalTestingClusterCountRequiresLocalEnvironment,
    #[error(
        "local testing cluster_count override is not supported for published profile 0.7.0 because that profile now runs through the streaming-indexer v2 path"
    )]
    LocalTestingClusterCountUnsupportedForPublishedProfileV0_7_0,
}

impl BatchRequest {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.stage.includes_ingestion() && self.items.is_empty() {
            return Err(ConfigError::EmptyItems);
        }
        if matches!(self.max_concurrency, Some(0)) {
            return Err(ConfigError::InvalidMaxConcurrency);
        }
        normalized_ref_name_segments(&self.ref_name)?;
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
            Self::LocalOverlay { block_store, .. } => {
                block_store.validate_for_overlay()?;
                if stage.includes_ingestion() {
                    self.local_embedding()?;
                }
            }
            Self::Production { block_store, .. } => {
                block_store.validate_for_overlay()?;
            }
            Self::ProductionV2 { block_store, .. } => {
                block_store.validate_for_azure_table()?;
            }
        }
        Ok(())
    }

    pub fn resolve_mutable_ref_store(
        &self,
        request_dir: &Path,
        ref_name: &str,
    ) -> Option<MutableRefStoreLocation> {
        let relative_path = mutable_ref_relative_path(ref_name)
            .expect("ref_name must be validated before resolving mutable ref storage");
        let relative_path_buf = mutable_ref_relative_path_buf(ref_name)
            .expect("ref_name must be validated before resolving mutable ref storage");
        match self {
            Self::Local {
                block_store_root, ..
            } => {
                let block_store_root = resolve_path(request_dir, block_store_root);
                let parent = block_store_root
                    .parent()
                    .unwrap_or(block_store_root.as_path());
                Some(MutableRefStoreLocation::LocalFile {
                    path: parent.join(relative_path_buf),
                })
            }
            Self::LocalOverlay { block_store, .. } | Self::Production { block_store, .. } => {
                Some(MutableRefStoreLocation::AzureBlob {
                    url: mutable_ref_store_blob_url(&block_store.container_sas_url, &relative_path),
                    display_path: mutable_ref_store_blob_display_path(
                        &block_store.container_sas_url,
                        &relative_path,
                    ),
                })
            }
            Self::ProductionV2 { block_store, .. } => Some(MutableRefStoreLocation::AzureTable {
                table_sas_url: block_store.container_sas_url.clone(),
                display_path: mutable_ref_store_table_display_path(
                    &block_store.container_sas_url,
                    &relative_path,
                ),
                partition_key: MUTABLE_REF_ROOT_DIR.into(),
                row_key: mutable_ref_store_table_row_key(&relative_path),
            }),
        }
    }

    pub fn local_embedding(&self) -> Result<Option<LocalEmbeddingConfig>, ConfigError> {
        match self {
            Self::Local { embedding, .. } | Self::LocalOverlay { embedding, .. } => {
                if embedding.base_url.trim().is_empty() {
                    Err(ConfigError::MissingLocalEmbeddingBaseUrl)
                } else {
                    Ok(Some(embedding.clone()))
                }
            }
            Self::Production { .. } | Self::ProductionV2 { .. } => Ok(None),
        }
    }
}

impl ProductionBlockStoreConfig {
    pub fn validate_for_overlay(&self) -> Result<(), ConfigError> {
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

    pub fn validate_for_azure_table(&self) -> Result<(), ConfigError> {
        if self.container_sas_url.trim().is_empty() {
            return Err(ConfigError::MissingProductionV2ContainerSasUrl);
        }
        if self
            .prefix
            .as_deref()
            .is_some_and(|prefix| !prefix.trim().is_empty())
        {
            return Err(ConfigError::UnsupportedProductionV2BlockStorePrefix);
        }
        if self.filesystem_cache_root.is_some() {
            return Err(ConfigError::UnsupportedProductionV2FilesystemCacheRoot);
        }
        if self.memory_cache_max_resident_blocks.is_some() {
            return Err(ConfigError::UnsupportedProductionV2MemoryCacheMaxResidentBlocks);
        }
        Ok(())
    }
}

fn mutable_ref_store_blob_url(container_sas_url: &str, file_name: &str) -> String {
    let (base, query) = match container_sas_url.split_once('?') {
        Some((base, query)) => (base.trim_end_matches('/'), Some(query)),
        None => (container_sas_url.trim_end_matches('/'), None),
    };
    match query {
        Some(query) => format!("{base}/{file_name}?{query}"),
        None => format!("{base}/{file_name}"),
    }
}

fn mutable_ref_store_blob_display_path(container_sas_url: &str, file_name: &str) -> String {
    let base = container_sas_url
        .split_once('?')
        .map_or(container_sas_url, |(base, _)| base)
        .trim_end_matches('/');
    format!("{base}/{file_name}")
}

fn mutable_ref_store_table_display_path(table_sas_url: &str, file_name: &str) -> String {
    let base = table_sas_url
        .split_once('?')
        .map_or(table_sas_url, |(base, _)| base)
        .trim_end_matches('/');
    format!("{base}/{file_name}")
}

fn mutable_ref_store_table_row_key(path: &str) -> String {
    let mut key = String::with_capacity(path.len() * 2);
    for byte in path.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut key, "{byte:02x}");
    }
    key
}

fn normalized_ref_name_segments(ref_name: &str) -> Result<Vec<String>, ConfigError> {
    let trimmed = ref_name.trim();
    if trimmed.is_empty() {
        return Err(ConfigError::MissingRefName);
    }
    let normalized = trimmed.replace('\\', "/");
    if normalized.starts_with('/') || normalized.ends_with('/') {
        return Err(ConfigError::InvalidRefName);
    }
    let segments = normalized
        .split('/')
        .map(str::to_string)
        .collect::<Vec<_>>();
    if segments
        .iter()
        .any(|segment| !is_valid_ref_name_segment(segment))
    {
        return Err(ConfigError::InvalidRefName);
    }
    Ok(segments)
}

fn is_valid_ref_name_segment(segment: &str) -> bool {
    if segment.is_empty()
        || segment.chars().any(|ch| {
            ch.is_whitespace()
                || ch.is_control()
                || matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*' | '#' | '%')
        })
    {
        return false;
    }

    let mut components = Path::new(segment).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

fn mutable_ref_relative_path(ref_name: &str) -> Result<String, ConfigError> {
    let segments = normalized_ref_name_segments(ref_name)?;
    Ok(format!("{MUTABLE_REF_ROOT_DIR}/{}", segments.join("/")))
}

fn mutable_ref_relative_path_buf(ref_name: &str) -> Result<PathBuf, ConfigError> {
    let segments = normalized_ref_name_segments(ref_name)?;
    let mut path = PathBuf::from(MUTABLE_REF_ROOT_DIR);
    for segment in segments {
        path.push(segment);
    }
    Ok(path)
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
    PUBLISHED_PROFILE_V0_7_0
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
            ref_name: "test-branch".into(),
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
            ContentRef::StoredReplay { .. } => panic!("expected a document content ref"),
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
            ref_name: "test-branch".into(),
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
            ref_name: "test-branch".into(),
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
            "ref_name": "test-branch",
            "items": [{
                "kind": "document",
                "path": "docs/sample.txt"
            }]
        }))
        .unwrap();

        assert_eq!(request.stage, ExecutionStage::FullPipeline);
        assert_eq!(request.profile_version, PUBLISHED_PROFILE_V0_7_0);
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
            ref_name: "test-branch".into(),
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
            ref_name: "test-branch".into(),
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
            ref_name: "test-branch".into(),
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
            ref_name: "test-branch".into(),
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
            "ref_name": "test-branch",
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
            "ref_name": "test-branch",
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
            EnvironmentConfig::Local { .. }
            | EnvironmentConfig::LocalOverlay { .. }
            | EnvironmentConfig::ProductionV2 { .. } => {
                panic!("expected production environment")
            }
        }
    }

    #[test]
    fn production_v2_request_accepts_direct_table_config() {
        let request: BatchRequest = serde_json::from_value(json!({
            "environment": {
                "kind": "production-v2",
                "block_store": {
                    "container_sas_url": "https://example.table.core.windows.net/archive-sync?sig=test"
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
            "ref_name": "test-branch",
            "items": [{
                "kind": "document",
                "path": "docs/sample.txt"
            }]
        }))
        .unwrap();

        assert!(request.validate().is_ok());
        match request.environment {
            EnvironmentConfig::ProductionV2 { block_store, .. } => {
                assert_eq!(
                    block_store.container_sas_url,
                    "https://example.table.core.windows.net/archive-sync?sig=test"
                );
                assert_eq!(block_store.filesystem_cache_root, None);
                assert_eq!(block_store.memory_cache_max_resident_blocks, None);
            }
            EnvironmentConfig::Local { .. }
            | EnvironmentConfig::LocalOverlay { .. }
            | EnvironmentConfig::Production { .. } => {
                panic!("expected production-v2 environment")
            }
        }
    }

    #[test]
    fn production_v2_request_rejects_non_empty_prefix() {
        let request = BatchRequest {
            environment: EnvironmentConfig::ProductionV2 {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.table.core.windows.net/archive-sync?sig=test".into(),
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
            ref_name: "test-branch".into(),
            items: vec![BatchItemConfig::Document {
                path: PathBuf::from("docs").join("sample.txt"),
                metadata: BTreeMap::new(),
            }],
        };

        assert!(matches!(
            request.validate(),
            Err(ConfigError::UnsupportedProductionV2BlockStorePrefix)
        ));
    }

    #[test]
    fn local_overlay_request_accepts_overlay_cache_layers_with_local_embedding() {
        let request: BatchRequest = serde_json::from_value(json!({
            "environment": {
                "kind": "local-overlay",
                "block_store": {
                    "container_sas_url": "https://example.blob.core.windows.net/archive-sync?sig=test",
                    "filesystem_cache_root": "cache",
                    "memory_cache_max_resident_blocks": 64
                },
                "embedding": {
                    "base_url": "http://localhost:8080"
                }
            },
            "embedding_spec": {
                "dims": 384,
                "encoding": "f32le"
            },
            "stage": "ingestion-and-embedding",
            "ref_name": "test-branch",
            "items": [{
                "kind": "document",
                "path": "docs/sample.txt"
            }]
        }))
        .unwrap();

        assert!(request.validate().is_ok());
        match request.environment {
            EnvironmentConfig::LocalOverlay {
                block_store,
                embedding,
            } => {
                assert_eq!(
                    block_store.filesystem_cache_root,
                    Some(PathBuf::from("cache"))
                );
                assert_eq!(block_store.memory_cache_max_resident_blocks, Some(64));
                assert_eq!(embedding.base_url, "http://localhost:8080");
            }
            EnvironmentConfig::Local { .. }
            | EnvironmentConfig::Production { .. }
            | EnvironmentConfig::ProductionV2 { .. } => {
                panic!("expected local-overlay environment")
            }
        }
    }

    #[test]
    fn local_overlay_request_reports_overlay_neutral_cache_error_text() {
        let request: BatchRequest = serde_json::from_value(json!({
            "environment": {
                "kind": "local-overlay",
                "block_store": {
                    "container_sas_url": "https://example.blob.core.windows.net/archive-sync?sig=test"
                },
                "embedding": {
                    "base_url": "http://localhost:8080"
                }
            },
            "embedding_spec": {
                "dims": 384,
                "encoding": "f32le"
            },
            "stage": "ingestion-and-embedding",
            "ref_name": "test-branch",
            "items": [{
                "kind": "document",
                "path": "docs/sample.txt"
            }]
        }))
        .unwrap();

        let error = request.validate().unwrap_err();
        assert_eq!(
            error.to_string(),
            "overlay block_store.filesystem_cache_root is required for the overlay-backed block store"
        );
    }

    #[test]
    fn production_resolve_mutable_ref_store_uses_ref_named_blob() {
        let environment = EnvironmentConfig::Production {
            block_store: ProductionBlockStoreConfig {
                container_sas_url: "https://example.blob.core.windows.net/archive-sync?sig=test"
                    .into(),
                prefix: None,
                filesystem_cache_root: Some(PathBuf::from("cache")),
                memory_cache_max_resident_blocks: Some(64),
            },
            embedding: ProductionEmbeddingConfig {
                endpoint: "https://example.openai.azure.com".into(),
                deployment: "embeddings".into(),
                api_version: default_azure_api_version(),
                api_key_env: None,
            },
        };

        assert_eq!(
            environment.resolve_mutable_ref_store(Path::new("request-root"), "feature/test"),
            Some(MutableRefStoreLocation::AzureBlob {
                url:
                    "https://example.blob.core.windows.net/archive-sync/refs/feature/test?sig=test"
                        .into(),
                display_path:
                    "https://example.blob.core.windows.net/archive-sync/refs/feature/test".into(),
            })
        );
    }

    #[test]
    fn production_v2_resolve_mutable_ref_store_uses_ref_named_table_entity() {
        let environment = EnvironmentConfig::ProductionV2 {
            block_store: ProductionBlockStoreConfig {
                container_sas_url: "https://example.table.core.windows.net/archive-sync?sig=test"
                    .into(),
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
        };

        assert_eq!(
            environment.resolve_mutable_ref_store(Path::new("request-root"), "feature/test"),
            Some(MutableRefStoreLocation::AzureTable {
                table_sas_url: "https://example.table.core.windows.net/archive-sync?sig=test"
                    .into(),
                display_path:
                    "https://example.table.core.windows.net/archive-sync/refs/feature/test".into(),
                partition_key: "refs".into(),
                row_key: "726566732f666561747572652f74657374".into(),
            })
        );
    }

    #[test]
    fn ref_name_segments_reject_urlish_windows_and_whitespace_characters() {
        for ref_name in [
            "feature/%2e%2e",
            "feature/branch:name",
            "feature/branch?preview",
            "feature/branch#draft",
            "feature/branch with space",
            "feature/\tbranch",
        ] {
            assert!(
                matches!(
                    normalized_ref_name_segments(ref_name),
                    Err(ConfigError::InvalidRefName)
                ),
                "expected invalid ref_name: {ref_name}"
            );
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
            "ref_name": "test-branch",
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
            ref_name: "test-branch".into(),
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
    fn production_v2_request_rejects_overlay_cache_fields() {
        let request = BatchRequest {
            environment: EnvironmentConfig::ProductionV2 {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.table.core.windows.net/archive-sync?sig=test".into(),
                    prefix: None,
                    filesystem_cache_root: Some("cache".into()),
                    memory_cache_max_resident_blocks: Some(64),
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
            ref_name: "test-branch".into(),
            items: vec![BatchItemConfig::Document {
                path: PathBuf::from("docs").join("sample.txt"),
                metadata: BTreeMap::new(),
            }],
        };

        assert!(matches!(
            request.validate(),
            Err(ConfigError::UnsupportedProductionV2FilesystemCacheRoot)
        ));
    }

    #[test]
    fn clustering_defaults_to_published_profile_v0_7_0() {
        let clustering = ClusteringConfigOverrides::default()
            .to_configured_clustering(
                default_profile_version(),
                &EnvironmentConfig::Local {
                    block_store_root: Path::new("blocks").to_path_buf(),
                    embedding: LocalEmbeddingConfig {
                        base_url: "http://127.0.0.1:8080".into(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 30,
                        max_retries: 5,
                        retry_delay_ms: 1_000,
                    },
                },
            )
            .unwrap();

        assert_eq!(clustering.profile_version, PUBLISHED_PROFILE_V0_7_0);
        assert_eq!(clustering.local_testing_cluster_count, None);
    }

    #[test]
    fn clustering_override_uses_request_profile_when_cli_omits_selector() {
        let clustering = ClusteringConfigOverrides::default()
            .to_configured_clustering(
                PublishedProfileVersion::new(0, 5, 0),
                &EnvironmentConfig::Local {
                    block_store_root: Path::new("blocks").to_path_buf(),
                    embedding: LocalEmbeddingConfig {
                        base_url: "http://127.0.0.1:8080".into(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 30,
                        max_retries: 5,
                        retry_delay_ms: 1_000,
                    },
                },
            )
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
            local_testing_cluster_count: None,
        }
        .to_configured_clustering(
            default_profile_version(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://127.0.0.1:8080".into(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 30,
                    max_retries: 5,
                    retry_delay_ms: 1_000,
                },
            },
        )
        .unwrap();

        assert_eq!(
            clustering.profile_version,
            PublishedProfileVersion::new(0, 5, 0)
        );
    }

    #[test]
    fn clustering_override_preserves_local_testing_cluster_count_for_local_environment() {
        let clustering = ClusteringConfigOverrides {
            profile_version: Some(PublishedProfileVersion::new(0, 6, 0)),
            local_testing_cluster_count: Some(32),
        }
        .to_configured_clustering(
            PublishedProfileVersion::new(0, 6, 0),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://127.0.0.1:8080".into(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 30,
                    max_retries: 5,
                    retry_delay_ms: 1_000,
                },
            },
        )
        .unwrap();

        assert_eq!(clustering.local_testing_cluster_count, Some(32));
    }

    #[test]
    fn clustering_override_preserves_local_testing_cluster_count_when_cli_selects_non_v2_over_request_v0_7_0()
     {
        let clustering = ClusteringConfigOverrides {
            profile_version: Some(PublishedProfileVersion::new(0, 6, 0)),
            local_testing_cluster_count: Some(32),
        }
        .to_configured_clustering(
            PublishedProfileVersion::new(0, 7, 0),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://127.0.0.1:8080".into(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 30,
                    max_retries: 5,
                    retry_delay_ms: 1_000,
                },
            },
        )
        .unwrap();

        assert_eq!(
            clustering.profile_version,
            PublishedProfileVersion::new(0, 6, 0)
        );
        assert_eq!(clustering.local_testing_cluster_count, Some(32));
    }

    #[test]
    fn clustering_override_rejects_local_testing_cluster_count_for_published_profile_v0_7_0() {
        let error = ClusteringConfigOverrides {
            profile_version: None,
            local_testing_cluster_count: Some(32),
        }
        .to_configured_clustering(
            default_profile_version(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://127.0.0.1:8080".into(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 30,
                    max_retries: 5,
                    retry_delay_ms: 1_000,
                },
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigError::LocalTestingClusterCountUnsupportedForPublishedProfileV0_7_0
        ));
    }

    #[test]
    fn clustering_override_rejects_local_testing_cluster_count_when_cli_selects_v0_7_0_over_request_non_v2()
     {
        let error = ClusteringConfigOverrides {
            profile_version: Some(PublishedProfileVersion::new(0, 7, 0)),
            local_testing_cluster_count: Some(32),
        }
        .to_configured_clustering(
            PublishedProfileVersion::new(0, 6, 0),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://127.0.0.1:8080".into(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 30,
                    max_retries: 5,
                    retry_delay_ms: 1_000,
                },
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigError::LocalTestingClusterCountUnsupportedForPublishedProfileV0_7_0
        ));
    }

    #[test]
    fn clustering_override_rejects_zero_local_testing_cluster_count() {
        let error = ClusteringConfigOverrides {
            profile_version: None,
            local_testing_cluster_count: Some(0),
        }
        .to_configured_clustering(
            default_profile_version(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://127.0.0.1:8080".into(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 30,
                    max_retries: 5,
                    retry_delay_ms: 1_000,
                },
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigError::InvalidLocalTestingClusterCount
        ));
    }

    #[test]
    fn clustering_override_rejects_local_testing_cluster_count_for_production() {
        let error = ClusteringConfigOverrides {
            profile_version: None,
            local_testing_cluster_count: Some(32),
        }
        .to_configured_clustering(
            default_profile_version(),
            &EnvironmentConfig::Production {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url: "https://example.test/container?sig=test".into(),
                    prefix: None,
                    filesystem_cache_root: Some(Path::new("cache").to_path_buf()),
                    memory_cache_max_resident_blocks: Some(1),
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://example.test".into(),
                    deployment: "text-embedding".into(),
                    api_version: "2024-02-01".into(),
                    api_key_env: Some("AZURE_OPENAI_API_KEY".into()),
                },
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigError::LocalTestingClusterCountRequiresLocalEnvironment
        ));
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
            "ref_name": "test-branch",
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
