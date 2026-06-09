use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ciborium::Value;
use clap::{Args, ValueEnum};
use lexongraph_block::EmbeddingSpec;
use lexongraph_directional_pca::DirectionalPcaParams;
use lexongraph_streaming_indexer::{
    BalanceConstraints, DEFAULT_EMBEDDING_COUNT_CUTOFF, IndexItem, Metadata,
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
const DEFAULT_DIRECTIONAL_PCA_RETAINED_DIMENSION_COUNT: usize = 1;
const DEFAULT_DIRECTIONAL_PCA_VARIANCE_EXPONENT: f32 = 1.0;
const DEFAULT_DIRECTIONAL_PCA_TEMPERATURE: f32 = 1.0;
const DEFAULT_DIRECTIONAL_PCA_MIN_INPUT_COUNT: usize = 2;
const DEFAULT_DIRECTIONAL_PCA_MIN_EFFECTIVE_RANK: usize = 1;
const DEFAULT_DIRECTIONAL_PCA_MIN_CUMULATIVE_VARIANCE: f32 = 0.0;

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

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ClusteringMode {
    #[default]
    Aggregation,
    Divisive,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ClusteringProvider {
    #[default]
    AdapterClusteringPlanner,
    BuiltIn,
}

impl ClusteringProvider {
    fn as_str(self) -> &'static str {
        match self {
            Self::AdapterClusteringPlanner => "adapter-clustering-planner",
            Self::BuiltIn => "built-in",
        }
    }
}

impl std::fmt::Display for ClusteringProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Default, ValueEnum, PartialEq, Eq)]
pub enum ClusteringAlgorithm {
    #[default]
    Dcbc,
    DirectionalPca,
    Adaptive,
}

impl ClusteringAlgorithm {
    fn as_str(self) -> &'static str {
        match self {
            Self::Dcbc => "dcbc",
            Self::DirectionalPca => "directional-pca",
            Self::Adaptive => "adaptive",
        }
    }
}

impl std::fmt::Display for ClusteringAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Args, Clone, Debug, Default, PartialEq)]
pub struct ClusteringConfigOverrides {
    #[arg(long, value_enum)]
    pub clustering_provider: Option<ClusteringProvider>,
    #[arg(long, value_enum)]
    pub clustering_mode: Option<ClusteringMode>,
    #[arg(long, value_enum)]
    pub clustering_algorithm: Option<ClusteringAlgorithm>,
    #[arg(long)]
    pub clustering_cluster_count: Option<u32>,
    #[arg(long)]
    pub clustering_random_seed: Option<u64>,
    #[arg(long)]
    pub clustering_min_cluster_occupancy: Option<u32>,
    #[arg(long)]
    pub clustering_max_cluster_occupancy: Option<u32>,
    #[arg(long)]
    pub clustering_max_cluster_size_ratio: Option<f64>,
    #[arg(long)]
    pub clustering_soft_balance_penalty: Option<f64>,
    #[arg(long)]
    pub clustering_retained_dimension_count: Option<usize>,
    #[arg(long)]
    pub clustering_variance_exponent: Option<f32>,
    #[arg(long)]
    pub clustering_temperature: Option<f32>,
    #[arg(long)]
    pub clustering_min_input_count: Option<usize>,
    #[arg(long)]
    pub clustering_min_effective_rank: Option<usize>,
    #[arg(long)]
    pub clustering_min_cumulative_variance: Option<f32>,
    #[arg(long)]
    pub clustering_embedding_count_cutoff: Option<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ConfiguredClustering {
    Dcbc {
        provider: ClusteringProvider,
        mode: ClusteringMode,
        cluster_count: Option<u32>,
        balance_constraints: Option<BalanceConstraints>,
        random_seed: Option<u64>,
    },
    DirectionalPca {
        provider: ClusteringProvider,
        mode: ClusteringMode,
        cluster_count: Option<u32>,
        random_seed: Option<u64>,
        params: DirectionalPcaParams,
    },
    Adaptive {
        provider: ClusteringProvider,
        mode: ClusteringMode,
        cluster_count: Option<u32>,
        random_seed: Option<u64>,
        balance_constraints: Option<BalanceConstraints>,
        params: DirectionalPcaParams,
        embedding_count_cutoff: usize,
    },
}

impl ClusteringConfigOverrides {
    pub fn validate(&self) -> Result<(), ConfigError> {
        let provider = self.effective_provider();
        let algorithm = self.effective_algorithm();
        let mode = self.effective_mode();
        self.validate_shared_numeric_options()?;
        match (provider, algorithm) {
            (ClusteringProvider::AdapterClusteringPlanner, ClusteringAlgorithm::Dcbc) => {
                if mode == ClusteringMode::Divisive {
                    return Err(ConfigError::UnsupportedClusteringModeForProvider {
                        provider,
                        mode,
                    });
                }
                if let Some(option) = [
                    self.clustering_random_seed
                        .map(|_| "clustering_random_seed"),
                    self.clustering_min_cluster_occupancy
                        .map(|_| "clustering_min_cluster_occupancy"),
                    self.clustering_max_cluster_occupancy
                        .map(|_| "clustering_max_cluster_occupancy"),
                    self.clustering_max_cluster_size_ratio
                        .map(|_| "clustering_max_cluster_size_ratio"),
                    self.clustering_soft_balance_penalty
                        .map(|_| "clustering_soft_balance_penalty"),
                    self.clustering_retained_dimension_count
                        .map(|_| "clustering_retained_dimension_count"),
                    self.clustering_variance_exponent
                        .map(|_| "clustering_variance_exponent"),
                    self.clustering_temperature
                        .map(|_| "clustering_temperature"),
                    self.clustering_min_input_count
                        .map(|_| "clustering_min_input_count"),
                    self.clustering_min_effective_rank
                        .map(|_| "clustering_min_effective_rank"),
                    self.clustering_min_cumulative_variance
                        .map(|_| "clustering_min_cumulative_variance"),
                    self.clustering_embedding_count_cutoff
                        .map(|_| "clustering_embedding_count_cutoff"),
                ]
                .into_iter()
                .flatten()
                .next()
                {
                    return Err(ConfigError::UnsupportedClusteringOptionForProvider {
                        option,
                        provider,
                    });
                }
            }
            (ClusteringProvider::AdapterClusteringPlanner, ClusteringAlgorithm::DirectionalPca) => {
                return Err(ConfigError::UnsupportedClusteringAlgorithmForProvider {
                    provider,
                    algorithm,
                });
            }
            (ClusteringProvider::AdapterClusteringPlanner, ClusteringAlgorithm::Adaptive) => {
                return Err(ConfigError::UnsupportedClusteringAlgorithmForProvider {
                    provider,
                    algorithm,
                });
            }
            (ClusteringProvider::BuiltIn, ClusteringAlgorithm::Dcbc) => {
                if self.clustering_retained_dimension_count.is_some() {
                    return Err(ConfigError::UnsupportedClusteringOptionForAlgorithm {
                        option: "clustering_retained_dimension_count",
                        algorithm,
                    });
                }
                if self.clustering_variance_exponent.is_some() {
                    return Err(ConfigError::UnsupportedClusteringOptionForAlgorithm {
                        option: "clustering_variance_exponent",
                        algorithm,
                    });
                }
                if self.clustering_temperature.is_some() {
                    return Err(ConfigError::UnsupportedClusteringOptionForAlgorithm {
                        option: "clustering_temperature",
                        algorithm,
                    });
                }
                if self.clustering_min_input_count.is_some() {
                    return Err(ConfigError::UnsupportedClusteringOptionForAlgorithm {
                        option: "clustering_min_input_count",
                        algorithm,
                    });
                }
                if self.clustering_min_effective_rank.is_some() {
                    return Err(ConfigError::UnsupportedClusteringOptionForAlgorithm {
                        option: "clustering_min_effective_rank",
                        algorithm,
                    });
                }
                if self.clustering_min_cumulative_variance.is_some() {
                    return Err(ConfigError::UnsupportedClusteringOptionForAlgorithm {
                        option: "clustering_min_cumulative_variance",
                        algorithm,
                    });
                }
                if self.clustering_embedding_count_cutoff.is_some() {
                    return Err(ConfigError::UnsupportedClusteringOptionForAlgorithm {
                        option: "clustering_embedding_count_cutoff",
                        algorithm,
                    });
                }
            }
            (ClusteringProvider::BuiltIn, ClusteringAlgorithm::DirectionalPca) => {
                if self.clustering_min_cluster_occupancy.is_some() {
                    return Err(ConfigError::UnsupportedClusteringOptionForAlgorithm {
                        option: "clustering_min_cluster_occupancy",
                        algorithm,
                    });
                }
                if self.clustering_max_cluster_occupancy.is_some() {
                    return Err(ConfigError::UnsupportedClusteringOptionForAlgorithm {
                        option: "clustering_max_cluster_occupancy",
                        algorithm,
                    });
                }
                if self.clustering_max_cluster_size_ratio.is_some() {
                    return Err(ConfigError::UnsupportedClusteringOptionForAlgorithm {
                        option: "clustering_max_cluster_size_ratio",
                        algorithm,
                    });
                }
                if self.clustering_soft_balance_penalty.is_some() {
                    return Err(ConfigError::UnsupportedClusteringOptionForAlgorithm {
                        option: "clustering_soft_balance_penalty",
                        algorithm,
                    });
                }
                if self.clustering_embedding_count_cutoff.is_some() {
                    return Err(ConfigError::UnsupportedClusteringOptionForAlgorithm {
                        option: "clustering_embedding_count_cutoff",
                        algorithm,
                    });
                }
                self.validate_directional_pca_numeric_options()?;
            }
            (ClusteringProvider::BuiltIn, ClusteringAlgorithm::Adaptive) => {
                self.validate_directional_pca_numeric_options()?;
                self.validate_adaptive_numeric_options()?;
            }
        }

        Ok(())
    }

    pub(crate) fn to_configured_clustering(&self) -> Result<ConfiguredClustering, ConfigError> {
        self.validate()?;
        let provider = self.effective_provider();
        let mode = self.effective_mode();
        let cluster_count = self.clustering_cluster_count;
        let random_seed = self.clustering_random_seed;
        Ok(match (provider, self.effective_algorithm()) {
            (ClusteringProvider::AdapterClusteringPlanner, ClusteringAlgorithm::Dcbc) => {
                ConfiguredClustering::Dcbc {
                    provider,
                    mode,
                    cluster_count,
                    balance_constraints: None,
                    random_seed: None,
                }
            }
            (ClusteringProvider::BuiltIn, ClusteringAlgorithm::Dcbc) => {
                ConfiguredClustering::Dcbc {
                    provider,
                    mode,
                    cluster_count,
                    balance_constraints: self.to_balance_constraints(),
                    random_seed,
                }
            }
            (ClusteringProvider::BuiltIn, ClusteringAlgorithm::DirectionalPca) => {
                ConfiguredClustering::DirectionalPca {
                    provider,
                    mode,
                    cluster_count,
                    random_seed,
                    params: self.directional_pca_params(),
                }
            }
            (ClusteringProvider::BuiltIn, ClusteringAlgorithm::Adaptive) => {
                ConfiguredClustering::Adaptive {
                    provider,
                    mode,
                    cluster_count,
                    random_seed,
                    balance_constraints: self.to_balance_constraints(),
                    params: self.directional_pca_params(),
                    embedding_count_cutoff: self
                        .clustering_embedding_count_cutoff
                        .unwrap_or(DEFAULT_EMBEDDING_COUNT_CUTOFF),
                }
            }
            (
                ClusteringProvider::AdapterClusteringPlanner,
                ClusteringAlgorithm::DirectionalPca | ClusteringAlgorithm::Adaptive,
            ) => {
                unreachable!("validated incompatible provider and algorithm")
            }
        })
    }

    fn effective_provider(&self) -> ClusteringProvider {
        self.clustering_provider.unwrap_or_default()
    }

    fn effective_algorithm(&self) -> ClusteringAlgorithm {
        self.clustering_algorithm.unwrap_or_default()
    }

    fn effective_mode(&self) -> ClusteringMode {
        self.clustering_mode.unwrap_or_default()
    }

    fn directional_pca_params(&self) -> DirectionalPcaParams {
        DirectionalPcaParams {
            retained_dimension_count: self
                .clustering_retained_dimension_count
                .unwrap_or(DEFAULT_DIRECTIONAL_PCA_RETAINED_DIMENSION_COUNT),
            variance_exponent: self
                .clustering_variance_exponent
                .unwrap_or(DEFAULT_DIRECTIONAL_PCA_VARIANCE_EXPONENT),
            temperature: self
                .clustering_temperature
                .unwrap_or(DEFAULT_DIRECTIONAL_PCA_TEMPERATURE),
            min_input_count: self
                .clustering_min_input_count
                .unwrap_or(DEFAULT_DIRECTIONAL_PCA_MIN_INPUT_COUNT),
            min_effective_rank: self
                .clustering_min_effective_rank
                .unwrap_or(DEFAULT_DIRECTIONAL_PCA_MIN_EFFECTIVE_RANK),
            min_cumulative_variance: self
                .clustering_min_cumulative_variance
                .unwrap_or(DEFAULT_DIRECTIONAL_PCA_MIN_CUMULATIVE_VARIANCE),
        }
    }

    fn to_balance_constraints(&self) -> Option<BalanceConstraints> {
        let constraints = BalanceConstraints {
            min_cluster_occupancy: self.clustering_min_cluster_occupancy,
            max_cluster_occupancy: self.clustering_max_cluster_occupancy,
            max_cluster_size_ratio: self.clustering_max_cluster_size_ratio,
            soft_balance_penalty: self.clustering_soft_balance_penalty,
        };
        if constraints.min_cluster_occupancy.is_none()
            && constraints.max_cluster_occupancy.is_none()
            && constraints.max_cluster_size_ratio.is_none()
            && constraints.soft_balance_penalty.is_none()
        {
            None
        } else {
            Some(constraints)
        }
    }

    fn validate_shared_numeric_options(&self) -> Result<(), ConfigError> {
        if matches!(self.clustering_cluster_count, Some(0)) {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_cluster_count",
                message: "must be at least 1".into(),
            });
        }
        if matches!(self.clustering_min_cluster_occupancy, Some(0)) {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_min_cluster_occupancy",
                message: "must be at least 1 when provided".into(),
            });
        }
        if matches!(self.clustering_max_cluster_occupancy, Some(0)) {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_max_cluster_occupancy",
                message: "must be at least 1 when provided".into(),
            });
        }
        if let (Some(min_cluster_occupancy), Some(max_cluster_occupancy)) = (
            self.clustering_min_cluster_occupancy,
            self.clustering_max_cluster_occupancy,
        ) && min_cluster_occupancy > max_cluster_occupancy
        {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_min_cluster_occupancy",
                message: "cannot exceed clustering_max_cluster_occupancy".into(),
            });
        }
        if let Some(max_cluster_size_ratio) = self.clustering_max_cluster_size_ratio
            && (!max_cluster_size_ratio.is_finite() || max_cluster_size_ratio <= 0.0)
        {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_max_cluster_size_ratio",
                message: "must be finite and positive".into(),
            });
        }
        if let Some(soft_balance_penalty) = self.clustering_soft_balance_penalty
            && (!soft_balance_penalty.is_finite() || soft_balance_penalty < 0.0)
        {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_soft_balance_penalty",
                message: "must be finite and non-negative".into(),
            });
        }

        Ok(())
    }

    fn validate_directional_pca_numeric_options(&self) -> Result<(), ConfigError> {
        let retained_dimension_count = self
            .clustering_retained_dimension_count
            .unwrap_or(DEFAULT_DIRECTIONAL_PCA_RETAINED_DIMENSION_COUNT);
        if retained_dimension_count == 0 {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_retained_dimension_count",
                message: "must be at least 1".into(),
            });
        }
        if let Some(cluster_count) = self.clustering_cluster_count
            && retained_dimension_count > cluster_count as usize
        {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_retained_dimension_count",
                message: "cannot exceed clustering_cluster_count".into(),
            });
        }
        if let Some(variance_exponent) = self.clustering_variance_exponent
            && (!variance_exponent.is_finite() || variance_exponent < 0.0)
        {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_variance_exponent",
                message: "must be finite and non-negative".into(),
            });
        }
        if let Some(temperature) = self.clustering_temperature
            && (!temperature.is_finite() || temperature <= 0.0)
        {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_temperature",
                message: "must be finite and positive".into(),
            });
        }
        if let Some(min_input_count) = self.clustering_min_input_count
            && min_input_count < 2
        {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_min_input_count",
                message: "must be at least 2".into(),
            });
        }
        let min_effective_rank = self
            .clustering_min_effective_rank
            .unwrap_or(DEFAULT_DIRECTIONAL_PCA_MIN_EFFECTIVE_RANK);
        if min_effective_rank == 0 || min_effective_rank > retained_dimension_count {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_min_effective_rank",
                message: "must be between 1 and clustering_retained_dimension_count".into(),
            });
        }
        if let Some(min_cumulative_variance) = self.clustering_min_cumulative_variance
            && (!min_cumulative_variance.is_finite()
                || !(0.0..=1.0).contains(&min_cumulative_variance))
        {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_min_cumulative_variance",
                message: "must be finite and in [0, 1]".into(),
            });
        }

        Ok(())
    }

    fn validate_adaptive_numeric_options(&self) -> Result<(), ConfigError> {
        if matches!(self.clustering_embedding_count_cutoff, Some(0)) {
            return Err(ConfigError::InvalidClusteringOption {
                option: "clustering_embedding_count_cutoff",
                message: "must be at least 1".into(),
            });
        }

        Ok(())
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
    pub account_url: String,
    pub container: String,
    #[serde(default)]
    pub prefix: Option<String>,
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
    #[error("clustering option {option} is not supported for algorithm {algorithm}")]
    UnsupportedClusteringOptionForAlgorithm {
        option: &'static str,
        algorithm: ClusteringAlgorithm,
    },
    #[error("clustering algorithm {algorithm} is not supported for provider {provider}")]
    UnsupportedClusteringAlgorithmForProvider {
        provider: ClusteringProvider,
        algorithm: ClusteringAlgorithm,
    },
    #[error("clustering mode {mode:?} is not supported for provider {provider}")]
    UnsupportedClusteringModeForProvider {
        provider: ClusteringProvider,
        mode: ClusteringMode,
    },
    #[error("clustering option {option} is not supported for provider {provider}")]
    UnsupportedClusteringOptionForProvider {
        option: &'static str,
        provider: ClusteringProvider,
    },
    #[error("invalid clustering option {option}: {message}")]
    InvalidClusteringOption {
        option: &'static str,
        message: String,
    },
}

impl BatchRequest {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.stage.includes_ingestion() && self.items.is_empty() {
            return Err(ConfigError::EmptyItems);
        }
        if matches!(self.max_concurrency, Some(0)) {
            return Err(ConfigError::InvalidMaxConcurrency);
        }
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
            max_concurrency: None,
            items: vec![BatchItemConfig::Document {
                path: PathBuf::from("docs").join("sample.txt"),
                metadata: BTreeMap::new(),
            }],
        };

        assert!(request.validate().is_ok());
    }

    #[test]
    fn clustering_defaults_to_adapter_aggregation_dcbc_with_no_explicit_cli_options() {
        let clustering = ClusteringConfigOverrides::default()
            .to_configured_clustering()
            .unwrap();

        match clustering {
            ConfiguredClustering::Dcbc {
                provider,
                mode,
                cluster_count,
                balance_constraints,
                random_seed,
            } => {
                assert_eq!(provider, ClusteringProvider::AdapterClusteringPlanner);
                assert_eq!(mode, ClusteringMode::Aggregation);
                assert_eq!(cluster_count, None);
                assert!(balance_constraints.is_none());
                assert_eq!(random_seed, None);
            }
            ConfiguredClustering::DirectionalPca { .. } => {
                panic!("expected default clustering algorithm to be dcbc")
            }
            ConfiguredClustering::Adaptive { .. } => {
                panic!("expected default clustering algorithm to be dcbc")
            }
        }
    }

    #[test]
    fn directional_pca_defaults_are_applied_when_algorithm_is_selected() {
        let clustering = ClusteringConfigOverrides {
            clustering_provider: Some(ClusteringProvider::BuiltIn),
            clustering_algorithm: Some(ClusteringAlgorithm::DirectionalPca),
            ..ClusteringConfigOverrides::default()
        }
        .to_configured_clustering()
        .unwrap();

        match clustering {
            ConfiguredClustering::DirectionalPca {
                provider,
                mode,
                cluster_count,
                random_seed,
                params,
            } => {
                assert_eq!(provider, ClusteringProvider::BuiltIn);
                assert_eq!(mode, ClusteringMode::Aggregation);
                assert_eq!(cluster_count, None);
                assert_eq!(random_seed, None);
                assert_eq!(
                    params,
                    DirectionalPcaParams {
                        retained_dimension_count: DEFAULT_DIRECTIONAL_PCA_RETAINED_DIMENSION_COUNT,
                        variance_exponent: DEFAULT_DIRECTIONAL_PCA_VARIANCE_EXPONENT,
                        temperature: DEFAULT_DIRECTIONAL_PCA_TEMPERATURE,
                        min_input_count: DEFAULT_DIRECTIONAL_PCA_MIN_INPUT_COUNT,
                        min_effective_rank: DEFAULT_DIRECTIONAL_PCA_MIN_EFFECTIVE_RANK,
                        min_cumulative_variance: DEFAULT_DIRECTIONAL_PCA_MIN_CUMULATIVE_VARIANCE,
                    }
                );
            }
            ConfiguredClustering::Dcbc { .. } => {
                panic!("expected directional-pca settings when that algorithm is selected")
            }
            ConfiguredClustering::Adaptive { .. } => {
                panic!("expected directional-pca settings when that algorithm is selected")
            }
        }
    }

    #[test]
    fn adaptive_defaults_are_applied_when_algorithm_is_selected() {
        let clustering = ClusteringConfigOverrides {
            clustering_provider: Some(ClusteringProvider::BuiltIn),
            clustering_algorithm: Some(ClusteringAlgorithm::Adaptive),
            ..ClusteringConfigOverrides::default()
        }
        .to_configured_clustering()
        .unwrap();

        match clustering {
            ConfiguredClustering::Adaptive {
                provider,
                mode,
                cluster_count,
                random_seed,
                balance_constraints,
                params,
                embedding_count_cutoff,
            } => {
                assert_eq!(provider, ClusteringProvider::BuiltIn);
                assert_eq!(mode, ClusteringMode::Aggregation);
                assert_eq!(cluster_count, None);
                assert_eq!(random_seed, None);
                assert!(balance_constraints.is_none());
                assert_eq!(
                    params,
                    DirectionalPcaParams {
                        retained_dimension_count: DEFAULT_DIRECTIONAL_PCA_RETAINED_DIMENSION_COUNT,
                        variance_exponent: DEFAULT_DIRECTIONAL_PCA_VARIANCE_EXPONENT,
                        temperature: DEFAULT_DIRECTIONAL_PCA_TEMPERATURE,
                        min_input_count: DEFAULT_DIRECTIONAL_PCA_MIN_INPUT_COUNT,
                        min_effective_rank: DEFAULT_DIRECTIONAL_PCA_MIN_EFFECTIVE_RANK,
                        min_cumulative_variance: DEFAULT_DIRECTIONAL_PCA_MIN_CUMULATIVE_VARIANCE,
                    }
                );
                assert_eq!(embedding_count_cutoff, DEFAULT_EMBEDDING_COUNT_CUTOFF);
            }
            ConfiguredClustering::Dcbc { .. } => {
                panic!("expected adaptive settings when that algorithm is selected")
            }
            ConfiguredClustering::DirectionalPca { .. } => {
                panic!("expected adaptive settings when that algorithm is selected")
            }
        }
    }

    #[test]
    fn divisive_mode_is_applied_when_selected() {
        let clustering = ClusteringConfigOverrides {
            clustering_provider: Some(ClusteringProvider::BuiltIn),
            clustering_mode: Some(ClusteringMode::Divisive),
            ..ClusteringConfigOverrides::default()
        }
        .to_configured_clustering()
        .unwrap();

        match clustering {
            ConfiguredClustering::Dcbc { mode, .. } => {
                assert_eq!(mode, ClusteringMode::Divisive);
            }
            ConfiguredClustering::DirectionalPca { .. } => {
                panic!("expected default clustering algorithm to remain dcbc")
            }
            ConfiguredClustering::Adaptive { .. } => {
                panic!("expected default clustering algorithm to remain dcbc")
            }
        }
    }

    #[test]
    fn dcbc_rejects_directional_pca_only_options() {
        let error = ClusteringConfigOverrides {
            clustering_provider: Some(ClusteringProvider::BuiltIn),
            clustering_retained_dimension_count: Some(1),
            ..ClusteringConfigOverrides::default()
        }
        .validate()
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigError::UnsupportedClusteringOptionForAlgorithm {
                option: "clustering_retained_dimension_count",
                algorithm: ClusteringAlgorithm::Dcbc,
            }
        ));
    }

    #[test]
    fn directional_pca_rejects_dcbc_only_options() {
        let error = ClusteringConfigOverrides {
            clustering_provider: Some(ClusteringProvider::BuiltIn),
            clustering_algorithm: Some(ClusteringAlgorithm::DirectionalPca),
            clustering_min_cluster_occupancy: Some(1),
            ..ClusteringConfigOverrides::default()
        }
        .validate()
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigError::UnsupportedClusteringOptionForAlgorithm {
                option: "clustering_min_cluster_occupancy",
                algorithm: ClusteringAlgorithm::DirectionalPca,
            }
        ));
    }

    #[test]
    fn directional_pca_requires_retained_dimension_count_not_exceed_cluster_count() {
        let error = ClusteringConfigOverrides {
            clustering_provider: Some(ClusteringProvider::BuiltIn),
            clustering_algorithm: Some(ClusteringAlgorithm::DirectionalPca),
            clustering_cluster_count: Some(2),
            clustering_retained_dimension_count: Some(3),
            ..ClusteringConfigOverrides::default()
        }
        .to_configured_clustering()
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigError::InvalidClusteringOption {
                option: "clustering_retained_dimension_count",
                ..
            }
        ));
    }

    #[test]
    fn omitted_directional_pca_cluster_count_allows_larger_retained_dimension_count() {
        let clustering = ClusteringConfigOverrides {
            clustering_provider: Some(ClusteringProvider::BuiltIn),
            clustering_algorithm: Some(ClusteringAlgorithm::DirectionalPca),
            clustering_retained_dimension_count: Some(3),
            ..ClusteringConfigOverrides::default()
        }
        .to_configured_clustering()
        .unwrap();

        match clustering {
            ConfiguredClustering::DirectionalPca {
                cluster_count,
                params,
                ..
            } => {
                assert_eq!(cluster_count, None);
                assert_eq!(params.retained_dimension_count, 3);
            }
            ConfiguredClustering::Dcbc { .. } => {
                panic!("expected directional-pca settings when that algorithm is selected")
            }
            ConfiguredClustering::Adaptive { .. } => {
                panic!("expected directional-pca settings when that algorithm is selected")
            }
        }
    }

    #[test]
    fn adapter_provider_rejects_divisive_mode() {
        let error = ClusteringConfigOverrides {
            clustering_mode: Some(ClusteringMode::Divisive),
            ..ClusteringConfigOverrides::default()
        }
        .validate()
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigError::UnsupportedClusteringModeForProvider {
                provider: ClusteringProvider::AdapterClusteringPlanner,
                mode: ClusteringMode::Divisive,
            }
        ));
    }

    #[test]
    fn adapter_provider_rejects_directional_pca() {
        let error = ClusteringConfigOverrides {
            clustering_algorithm: Some(ClusteringAlgorithm::DirectionalPca),
            ..ClusteringConfigOverrides::default()
        }
        .validate()
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigError::UnsupportedClusteringAlgorithmForProvider {
                provider: ClusteringProvider::AdapterClusteringPlanner,
                algorithm: ClusteringAlgorithm::DirectionalPca,
            }
        ));
    }

    #[test]
    fn adapter_provider_rejects_adaptive() {
        let error = ClusteringConfigOverrides {
            clustering_algorithm: Some(ClusteringAlgorithm::Adaptive),
            ..ClusteringConfigOverrides::default()
        }
        .validate()
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigError::UnsupportedClusteringAlgorithmForProvider {
                provider: ClusteringProvider::AdapterClusteringPlanner,
                algorithm: ClusteringAlgorithm::Adaptive,
            }
        ));
    }

    #[test]
    fn adaptive_rejects_zero_embedding_count_cutoff() {
        let error = ClusteringConfigOverrides {
            clustering_provider: Some(ClusteringProvider::BuiltIn),
            clustering_algorithm: Some(ClusteringAlgorithm::Adaptive),
            clustering_embedding_count_cutoff: Some(0),
            ..ClusteringConfigOverrides::default()
        }
        .validate()
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigError::InvalidClusteringOption {
                option: "clustering_embedding_count_cutoff",
                ..
            }
        ));
    }
}
