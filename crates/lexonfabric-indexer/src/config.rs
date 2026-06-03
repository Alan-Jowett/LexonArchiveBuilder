use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ciborium::Value;
use lexongraph_block::EmbeddingSpec;
use lexongraph_indexer::{IndexItem, Metadata};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::resolver::ContentRef;

const DEFAULT_BLOCK_SIZE_TARGET: usize = 65_536;
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_MAX_RETRIES: u32 = 5;
const DEFAULT_RETRY_DELAY_MS: u64 = 1_000;

#[derive(Clone, Debug, Deserialize)]
pub struct BatchRequest {
    pub environment: EnvironmentConfig,
    pub embedding_spec: EmbeddingSpecConfig,
    #[serde(default = "default_block_size_target")]
    pub block_size_target: usize,
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

#[derive(Clone, Debug, Serialize)]
pub struct BatchSummary {
    pub root_id: String,
    pub block_ids: Vec<String>,
    pub block_count: usize,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("batch request must contain at least one item")]
    EmptyItems,
    #[error("local embedding base_url must not be empty")]
    MissingLocalEmbeddingBaseUrl,
}

impl BatchRequest {
    pub fn to_index_items(
        &self,
        request_dir: &Path,
    ) -> Result<Vec<IndexItem<ContentRef>>, ConfigError> {
        if self.items.is_empty() {
            return Err(ConfigError::EmptyItems);
        }

        Ok(self
            .items
            .iter()
            .map(|item| item.to_index_item(request_dir))
            .collect::<Vec<_>>())
    }

    pub fn to_embedding_spec(&self) -> EmbeddingSpec {
        self.embedding_spec.clone().into()
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
    fn to_index_item(&self, request_dir: &Path) -> IndexItem<ContentRef> {
        match self {
            Self::Mailbox { path, metadata } => {
                let resolved = resolve_path(request_dir, path);
                IndexItem {
                    metadata: metadata_to_lexongraph(metadata, "mailbox", &resolved),
                    content_ref: ContentRef::Mailbox { path: resolved },
                }
            }
            Self::Document { path, metadata } => {
                let resolved = resolve_path(request_dir, path);
                IndexItem {
                    metadata: metadata_to_lexongraph(metadata, "document", &resolved),
                    content_ref: ContentRef::Document { path: resolved },
                }
            }
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

fn metadata_to_lexongraph(
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

fn resolve_path(request_dir: &Path, candidate: &Path) -> PathBuf {
    if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        request_dir.join(candidate)
    }
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

fn default_azure_api_version() -> String {
    "2024-02-01".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

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
            items: vec![BatchItemConfig::Document {
                path: relative_document_path.clone(),
                metadata: BTreeMap::new(),
            }],
        };

        let items = request.to_index_items(&request_root).unwrap();

        match &items[0].content_ref {
            ContentRef::Document { path } => {
                assert_eq!(path, &request_root.join(relative_document_path));
            }
            ContentRef::Mailbox { .. } => panic!("expected a document content ref"),
        }
    }
}
