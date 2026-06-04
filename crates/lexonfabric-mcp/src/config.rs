use std::path::{Path, PathBuf};

use lexonfabric_indexer::config::{EmbeddingSpecConfig, EnvironmentConfig};
use serde::Deserialize;
use thiserror::Error;

const DEFAULT_TOP_K: usize = 5;
const DEFAULT_TRAVERSAL_WIDTH: usize = 3;

#[derive(Clone, Debug, Deserialize)]
pub struct McpConfig {
    pub environment: EnvironmentConfig,
    pub embedding_spec: EmbeddingSpecConfig,
    pub index: IndexConfig,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
    #[serde(default = "default_traversal_width")]
    pub traversal_width: usize,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum IndexConfig {
    SummaryFile { path: PathBuf },
    RootId { root_id: String },
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("top_k must be at least 1")]
    InvalidTopK,
    #[error("traversal_width must be at least 1")]
    InvalidTraversalWidth,
}

impl McpConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.top_k == 0 {
            return Err(ConfigError::InvalidTopK);
        }
        if self.traversal_width == 0 {
            return Err(ConfigError::InvalidTraversalWidth);
        }
        Ok(())
    }

    pub fn resolve_summary_path(&self, request_dir: &Path) -> Option<PathBuf> {
        match &self.index {
            IndexConfig::SummaryFile { path } => Some(resolve_path(request_dir, path)),
            IndexConfig::RootId { .. } => None,
        }
    }

    pub fn root_id_literal(&self) -> Option<&str> {
        match &self.index {
            IndexConfig::SummaryFile { .. } => None,
            IndexConfig::RootId { root_id } => Some(root_id.as_str()),
        }
    }
}

fn resolve_path(request_dir: &Path, candidate: &Path) -> PathBuf {
    if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        request_dir.join(candidate)
    }
}

fn default_top_k() -> usize {
    DEFAULT_TOP_K
}

fn default_traversal_width() -> usize {
    DEFAULT_TRAVERSAL_WIDTH
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use lexonfabric_indexer::config::LocalEmbeddingConfig;

    #[test]
    fn relative_summary_paths_are_resolved_against_config_directory() {
        let config = McpConfig {
            environment: EnvironmentConfig::Local {
                block_store_root: PathBuf::from("blocks"),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://localhost:8080".into(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 30,
                    max_retries: 1,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 384,
                encoding: "f32le".into(),
            },
            index: IndexConfig::SummaryFile {
                path: PathBuf::from("output").join("summary.json"),
            },
            top_k: default_top_k(),
            traversal_width: default_traversal_width(),
        };

        let resolved = config
            .resolve_summary_path(Path::new("examples").join("local").as_path())
            .unwrap();

        assert_eq!(
            resolved,
            Path::new("examples")
                .join("local")
                .join("output")
                .join("summary.json")
        );
    }
}
