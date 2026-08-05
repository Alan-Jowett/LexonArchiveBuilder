// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::path::{Path, PathBuf};

use lexonarchivebuilder_indexer::config::{
    ConfigError as IndexerConfigError, DEFAULT_LOCAL_EMBEDDING_MODEL, DEFAULT_MAX_RETRIES,
    DEFAULT_REQUEST_TIMEOUT_SECS, DEFAULT_RETRY_DELAY_MS, EmbeddingSpecConfig, EnvironmentConfig,
};
use serde::Deserialize;
use thiserror::Error;

const DEFAULT_TOP_K: usize = 5;
const DEFAULT_TRAVERSAL_WIDTH: usize = 3;
pub const DEFAULT_SEARCH_CHUNKS_DESCRIPTION: &str =
    "Search indexed LexonArchiveBuilder chunks in the configured block store";
pub const DEFAULT_GET_DOCUMENT_DESCRIPTION: &str =
    "Request a named document from the configured LexonArchiveBuilder index";
pub const DEFAULT_GET_EMAIL_DESCRIPTION: &str =
    "Retrieve an email entry from a search result leaf_block_id";
pub const DEFAULT_GET_THREAD_DESCRIPTION: &str =
    "Request a named thread from the configured LexonArchiveBuilder index";

#[derive(Clone, Debug, Deserialize)]
pub struct McpConfig {
    pub environment: McpEnvironmentConfig,
    pub embedding_spec: EmbeddingSpecConfig,
    pub index: IndexConfig,
    #[serde(default)]
    pub tool_descriptions: ToolDescriptionsConfig,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
    #[serde(default = "default_traversal_width")]
    pub traversal_width: usize,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDescriptionsConfig {
    pub search_chunks: Option<String>,
    pub get_document: Option<String>,
    pub get_email: Option<String>,
    pub get_thread: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum McpEnvironmentConfig {
    Shared(EnvironmentConfig),
    GatewayHttp3(GatewayHttp3McpEnvironmentConfig),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayHttp3McpEnvironmentConfig {
    pub kind: GatewayHttp3Kind,
    pub gateway_dns_name: String,
    #[serde(default = "default_gateway_model")]
    pub model: String,
    #[serde(default = "default_gateway_request_timeout_secs")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_gateway_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_gateway_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GatewayHttp3Kind {
    GatewayHttp3,
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
    #[error("gateway_dns_name must not be empty")]
    EmptyGatewayDnsName,
    #[error("tool description for {tool_name} must not be empty")]
    EmptyToolDescription { tool_name: &'static str },
    #[error(transparent)]
    IndexerConfig(#[from] IndexerConfigError),
}

impl McpConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.top_k == 0 {
            return Err(ConfigError::InvalidTopK);
        }
        if self.traversal_width == 0 {
            return Err(ConfigError::InvalidTraversalWidth);
        }
        match &self.environment {
            McpEnvironmentConfig::Shared(environment) => environment.validate()?,
            McpEnvironmentConfig::GatewayHttp3(gateway) => {
                if gateway.gateway_dns_name.trim().is_empty() {
                    return Err(ConfigError::EmptyGatewayDnsName);
                }
            }
        }
        for (tool_name, description) in [
            (
                "search_chunks",
                self.tool_descriptions.search_chunks.as_deref(),
            ),
            (
                "get_document",
                self.tool_descriptions.get_document.as_deref(),
            ),
            ("get_email", self.tool_descriptions.get_email.as_deref()),
            ("get_thread", self.tool_descriptions.get_thread.as_deref()),
        ] {
            if matches!(description, Some(value) if value.trim().is_empty()) {
                return Err(ConfigError::EmptyToolDescription { tool_name });
            }
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

    pub fn tool_description(&self, tool_name: &str) -> &str {
        match tool_name {
            "search_chunks" => self
                .tool_descriptions
                .search_chunks
                .as_deref()
                .unwrap_or(DEFAULT_SEARCH_CHUNKS_DESCRIPTION),
            "get_document" => self
                .tool_descriptions
                .get_document
                .as_deref()
                .unwrap_or(DEFAULT_GET_DOCUMENT_DESCRIPTION),
            "get_email" => self
                .tool_descriptions
                .get_email
                .as_deref()
                .unwrap_or(DEFAULT_GET_EMAIL_DESCRIPTION),
            "get_thread" => self
                .tool_descriptions
                .get_thread
                .as_deref()
                .unwrap_or(DEFAULT_GET_THREAD_DESCRIPTION),
            _ => panic!("unknown registered MCP tool {tool_name}"),
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

fn default_gateway_model() -> String {
    DEFAULT_LOCAL_EMBEDDING_MODEL.to_string()
}

fn default_gateway_request_timeout_secs() -> u64 {
    DEFAULT_REQUEST_TIMEOUT_SECS
}

fn default_gateway_max_retries() -> u32 {
    DEFAULT_MAX_RETRIES
}

fn default_gateway_retry_delay_ms() -> u64 {
    DEFAULT_RETRY_DELAY_MS
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use lexonarchivebuilder_indexer::config::LocalEmbeddingConfig;
    use lexonarchivebuilder_indexer::config::ProductionEmbeddingConfig;

    #[test]
    fn relative_summary_paths_are_resolved_against_config_directory() {
        let config = McpConfig {
            environment: McpEnvironmentConfig::Shared(EnvironmentConfig::Local {
                block_store_root: PathBuf::from("blocks"),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://localhost:8080".into(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 30,
                    max_retries: 1,
                    retry_delay_ms: 1,
                },
            }),
            embedding_spec: EmbeddingSpecConfig {
                dims: 384,
                encoding: "f32le".into(),
            },
            index: IndexConfig::SummaryFile {
                path: PathBuf::from("output").join("summary.json"),
            },
            tool_descriptions: ToolDescriptionsConfig::default(),
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

    #[test]
    fn production_config_requires_overlay_block_store_fields() {
        let config = McpConfig {
            environment: McpEnvironmentConfig::Shared(EnvironmentConfig::Production {
                block_store: lexonarchivebuilder_indexer::config::ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.blob.core.windows.net/archive-sync?sig=test".into(),
                    prefix: None,
                    filesystem_cache_root: None,
                    memory_cache_max_resident_blocks: None,
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://example.openai.azure.com".into(),
                    deployment: "embeddings".into(),
                    api_version: "2024-02-01".into(),
                    api_key_env: None,
                },
            }),
            embedding_spec: EmbeddingSpecConfig {
                dims: 384,
                encoding: "f32le".into(),
            },
            index: IndexConfig::RootId {
                root_id: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff".into(),
            },
            tool_descriptions: ToolDescriptionsConfig::default(),
            top_k: default_top_k(),
            traversal_width: default_traversal_width(),
        };

        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("filesystem_cache_root"));
    }

    #[test]
    fn gateway_config_uses_shared_embedding_defaults() {
        let config: McpConfig = serde_json::from_str(
            r#"{
                "environment": {
                    "kind": "gateway-http3",
                    "gateway_dns_name": "gateway.example.test"
                },
                "embedding_spec": { "dims": 384, "encoding": "f32le" },
                "index": {
                    "kind": "root-id",
                    "root_id": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                }
            }"#,
        )
        .unwrap();

        let McpEnvironmentConfig::GatewayHttp3(gateway) = config.environment else {
            panic!("expected gateway-http3 environment");
        };
        assert_eq!(gateway.model, DEFAULT_LOCAL_EMBEDDING_MODEL);
        assert_eq!(gateway.request_timeout_secs, DEFAULT_REQUEST_TIMEOUT_SECS);
        assert_eq!(gateway.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(gateway.retry_delay_ms, DEFAULT_RETRY_DELAY_MS);
    }

    #[test]
    fn gateway_config_rejects_independent_embedding_settings() {
        for (field, value) in [
            ("base_url", "http://localhost:8080"),
            ("endpoint", "https://example.test/embeddings"),
            ("api_key_env", "EMBEDDING_API_KEY"),
        ] {
            let mut environment = serde_json::json!({
                "kind": "gateway-http3",
                "gateway_dns_name": "gateway.example.test"
            });
            environment
                .as_object_mut()
                .unwrap()
                .insert(field.to_string(), serde_json::Value::String(value.into()));
            let request = serde_json::json!({
                "environment": environment,
                "embedding_spec": { "dims": 384, "encoding": "f32le" },
                "index": {
                    "kind": "root-id",
                    "root_id": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                }
            });
            assert!(
                serde_json::from_value::<McpConfig>(request).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn gateway_config_rejects_empty_dns_name() {
        let config: McpConfig = serde_json::from_str(
            r#"{
                "environment": {
                    "kind": "gateway-http3",
                    "gateway_dns_name": " "
                },
                "embedding_spec": { "dims": 384, "encoding": "f32le" },
                "index": {
                    "kind": "root-id",
                    "root_id": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                }
            }"#,
        )
        .unwrap();

        assert!(matches!(
            config.validate(),
            Err(ConfigError::EmptyGatewayDnsName)
        ));
    }

    #[test]
    fn tool_descriptions_override_configured_tools_only() {
        let config: McpConfig = serde_json::from_str(
            r#"{
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": { "base_url": "http://localhost:8080" }
                },
                "embedding_spec": { "dims": 384, "encoding": "f32le" },
                "index": {
                    "kind": "root-id",
                    "root_id": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                },
                "tool_descriptions": {
                    "search_chunks": "Search this corpus for evidence."
                }
            }"#,
        )
        .unwrap();

        assert_eq!(
            config.tool_description("search_chunks"),
            "Search this corpus for evidence."
        );
        assert_eq!(
            config.tool_description("get_email"),
            DEFAULT_GET_EMAIL_DESCRIPTION
        );
    }

    #[test]
    fn tool_descriptions_reject_unknown_and_whitespace_values() {
        let unknown = serde_json::from_str::<McpConfig>(
            r#"{
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": { "base_url": "http://localhost:8080" }
                },
                "embedding_spec": { "dims": 384, "encoding": "f32le" },
                "index": {
                    "kind": "root-id",
                    "root_id": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                },
                "tool_descriptions": { "unknown_tool": "description" }
            }"#,
        );
        assert!(unknown.is_err());

        let whitespace: McpConfig = serde_json::from_str(
            r#"{
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": { "base_url": "http://localhost:8080" }
                },
                "embedding_spec": { "dims": 384, "encoding": "f32le" },
                "index": {
                    "kind": "root-id",
                    "root_id": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                },
                "tool_descriptions": { "search_chunks": "  " }
            }"#,
        )
        .unwrap();
        assert!(matches!(
            whitespace.validate(),
            Err(ConfigError::EmptyToolDescription {
                tool_name: "search_chunks"
            })
        ));
    }
}
