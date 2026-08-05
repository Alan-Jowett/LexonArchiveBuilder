// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::{borrow::Cow, sync::Arc, time::Instant};

use anyhow::Result;
use rmcp::schemars;
use rmcp::{
    ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Implementation, ServerCapabilities, ServerInfo},
    schemars::JsonSchema,
    tool, tool_handler, tool_router,
    transport::stdio,
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::runtime::{
    EmailRetrievalResponse, McpRuntime, NamedRetrievalRequest, NamedRetrievalResponse,
    RuntimeError, SearchChunksRequest, SearchChunksResponse,
};

#[derive(Clone)]
pub struct LexonArchiveBuilderMcpServer {
    runtime: Arc<McpRuntime>,
    tool_router: ToolRouter<Self>,
}

#[expect(
    dead_code,
    reason = "used only to generate the advertised output schema"
)]
#[derive(JsonSchema)]
struct TimedResponse<T> {
    #[serde(flatten)]
    response: T,
    elapsed_ms: u64,
}

impl LexonArchiveBuilderMcpServer {
    pub fn new(runtime: Arc<McpRuntime>) -> Self {
        let mut tool_router = Self::tool_router();
        for tool_name in ["search_chunks", "get_document", "get_email", "get_thread"] {
            let route = tool_router
                .map
                .get_mut(tool_name)
                .expect("registered MCP tool must have a router entry");
            route.attr.description = Some(Cow::Owned(runtime.tool_description(tool_name).into()));
        }
        Self {
            runtime,
            tool_router,
        }
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn timed_error(started: Instant, error: impl ToString) -> CallToolResult {
    CallToolResult::structured_error(json!({
        "error": error.to_string(),
        "elapsed_ms": elapsed_ms(started),
    }))
}

fn timed_result<T>(started: Instant, result: Result<T, RuntimeError>) -> CallToolResult
where
    T: Serialize,
{
    match result {
        Ok(response) => match serde_json::to_value(response) {
            Ok(Value::Object(mut response)) => {
                response.insert("elapsed_ms".into(), Value::from(elapsed_ms(started)));
                CallToolResult::structured(Value::Object(response))
            }
            Ok(_) => timed_error(started, "MCP tool response must serialize as an object"),
            Err(error) => timed_error(
                started,
                format!("failed to serialize MCP tool response: {error}"),
            ),
        },
        Err(error) => timed_error(started, error),
    }
}

#[tool_router(router = tool_router)]
impl LexonArchiveBuilderMcpServer {
    #[tool(
        name = "search_chunks",
        description = "Search indexed LexonArchiveBuilder chunks in the configured block store",
        output_schema = rmcp::handler::server::tool::schema_for_output::<TimedResponse<SearchChunksResponse>>()
            .expect("timed search response schema must be valid")
    )]
    pub async fn search_chunks(&self, params: Parameters<SearchChunksRequest>) -> CallToolResult {
        let started = Instant::now();
        timed_result(started, self.runtime.search_chunks_blocking(params.0))
    }

    #[tool(
        name = "get_document",
        description = "Request a named document from the configured LexonArchiveBuilder index",
        output_schema = rmcp::handler::server::tool::schema_for_output::<TimedResponse<NamedRetrievalResponse>>()
            .expect("timed named retrieval response schema must be valid")
    )]
    pub async fn get_document(&self, params: Parameters<NamedRetrievalRequest>) -> CallToolResult {
        let started = Instant::now();
        timed_result(started, Ok(self.runtime.get_document(params.0)))
    }

    #[tool(
        name = "get_email",
        description = "Retrieve email entries from a search result leaf_block_id",
        output_schema = rmcp::handler::server::tool::schema_for_output::<TimedResponse<EmailRetrievalResponse>>()
            .expect("timed email retrieval response schema must be valid")
    )]
    pub async fn get_email(&self, params: Parameters<NamedRetrievalRequest>) -> CallToolResult {
        let started = Instant::now();
        timed_result(started, self.runtime.get_email(params.0).await)
    }

    #[tool(
        name = "get_thread",
        description = "Request a named thread from the configured LexonArchiveBuilder index",
        output_schema = rmcp::handler::server::tool::schema_for_output::<TimedResponse<NamedRetrievalResponse>>()
            .expect("timed named retrieval response schema must be valid")
    )]
    pub async fn get_thread(&self, params: Parameters<NamedRetrievalRequest>) -> CallToolResult {
        let started = Instant::now();
        timed_result(started, Ok(self.runtime.get_thread(params.0)))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for LexonArchiveBuilderMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_instructions(
                "LexonArchiveBuilder MCP server for chunk search over the configured block store and embedding profile.",
            )
    }
}

pub async fn serve_stdio(runtime: Arc<McpRuntime>) -> Result<()> {
    let service = LexonArchiveBuilderMcpServer::new(runtime)
        .serve(stdio())
        .await?;
    service.waiting().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use ciborium::Value as CborValue;
    use lexonarchivebuilder_indexer::block_store::ConfiguredBlockStore;
    use lexonarchivebuilder_indexer::config::{
        EmbeddingSpecConfig, EnvironmentConfig, LocalEmbeddingConfig,
    };
    use lexongraph_block::{Block, Content, EmbeddingSpec, LeafBlock, LeafEntry, VERSION_1};
    use lexongraph_block_store::BlockStore;
    use tempfile::tempdir;

    use super::*;
    use crate::config::{IndexConfig, McpConfig, McpEnvironmentConfig, ToolDescriptionsConfig};

    fn test_runtime() -> Arc<McpRuntime> {
        Arc::new(
            McpRuntime::new(
                PathBuf::from("."),
                McpConfig {
                    environment: McpEnvironmentConfig::Shared(EnvironmentConfig::Local {
                        block_store_root: PathBuf::from("blocks"),
                        embedding: LocalEmbeddingConfig {
                            base_url: "http://localhost:8080".into(),
                            model: "all-MiniLM-L6-v2".into(),
                            api_key_env: None,
                            request_timeout_secs: 30,
                            max_retries: 0,
                            retry_delay_ms: 1,
                        },
                    }),
                    embedding_spec: EmbeddingSpecConfig {
                        dims: 384,
                        encoding: "f32le".into(),
                    },
                    index: IndexConfig::RootId {
                        root_id: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                            .into(),
                    },
                    tool_descriptions: ToolDescriptionsConfig {
                        search_chunks: Some("Search this corpus for evidence.".into()),
                        ..ToolDescriptionsConfig::default()
                    },
                    top_k: 5,
                    traversal_width: 3,
                },
            )
            .unwrap(),
        )
    }

    fn assert_timed_result(result: &CallToolResult, is_error: bool) -> &Value {
        assert_eq!(result.is_error, Some(is_error));
        let structured = result
            .structured_content
            .as_ref()
            .expect("expected structured content");
        assert!(
            structured["elapsed_ms"].is_u64(),
            "expected integer elapsed_ms"
        );
        let text = result
            .content
            .first()
            .and_then(|content| content.as_text())
            .expect("expected text content");
        let text_value: Value =
            serde_json::from_str(&text.text).expect("text content must be structured JSON");
        assert_eq!(&text_value, structured);
        structured
    }

    fn assert_timed_output_schema(server: &LexonArchiveBuilderMcpServer, tool_name: &str) {
        let schema = server.tool_router.map[tool_name]
            .attr
            .output_schema
            .as_ref()
            .expect("expected output schema");
        assert!(
            schema
                .get("properties")
                .and_then(Value::as_object)
                .is_some_and(|properties| properties.contains_key("elapsed_ms")),
            "expected {tool_name} output schema to contain elapsed_ms"
        );
    }

    #[test]
    fn server_advertises_configured_tool_descriptions() {
        let runtime = test_runtime();
        let server = LexonArchiveBuilderMcpServer::new(runtime);

        assert_eq!(
            server.tool_router.map["search_chunks"]
                .attr
                .description
                .as_deref(),
            Some("Search this corpus for evidence.")
        );
        assert_eq!(
            server.tool_router.map["get_email"]
                .attr
                .description
                .as_deref(),
            Some("Retrieve email entries from a search result leaf_block_id")
        );
        for tool_name in ["search_chunks", "get_document", "get_email", "get_thread"] {
            assert_timed_output_schema(&server, tool_name);
        }
    }

    #[tokio::test]
    async fn tool_results_include_elapsed_time_for_successes_and_failures() {
        let successful_search = timed_result(
            Instant::now(),
            Ok(SearchChunksResponse {
                root_id: "root".into(),
                top_k: 5,
                traversal_width: 3,
                results: Vec::new(),
            }),
        );
        let search_content = assert_timed_result(&successful_search, false);
        assert_eq!(search_content["root_id"], "root");
        assert_eq!(search_content["top_k"], 5);
        assert_eq!(search_content["traversal_width"], 3);

        let server = LexonArchiveBuilderMcpServer::new(test_runtime());
        let document = server
            .get_document(Parameters(NamedRetrievalRequest {
                name: "document".into(),
            }))
            .await;
        assert_eq!(
            assert_timed_result(&document, false)["status"],
            "unsupported"
        );

        let thread = server
            .get_thread(Parameters(NamedRetrievalRequest {
                name: "thread".into(),
            }))
            .await;
        assert_eq!(assert_timed_result(&thread, false)["status"], "unsupported");

        let invalid_search = server
            .search_chunks(Parameters(SearchChunksRequest {
                query: "query".into(),
                top_k: Some(0),
                traversal_width: None,
            }))
            .await;
        assert_eq!(
            assert_timed_result(&invalid_search, true)["error"],
            "top_k must be at least 1"
        );

        let invalid_email = server
            .get_email(Parameters(NamedRetrievalRequest {
                name: "invalid".into(),
            }))
            .await;
        assert!(
            assert_timed_result(&invalid_email, true)["error"]
                .as_str()
                .expect("expected error text")
                .contains("failed to parse email leaf_block_id")
        );

        let temp = tempdir().unwrap();
        let environment = EnvironmentConfig::Local {
            block_store_root: PathBuf::from("block-store"),
            embedding: LocalEmbeddingConfig {
                base_url: "http://localhost:8080".into(),
                model: "all-MiniLM-L6-v2".into(),
                api_key_env: None,
                request_timeout_secs: 5,
                max_retries: 0,
                retry_delay_ms: 1,
            },
        };
        let store = ConfiguredBlockStore::from_environment(temp.path(), &environment).unwrap();
        let leaf_block_id = store
            .put(&Block::Leaf(LeafBlock {
                version: VERSION_1,
                level: 0,
                embedding_spec: EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                entries: vec![LeafEntry {
                    embedding: vec![0; 8],
                    metadata: vec![("source_kind".into(), CborValue::Text("email".into()))],
                    content: Content {
                        media_type: "text/plain".into(),
                        body: b"email".to_vec(),
                    },
                }],
                ext: None,
            }))
            .await
            .unwrap();
        let email_server = LexonArchiveBuilderMcpServer::new(Arc::new(
            McpRuntime::new(
                temp.path().to_path_buf(),
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
            .unwrap(),
        ));
        let email = email_server
            .get_email(Parameters(NamedRetrievalRequest {
                name: leaf_block_id.to_string(),
            }))
            .await;
        assert_eq!(
            assert_timed_result(&email, false)["leaf_block_id"],
            leaf_block_id.to_string()
        );
    }
}
