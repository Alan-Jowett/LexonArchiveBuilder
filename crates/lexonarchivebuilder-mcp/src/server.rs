// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::{borrow::Cow, sync::Arc};

use anyhow::Result;
use rmcp::{
    Json, ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
    transport::stdio,
};

use crate::runtime::{
    EmailRetrievalResponse, McpRuntime, NamedRetrievalRequest, NamedRetrievalResponse,
    SearchChunksRequest, SearchChunksResponse,
};

#[derive(Clone)]
pub struct LexonArchiveBuilderMcpServer {
    runtime: Arc<McpRuntime>,
    tool_router: ToolRouter<Self>,
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

#[tool_router(router = tool_router)]
impl LexonArchiveBuilderMcpServer {
    #[tool(
        name = "search_chunks",
        description = "Search indexed LexonArchiveBuilder chunks in the configured block store"
    )]
    pub async fn search_chunks(
        &self,
        params: Parameters<SearchChunksRequest>,
    ) -> Result<Json<SearchChunksResponse>, String> {
        self.runtime
            .search_chunks_blocking(params.0)
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "get_document",
        description = "Request a named document from the configured LexonArchiveBuilder index"
    )]
    pub async fn get_document(
        &self,
        params: Parameters<NamedRetrievalRequest>,
    ) -> Result<Json<NamedRetrievalResponse>, String> {
        Ok(Json(self.runtime.get_document(params.0)))
    }

    #[tool(
        name = "get_email",
        description = "Retrieve email entries from a search result leaf_block_id"
    )]
    pub async fn get_email(
        &self,
        params: Parameters<NamedRetrievalRequest>,
    ) -> Result<Json<EmailRetrievalResponse>, String> {
        self.runtime
            .get_email(params.0)
            .await
            .map(Json)
            .map_err(|error| error.to_string())
    }

    #[tool(
        name = "get_thread",
        description = "Request a named thread from the configured LexonArchiveBuilder index"
    )]
    pub async fn get_thread(
        &self,
        params: Parameters<NamedRetrievalRequest>,
    ) -> Result<Json<NamedRetrievalResponse>, String> {
        Ok(Json(self.runtime.get_thread(params.0)))
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

    use lexonarchivebuilder_indexer::config::{
        EmbeddingSpecConfig, EnvironmentConfig, LocalEmbeddingConfig,
    };

    use super::*;
    use crate::config::{IndexConfig, McpConfig, McpEnvironmentConfig, ToolDescriptionsConfig};

    #[test]
    fn server_advertises_configured_tool_descriptions() {
        let runtime = Arc::new(
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
        );
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
    }
}
