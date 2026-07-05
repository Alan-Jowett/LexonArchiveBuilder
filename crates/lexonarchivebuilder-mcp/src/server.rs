// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::sync::Arc;

use anyhow::Result;
use rmcp::{
    Json, ServerHandler, ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{Implementation, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
    transport::stdio,
};

use crate::runtime::{
    McpRuntime, NamedRetrievalRequest, NamedRetrievalResponse, SearchChunksRequest,
    SearchChunksResponse,
};

#[derive(Clone)]
pub struct LexonArchiveBuilderMcpServer {
    runtime: Arc<McpRuntime>,
    tool_router: ToolRouter<Self>,
}

impl LexonArchiveBuilderMcpServer {
    pub fn new(runtime: Arc<McpRuntime>) -> Self {
        Self {
            runtime,
            tool_router: Self::tool_router(),
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
        description = "Request a named email from the configured LexonArchiveBuilder index"
    )]
    pub async fn get_email(
        &self,
        params: Parameters<NamedRetrievalRequest>,
    ) -> Result<Json<NamedRetrievalResponse>, String> {
        Ok(Json(self.runtime.get_email(params.0)))
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
                "LexonArchiveBuilder MCP server for chunk search over a filesystem-backed block store using the local embedding profile.",
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
