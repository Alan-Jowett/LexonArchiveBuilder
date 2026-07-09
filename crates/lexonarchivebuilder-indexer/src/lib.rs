// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

pub mod block_copy;
pub mod block_store;
pub mod config;
mod custom_blocks;
pub mod embedding;
pub mod mailbox;
mod paths;
pub mod quality;
pub mod resolver;
pub mod runtime;
pub mod search;
pub mod tree_tools;

pub use config::{BatchRequest, BatchSummary, ClusteringConfigOverrides, ExecutionStage};
pub use runtime::{
    ClusteringFailureDiagnostics, INGESTION_ONLY_ROOT_ID_PLACEHOLDER,
    clustering_failure_diagnostics_path, run_request, run_request_file,
    run_request_file_with_outputs, run_request_file_with_overrides, run_request_file_with_stage,
    run_request_with_overrides, validate_request_file_with_overrides,
    write_clustering_failure_diagnostics_file, write_summary_file,
};
