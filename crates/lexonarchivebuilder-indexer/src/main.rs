// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::ffi::OsStr;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use env_logger::Env;
use lexonarchivebuilder_indexer::block_copy::{
    CopyDestinationMode, DEFAULT_MAX_IN_FLIGHT_DESTINATION_WRITES, RootedBlockCopyProgress,
    RootedBlockCopyProgressSnapshot, copy_rooted_blocks_with_mode_and_limit_and_progress,
    default_report_path as default_copy_report_path,
    render_report_summary as render_copy_report_summary, write_report as write_copy_report,
};
use lexonarchivebuilder_indexer::block_store::ConfiguredBlockStore;
use lexonarchivebuilder_indexer::config::{
    EnvironmentConfig, LocalEmbeddingConfig, ProductionBlockStoreConfig, ProductionEmbeddingConfig,
};
use lexonarchivebuilder_indexer::embedding::ConfiguredEmbeddingProvider;
use lexonarchivebuilder_indexer::quality::{
    TnnRecallConfig, assess_rooted_tree_with_config,
    default_report_path as default_quality_report_path, default_tnn_recall_sample_size,
    default_tnn_recall_seed, render_report_summary, write_report as write_quality_report,
};
use lexonarchivebuilder_indexer::search::{
    default_report_path as default_search_report_path,
    default_traversal_width as default_search_traversal_width,
    render_report_summary as render_search_report_summary, search_rooted_tree,
    write_report as write_search_report,
};
use lexonarchivebuilder_indexer::tree_tools::parse_block_hash;
use lexonarchivebuilder_indexer::{
    ClusteringConfigOverrides, ExecutionStage, run_request_file_with_outputs,
    validate_request_file_with_overrides, write_summary_file,
};
const DEFAULT_LOCAL_MODEL: &str = "all-MiniLM-L6-v2";
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_MAX_RETRIES: u32 = 5;
const DEFAULT_RETRY_DELAY_MS: u64 = 1_000;
const STRUCTURAL_FINDINGS_EXIT_CODE: i32 = 2;
const COPY_FAILURES_EXIT_CODE: i32 = 3;
const COPY_LIVENESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const RUST_LOG_ENV_VAR: &str = "RUST_LOG";

#[derive(Debug, Parser)]
#[command(author, version, about = "LexonArchiveBuilder batch indexer MVP")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run {
        #[arg(long)]
        request: PathBuf,
        #[arg(long)]
        summary_out: Option<PathBuf>,
        #[arg(long)]
        stage: Option<ExecutionStage>,
        #[arg(long)]
        validate_only: bool,
        #[command(flatten)]
        clustering: ClusteringConfigOverrides,
    },
    Quality {
        #[arg(long)]
        root_id: String,
        #[arg(long, default_value_t = default_tnn_recall_sample_size())]
        tnn_recall_sample_size: usize,
        #[arg(long, default_value_t = default_tnn_recall_seed())]
        tnn_recall_seed: u64,
        #[arg(long, default_value_t = default_search_traversal_width())]
        traversal_width: usize,
        #[arg(
            long,
            help = "Skip full-tree quality traversal and instead sample query embeddings by deterministic random walks from the root."
        )]
        fast_random_walk: bool,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[command(flatten)]
        block_store: BlockStoreArgs,
    },
    Search {
        #[arg(long)]
        query: String,
        #[arg(long)]
        root_id: String,
        #[arg(long, default_value_t = 5)]
        top_k: usize,
        #[arg(long, default_value_t = default_search_traversal_width())]
        traversal_width: usize,
        #[arg(
            long,
            help = "Base URL for an OpenAI-compatible embedding service. A full /v1/embeddings URL is also accepted."
        )]
        embedding_endpoint: String,
        #[arg(long, default_value = DEFAULT_LOCAL_MODEL)]
        embedding_model: String,
        #[arg(long)]
        embedding_api_key_env: Option<String>,
        #[arg(long, default_value_t = DEFAULT_REQUEST_TIMEOUT_SECS)]
        embedding_request_timeout_secs: u64,
        #[arg(long, default_value_t = DEFAULT_MAX_RETRIES)]
        embedding_max_retries: u32,
        #[arg(long, default_value_t = DEFAULT_RETRY_DELAY_MS)]
        embedding_retry_delay_ms: u64,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[command(flatten)]
        block_store: BlockStoreArgs,
    },
    Copy {
        #[arg(long = "root-id", required = true, num_args = 1..)]
        root_ids: Vec<String>,
        #[arg(
            long,
            help = "Skip destination existence reads and attempt destination writes directly."
        )]
        blind_write: bool,
        #[arg(
            long,
            default_value_t = DEFAULT_MAX_IN_FLIGHT_DESTINATION_WRITES,
            value_parser = parse_positive_usize,
            value_name = "COUNT",
            help = "Maximum destination block writes to keep in flight concurrently."
        )]
        max_in_flight_destination_writes: usize,
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[command(flatten)]
        source_block_store: SourceBlockStoreArgs,
        #[command(flatten)]
        destination_block_store: DestinationBlockStoreArgs,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum ReadableBlockStoreProfile {
    Local,
    LocalRedb,
    Production,
    ProductionV2,
    GatewayHttp3,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum WritableBlockStoreProfile {
    Local,
    LocalRedb,
    Production,
    ProductionV2,
}

#[derive(Debug, Args)]
struct BlockStoreArgs {
    #[arg(long, value_enum, default_value_t = ReadableBlockStoreProfile::Local)]
    block_store_profile: ReadableBlockStoreProfile,
    #[arg(
        long,
        required_if_eq_any([
            ("block_store_profile", "local"),
            ("block_store_profile", "local-redb"),
        ])
    )]
    block_store_root: Option<PathBuf>,
    #[arg(
        long,
        required_if_eq_any([
            ("block_store_profile", "production"),
            ("block_store_profile", "production-v2"),
        ])
    )]
    block_store_container_sas_url: Option<String>,
    #[arg(long, required_if_eq("block_store_profile", "gateway-http3"))]
    block_store_gateway_dns_name: Option<String>,
    #[arg(long, required_if_eq("block_store_profile", "production"))]
    block_store_filesystem_cache_root: Option<PathBuf>,
    #[arg(long, required_if_eq("block_store_profile", "production"))]
    block_store_memory_cache_max_resident_blocks: Option<usize>,
    #[arg(
        long,
        help = "Reserved for non-Azure block store backends. The approved Azure-backed production profiles reject non-empty prefixes."
    )]
    block_store_prefix: Option<String>,
}

impl BlockStoreArgs {
    fn try_environment_config(&self) -> anyhow::Result<EnvironmentConfig> {
        block_store_environment_config(
            self.block_store_profile,
            self.block_store_root.clone(),
            self.block_store_container_sas_url.clone(),
            self.block_store_filesystem_cache_root.clone(),
            self.block_store_memory_cache_max_resident_blocks,
            self.block_store_prefix.clone(),
        )
    }
}

#[derive(Debug, Args)]
struct SourceBlockStoreArgs {
    #[arg(long, value_enum, default_value_t = ReadableBlockStoreProfile::Local)]
    source_block_store_profile: ReadableBlockStoreProfile,
    #[arg(
        long,
        required_if_eq_any([
            ("source_block_store_profile", "local"),
            ("source_block_store_profile", "local-redb"),
        ])
    )]
    source_block_store_root: Option<PathBuf>,
    #[arg(
        long,
        required_if_eq_any([
            ("source_block_store_profile", "production"),
            ("source_block_store_profile", "production-v2"),
        ])
    )]
    source_block_store_container_sas_url: Option<String>,
    #[arg(long, required_if_eq("source_block_store_profile", "gateway-http3"))]
    source_block_store_gateway_dns_name: Option<String>,
    #[arg(long, required_if_eq("source_block_store_profile", "production"))]
    source_block_store_filesystem_cache_root: Option<PathBuf>,
    #[arg(long, required_if_eq("source_block_store_profile", "production"))]
    source_block_store_memory_cache_max_resident_blocks: Option<usize>,
    #[arg(
        long,
        help = "Reserved for non-Azure block store backends. The approved Azure-backed production profiles reject non-empty prefixes."
    )]
    source_block_store_prefix: Option<String>,
}

impl SourceBlockStoreArgs {
    fn try_environment_config(&self) -> anyhow::Result<EnvironmentConfig> {
        block_store_environment_config(
            self.source_block_store_profile,
            self.source_block_store_root.clone(),
            self.source_block_store_container_sas_url.clone(),
            self.source_block_store_filesystem_cache_root.clone(),
            self.source_block_store_memory_cache_max_resident_blocks,
            self.source_block_store_prefix.clone(),
        )
    }
}

#[derive(Debug, Args)]
struct DestinationBlockStoreArgs {
    #[arg(long, value_enum, default_value_t = WritableBlockStoreProfile::Local)]
    destination_block_store_profile: WritableBlockStoreProfile,
    #[arg(
        long,
        required_if_eq_any([
            ("destination_block_store_profile", "local"),
            ("destination_block_store_profile", "local-redb"),
        ])
    )]
    destination_block_store_root: Option<PathBuf>,
    #[arg(
        long,
        required_if_eq_any([
            ("destination_block_store_profile", "production"),
            ("destination_block_store_profile", "production-v2"),
        ])
    )]
    destination_block_store_container_sas_url: Option<String>,
    #[arg(long, required_if_eq("destination_block_store_profile", "production"))]
    destination_block_store_filesystem_cache_root: Option<PathBuf>,
    #[arg(long, required_if_eq("destination_block_store_profile", "production"))]
    destination_block_store_memory_cache_max_resident_blocks: Option<usize>,
    #[arg(
        long,
        help = "Reserved for non-Azure block store backends. The approved Azure-backed production profiles reject non-empty prefixes."
    )]
    destination_block_store_prefix: Option<String>,
}

impl DestinationBlockStoreArgs {
    fn to_environment_config(&self) -> EnvironmentConfig {
        destination_block_store_environment_config(
            self.destination_block_store_profile,
            self.destination_block_store_root.clone(),
            self.destination_block_store_container_sas_url.clone(),
            self.destination_block_store_filesystem_cache_root.clone(),
            self.destination_block_store_memory_cache_max_resident_blocks,
            self.destination_block_store_prefix.clone(),
        )
    }
}

fn block_store_environment_config(
    block_store_profile: ReadableBlockStoreProfile,
    block_store_root: Option<PathBuf>,
    block_store_container_sas_url: Option<String>,
    block_store_filesystem_cache_root: Option<PathBuf>,
    block_store_memory_cache_max_resident_blocks: Option<usize>,
    block_store_prefix: Option<String>,
) -> anyhow::Result<EnvironmentConfig> {
    let environment = match block_store_profile {
        ReadableBlockStoreProfile::Local => EnvironmentConfig::Local {
            block_store_root: block_store_root.expect("local block_store_root is required by clap"),
            embedding: unused_local_embedding(),
        },
        ReadableBlockStoreProfile::LocalRedb => EnvironmentConfig::LocalRedb {
            block_store_root: block_store_root
                .expect("local-redb block_store_root is required by clap"),
            embedding: unused_local_embedding(),
        },
        ReadableBlockStoreProfile::Production => EnvironmentConfig::Production {
            block_store: ProductionBlockStoreConfig {
                container_sas_url: block_store_container_sas_url
                    .expect("production container_sas_url is required by clap"),
                filesystem_cache_root: block_store_filesystem_cache_root,
                memory_cache_max_resident_blocks: block_store_memory_cache_max_resident_blocks,
                prefix: block_store_prefix,
            },
            embedding: ProductionEmbeddingConfig {
                endpoint: "https://unused.production.example".into(),
                deployment: "unused".into(),
                api_version: "2024-02-01".into(),
                api_key_env: None,
            },
        },
        ReadableBlockStoreProfile::ProductionV2 => EnvironmentConfig::ProductionV2 {
            block_store: ProductionBlockStoreConfig {
                container_sas_url: block_store_container_sas_url
                    .expect("production-v2 container_sas_url is required by clap"),
                filesystem_cache_root: block_store_filesystem_cache_root,
                memory_cache_max_resident_blocks: block_store_memory_cache_max_resident_blocks,
                prefix: block_store_prefix,
            },
            embedding: ProductionEmbeddingConfig {
                endpoint: "https://unused.production.example".into(),
                deployment: "unused".into(),
                api_version: "2024-02-01".into(),
                api_key_env: None,
            },
        },
        ReadableBlockStoreProfile::GatewayHttp3 => {
            anyhow::bail!(
                "gateway-http3 is a read-only block-store profile and does not map to EnvironmentConfig"
            )
        }
    };
    Ok(environment)
}

fn destination_block_store_environment_config(
    block_store_profile: WritableBlockStoreProfile,
    block_store_root: Option<PathBuf>,
    block_store_container_sas_url: Option<String>,
    block_store_filesystem_cache_root: Option<PathBuf>,
    block_store_memory_cache_max_resident_blocks: Option<usize>,
    block_store_prefix: Option<String>,
) -> EnvironmentConfig {
    let readable_profile = match block_store_profile {
        WritableBlockStoreProfile::Local => ReadableBlockStoreProfile::Local,
        WritableBlockStoreProfile::LocalRedb => ReadableBlockStoreProfile::LocalRedb,
        WritableBlockStoreProfile::Production => ReadableBlockStoreProfile::Production,
        WritableBlockStoreProfile::ProductionV2 => ReadableBlockStoreProfile::ProductionV2,
    };
    block_store_environment_config(
        readable_profile,
        block_store_root,
        block_store_container_sas_url,
        block_store_filesystem_cache_root,
        block_store_memory_cache_max_resident_blocks,
        block_store_prefix,
    )
    .expect("writable block-store profiles always map to EnvironmentConfig")
}

fn configured_block_store_from_environment(
    environment: &EnvironmentConfig,
) -> anyhow::Result<ConfiguredBlockStore> {
    ConfiguredBlockStore::from_environment(Path::new("."), environment)
        .context("failed to configure block store")
}

async fn await_with_copy_liveness<Fut, F, T>(
    operation: Fut,
    heartbeat_interval: Duration,
    heartbeat_message: F,
) -> T
where
    Fut: Future<Output = T>,
    F: Fn() -> String + Send + 'static,
{
    let keep_running = Arc::new(AtomicBool::new(true));
    let heartbeat_thread = if heartbeat_interval.is_zero() {
        None
    } else {
        let keep_running = Arc::clone(&keep_running);
        Some(std::thread::spawn(move || {
            while keep_running.load(Ordering::Acquire) {
                std::thread::park_timeout(heartbeat_interval);
                if !keep_running.load(Ordering::Acquire) {
                    break;
                }
                eprintln!("{}", heartbeat_message());
            }
        }))
    };

    let result = operation.await;
    keep_running.store(false, Ordering::Release);
    if let Some(heartbeat_thread) = heartbeat_thread {
        heartbeat_thread.thread().unpark();
        let _ = heartbeat_thread.join();
    }
    result
}

fn format_copy_liveness_message(
    root_count: usize,
    elapsed: Duration,
    progress: RootedBlockCopyProgressSnapshot,
) -> String {
    let mut details = vec![format!("read {}", progress.read_source_block_count)];
    if let Some(copied_block_count) = progress.copied_block_count {
        details.push(format!("copied {copied_block_count}"));
    }
    if let Some(skipped_already_present_block_count) = progress.skipped_already_present_block_count
    {
        details.push(format!("skipped {skipped_already_present_block_count}"));
    }
    if let Some(attempted_write_block_count) = progress.attempted_write_block_count {
        details.push(format!("attempted {attempted_write_block_count}"));
    }
    details.push(format!("failed {}", progress.failed_block_count));
    format!(
        "Rooted block copy still running after {}s for {} requested root(s): {}...",
        elapsed.as_secs(),
        root_count,
        details.join(", ")
    )
}

fn build_copy_liveness_message(
    root_count: usize,
    progress: RootedBlockCopyProgress,
) -> impl Fn() -> String + Send + 'static {
    let start = std::time::Instant::now();
    move || format_copy_liveness_message(root_count, start.elapsed(), progress.snapshot())
}

fn rust_log_requested_with(value: Option<&OsStr>) -> bool {
    value
        .and_then(OsStr::to_str)
        .is_some_and(|filter| !filter.trim().is_empty())
}

fn initialize_process_logging() {
    if !rust_log_requested_with(std::env::var_os(RUST_LOG_ENV_VAR).as_deref()) {
        return;
    }

    let _ = env_logger::Builder::from_env(Env::default()).try_init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    initialize_process_logging();
    let cli = Cli::parse();

    match cli.command {
        Command::Run {
            request,
            summary_out,
            stage,
            validate_only,
            clustering,
        } => {
            if validate_only {
                validate_request_file_with_overrides(
                    &request,
                    stage,
                    clustering,
                    summary_out.as_deref(),
                )
                .await
                .with_context(|| format!("failed to validate request {}", request.display()))?;
                println!("Validation OK");
            } else {
                let summary = run_request_file_with_outputs(
                    &request,
                    stage,
                    clustering,
                    summary_out.as_deref(),
                )
                .await
                .with_context(|| format!("failed to run request {}", request.display()))?;
                let rendered = serde_json::to_string_pretty(&summary)
                    .context("failed to render batch summary")?;
                if let Some(output_path) = summary_out.as_ref() {
                    write_summary_file(output_path, &summary)?;
                }
                println!("{rendered}");
            }
        }
        Command::Quality {
            root_id,
            tnn_recall_sample_size,
            tnn_recall_seed,
            traversal_width,
            fast_random_walk,
            json_out,
            block_store,
        } => {
            let root_id = parse_block_hash(&root_id)?;
            let store = configured_block_store(&block_store)?;
            let report = assess_rooted_tree_with_config(
                &root_id,
                &store,
                TnnRecallConfig {
                    sample_size: tnn_recall_sample_size,
                    seed: tnn_recall_seed,
                    traversal_width,
                    fast_random_walk,
                },
            )?;
            let output_path = json_out.unwrap_or_else(|| default_quality_report_path(&root_id));
            write_quality_report(&output_path, &report)?;
            println!("{}", render_report_summary(&report));
            println!("JSON report: {}", output_path.display());
            if report.summary.structural_finding_count > 0 {
                std::process::exit(STRUCTURAL_FINDINGS_EXIT_CODE);
            }
        }
        Command::Search {
            query,
            root_id,
            top_k,
            traversal_width,
            embedding_endpoint,
            embedding_model,
            embedding_api_key_env,
            embedding_request_timeout_secs,
            embedding_max_retries,
            embedding_retry_delay_ms,
            json_out,
            block_store,
        } => {
            let root_id = parse_block_hash(&root_id)?;
            let store = configured_block_store(&block_store)?;
            let provider =
                ConfiguredEmbeddingProvider::from_environment(&EnvironmentConfig::Local {
                    block_store_root: PathBuf::from("."),
                    embedding: LocalEmbeddingConfig {
                        base_url: normalize_embedding_base_url(&embedding_endpoint),
                        model: embedding_model,
                        api_key_env: embedding_api_key_env,
                        request_timeout_secs: embedding_request_timeout_secs,
                        max_retries: embedding_max_retries,
                        retry_delay_ms: embedding_retry_delay_ms,
                    },
                })
                .context("failed to configure embedding provider")?;
            let report =
                search_rooted_tree(&store, &provider, &root_id, &query, top_k, traversal_width)
                    .await
                    .context("failed to search rooted tree")?;
            let output_path =
                json_out.unwrap_or_else(|| default_search_report_path(&root_id, &query));
            write_search_report(&output_path, &report)?;
            println!("{}", render_search_report_summary(&report));
            println!("JSON report: {}", output_path.display());
        }
        Command::Copy {
            root_ids,
            blind_write,
            max_in_flight_destination_writes,
            json_out,
            source_block_store,
            destination_block_store,
        } => {
            let root_ids = root_ids
                .iter()
                .map(|root_id| parse_block_hash(root_id))
                .collect::<Result<Vec<_>, _>>()?;
            let source_store = configured_source_block_store(&source_block_store)?;
            let destination_store = configured_destination_block_store(&destination_block_store)?;
            let destination_mode = if blind_write {
                CopyDestinationMode::BlindWrite
            } else {
                CopyDestinationMode::ReadBeforeWrite
            };
            let progress = RootedBlockCopyProgress::new(destination_mode);
            let report = await_with_copy_liveness(
                copy_rooted_blocks_with_mode_and_limit_and_progress(
                    &source_store,
                    &destination_store,
                    &root_ids,
                    destination_mode,
                    max_in_flight_destination_writes,
                    Some(progress.clone()),
                ),
                COPY_LIVENESS_HEARTBEAT_INTERVAL,
                build_copy_liveness_message(root_ids.len(), progress.clone()),
            )
            .await;
            let output_path = json_out.unwrap_or_else(|| default_copy_report_path(&root_ids));
            write_copy_report(&output_path, &report)?;
            println!("{}", render_copy_report_summary(&report));
            println!("JSON report: {}", output_path.display());
            if report.failed_block_count > 0 {
                std::process::exit(COPY_FAILURES_EXIT_CODE);
            }
        }
    }

    Ok(())
}

fn configured_block_store(args: &BlockStoreArgs) -> anyhow::Result<ConfiguredBlockStore> {
    match args.block_store_profile {
        ReadableBlockStoreProfile::GatewayHttp3 => ConfiguredBlockStore::gateway_http3_store(
            args.block_store_gateway_dns_name
                .as_deref()
                .expect("gateway-http3 dns name is required by clap"),
        )
        .context("failed to configure gateway-http3 block store"),
        _ => configured_block_store_from_environment(&args.try_environment_config()?),
    }
}

fn configured_source_block_store(
    args: &SourceBlockStoreArgs,
) -> anyhow::Result<ConfiguredBlockStore> {
    match args.source_block_store_profile {
        ReadableBlockStoreProfile::GatewayHttp3 => ConfiguredBlockStore::gateway_http3_store(
            args.source_block_store_gateway_dns_name
                .as_deref()
                .expect("gateway-http3 dns name is required by clap"),
        )
        .context("failed to configure gateway-http3 source block store"),
        _ => configured_block_store_from_environment(&args.try_environment_config()?),
    }
}

fn configured_destination_block_store(
    args: &DestinationBlockStoreArgs,
) -> anyhow::Result<ConfiguredBlockStore> {
    configured_block_store_from_environment(&args.to_environment_config())
}

fn unused_local_embedding() -> LocalEmbeddingConfig {
    LocalEmbeddingConfig {
        base_url: "http://unused.local".into(),
        model: DEFAULT_LOCAL_MODEL.into(),
        api_key_env: None,
        request_timeout_secs: DEFAULT_REQUEST_TIMEOUT_SECS,
        max_retries: DEFAULT_MAX_RETRIES,
        retry_delay_ms: DEFAULT_RETRY_DELAY_MS,
    }
}

fn normalize_embedding_base_url(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    trimmed
        .strip_suffix("/v1/embeddings")
        .unwrap_or(trimmed)
        .to_string()
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("invalid positive integer: {value}"))?;
    if parsed == 0 {
        return Err("value must be greater than zero".into());
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use tokio::time::sleep;

    #[test]
    fn run_command_parses_stage_override() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "run",
            "--request",
            "request.json",
            "--stage",
            "clustering-and-block-assembly",
        ])
        .unwrap();

        match cli.command {
            Command::Run { stage, .. } => {
                assert_eq!(stage, Some(ExecutionStage::ClusteringAndBlockAssembly));
            }
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn run_command_parses_profile_version_override() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "run",
            "--request",
            "request.json",
            "--profile-version",
            "0.5.0",
        ])
        .unwrap();

        match cli.command {
            Command::Run { clustering, .. } => {
                assert_eq!(
                    clustering.profile_version,
                    Some(lexongraph_streaming_indexer::PublishedProfileVersion::new(
                        0, 5, 0
                    ))
                );
            }
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn run_command_parses_local_testing_cluster_count_override() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "run",
            "--request",
            "request.json",
            "--local-testing-cluster-count",
            "32",
        ])
        .unwrap();

        match cli.command {
            Command::Run { clustering, .. } => {
                assert_eq!(clustering.local_testing_cluster_count, Some(32));
            }
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn run_command_rejects_retired_low_level_clustering_flags() {
        let error = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "run",
            "--request",
            "request.json",
            "--clustering-algorithm",
            "directional-pca",
        ])
        .unwrap_err()
        .to_string();

        assert!(error.contains("--clustering-algorithm"));
    }

    #[test]
    fn run_command_parses_validate_only_flag() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "run",
            "--request",
            "request.json",
            "--validate-only",
        ])
        .unwrap();

        match cli.command {
            Command::Run { validate_only, .. } => assert!(validate_only),
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn quality_command_parses_local_block_store_args() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "quality",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--tnn-recall-sample-size",
            "17",
            "--tnn-recall-seed",
            "9",
            "--traversal-width",
            "7",
            "--block-store-root",
            "blocks",
        ])
        .unwrap();

        match cli.command {
            Command::Quality {
                root_id,
                tnn_recall_sample_size,
                tnn_recall_seed,
                traversal_width,
                fast_random_walk,
                block_store,
                ..
            } => {
                assert_eq!(
                    root_id,
                    "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                );
                assert_eq!(tnn_recall_sample_size, 17);
                assert_eq!(tnn_recall_seed, 9);
                assert_eq!(traversal_width, 7);
                assert!(!fast_random_walk);
                assert_eq!(
                    block_store.block_store_profile,
                    ReadableBlockStoreProfile::Local
                );
                assert_eq!(block_store.block_store_root, Some(PathBuf::from("blocks")));
            }
            _ => panic!("expected quality command"),
        }
    }

    #[test]
    fn quality_command_parses_local_redb_block_store_args() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "quality",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--block-store-profile",
            "local-redb",
            "--block-store-root",
            "blocks",
        ])
        .unwrap();

        match cli.command {
            Command::Quality { block_store, .. } => {
                assert_eq!(
                    block_store.block_store_profile,
                    ReadableBlockStoreProfile::LocalRedb
                );
                let environment = block_store.try_environment_config().unwrap();
                match environment {
                    EnvironmentConfig::LocalRedb {
                        block_store_root, ..
                    } => {
                        assert_eq!(block_store_root, PathBuf::from("blocks"));
                    }
                    _ => panic!("expected local-redb environment"),
                }
            }
            _ => panic!("expected quality command"),
        }
    }

    #[test]
    fn quality_command_parses_fast_random_walk_flag() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "quality",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--tnn-recall-sample-size",
            "5",
            "--fast-random-walk",
            "--block-store-root",
            "blocks",
        ])
        .unwrap();

        match cli.command {
            Command::Quality {
                fast_random_walk, ..
            } => assert!(fast_random_walk),
            _ => panic!("expected quality command"),
        }
    }

    #[test]
    fn quality_command_rejects_production_profile_without_overlay_args() {
        let error = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "quality",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--block-store-profile",
            "production",
            "--block-store-container-sas-url",
            "https://example.blob.core.windows.net/archive-sync?sig=test",
        ])
        .unwrap_err()
        .to_string();

        assert!(error.contains("--block-store-filesystem-cache-root"));
        assert!(error.contains("--block-store-memory-cache-max-resident-blocks"));
    }

    #[test]
    fn quality_command_parses_production_overlay_args() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "quality",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--block-store-profile",
            "production",
            "--block-store-container-sas-url",
            "https://example.blob.core.windows.net/archive-sync?sig=test",
            "--block-store-filesystem-cache-root",
            "cache",
            "--block-store-memory-cache-max-resident-blocks",
            "64",
        ])
        .unwrap();

        match cli.command {
            Command::Quality { block_store, .. } => {
                assert_eq!(
                    block_store.block_store_profile,
                    ReadableBlockStoreProfile::Production
                );
                assert_eq!(
                    block_store.block_store_container_sas_url,
                    Some("https://example.blob.core.windows.net/archive-sync?sig=test".into())
                );
                assert_eq!(
                    block_store.block_store_filesystem_cache_root,
                    Some(PathBuf::from("cache"))
                );
                assert_eq!(
                    block_store.block_store_memory_cache_max_resident_blocks,
                    Some(64)
                );
                let environment = block_store.try_environment_config().unwrap();
                match environment {
                    EnvironmentConfig::Production { block_store, .. } => {
                        assert_eq!(
                            block_store.container_sas_url,
                            "https://example.blob.core.windows.net/archive-sync?sig=test"
                        );
                        assert_eq!(
                            block_store.filesystem_cache_root,
                            Some(PathBuf::from("cache"))
                        );
                        assert_eq!(block_store.memory_cache_max_resident_blocks, Some(64));
                        assert_eq!(block_store.prefix, None);
                    }
                    EnvironmentConfig::Local { .. }
                    | EnvironmentConfig::LocalRedb { .. }
                    | EnvironmentConfig::LocalOverlay { .. }
                    | EnvironmentConfig::ProductionV2 { .. } => {
                        panic!("expected production environment")
                    }
                }
            }
            _ => panic!("expected quality command"),
        }
    }

    #[test]
    fn quality_command_parses_production_v2_args_without_overlay_cache_fields() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "quality",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--block-store-profile",
            "production-v2",
            "--block-store-container-sas-url",
            "https://example.table.core.windows.net/archive-sync?sig=test",
        ])
        .unwrap();

        match cli.command {
            Command::Quality { block_store, .. } => {
                assert_eq!(
                    block_store.block_store_profile,
                    ReadableBlockStoreProfile::ProductionV2
                );
                let environment = block_store.try_environment_config().unwrap();
                match environment {
                    EnvironmentConfig::ProductionV2 { block_store, .. } => {
                        assert_eq!(
                            block_store.container_sas_url,
                            "https://example.table.core.windows.net/archive-sync?sig=test"
                        );
                        assert_eq!(block_store.filesystem_cache_root, None);
                        assert_eq!(block_store.memory_cache_max_resident_blocks, None);
                    }
                    EnvironmentConfig::Local { .. }
                    | EnvironmentConfig::LocalRedb { .. }
                    | EnvironmentConfig::LocalOverlay { .. }
                    | EnvironmentConfig::Production { .. } => {
                        panic!("expected production-v2 environment")
                    }
                }
            }
            _ => panic!("expected quality command"),
        }
    }

    #[test]
    fn quality_command_parses_gateway_http3_args() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "quality",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--block-store-profile",
            "gateway-http3",
            "--block-store-gateway-dns-name",
            "gateway.example.com",
        ])
        .unwrap();

        match cli.command {
            Command::Quality { block_store, .. } => {
                assert_eq!(
                    block_store.block_store_profile,
                    ReadableBlockStoreProfile::GatewayHttp3
                );
                assert_eq!(
                    block_store.block_store_gateway_dns_name,
                    Some("gateway.example.com".into())
                );
            }
            _ => panic!("expected quality command"),
        }
    }

    #[test]
    fn gateway_http3_profile_rejects_environment_config_conversion() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "quality",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--block-store-profile",
            "gateway-http3",
            "--block-store-gateway-dns-name",
            "gateway.example.com",
        ])
        .unwrap();

        match cli.command {
            Command::Quality { block_store, .. } => {
                let error = block_store
                    .try_environment_config()
                    .unwrap_err()
                    .to_string();
                assert!(error.contains("read-only block-store profile"));
            }
            _ => panic!("expected quality command"),
        }
    }

    #[test]
    fn search_command_parses_required_args() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "search",
            "--query",
            "hello",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--embedding-endpoint",
            "http://localhost:8080",
            "--block-store-root",
            "blocks",
        ])
        .unwrap();

        match cli.command {
            Command::Search {
                top_k,
                traversal_width,
                embedding_model,
                ..
            } => {
                assert_eq!(top_k, 5);
                assert_eq!(traversal_width, 3);
                assert_eq!(embedding_model, DEFAULT_LOCAL_MODEL);
            }
            _ => panic!("expected search command"),
        }
    }

    #[test]
    fn search_command_parses_production_v2_args_without_overlay_cache_fields() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "search",
            "--query",
            "hello",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--embedding-endpoint",
            "http://localhost:8080",
            "--block-store-profile",
            "production-v2",
            "--block-store-container-sas-url",
            "https://example.table.core.windows.net/archive-sync?sig=test",
        ])
        .unwrap();

        match cli.command {
            Command::Search { block_store, .. } => {
                assert_eq!(
                    block_store.block_store_profile,
                    ReadableBlockStoreProfile::ProductionV2
                );
                let environment = block_store.try_environment_config().unwrap();
                match environment {
                    EnvironmentConfig::ProductionV2 { block_store, .. } => {
                        assert_eq!(
                            block_store.container_sas_url,
                            "https://example.table.core.windows.net/archive-sync?sig=test"
                        );
                        assert_eq!(block_store.filesystem_cache_root, None);
                        assert_eq!(block_store.memory_cache_max_resident_blocks, None);
                    }
                    EnvironmentConfig::Local { .. }
                    | EnvironmentConfig::LocalRedb { .. }
                    | EnvironmentConfig::LocalOverlay { .. }
                    | EnvironmentConfig::Production { .. } => {
                        panic!("expected production-v2 environment")
                    }
                }
            }
            _ => panic!("expected search command"),
        }
    }

    #[test]
    fn search_command_parses_gateway_http3_args() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "search",
            "--query",
            "hello",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--embedding-endpoint",
            "http://localhost:8080",
            "--block-store-profile",
            "gateway-http3",
            "--block-store-gateway-dns-name",
            "gateway.example.com",
        ])
        .unwrap();

        match cli.command {
            Command::Search { block_store, .. } => {
                assert_eq!(
                    block_store.block_store_profile,
                    ReadableBlockStoreProfile::GatewayHttp3
                );
                assert_eq!(
                    block_store.block_store_gateway_dns_name,
                    Some("gateway.example.com".to_string())
                );
            }
            _ => panic!("expected search command"),
        }
    }

    #[test]
    fn copy_command_parses_required_args() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "copy",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--source-block-store-root",
            "source-blocks",
            "--destination-block-store-root",
            "destination-blocks",
        ])
        .unwrap();

        match cli.command {
            Command::Copy {
                root_ids,
                blind_write,
                source_block_store,
                destination_block_store,
                ..
            } => {
                assert_eq!(root_ids.len(), 1);
                assert_eq!(
                    root_ids[0],
                    "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                );
                assert_eq!(
                    source_block_store.source_block_store_profile,
                    ReadableBlockStoreProfile::Local
                );
                assert_eq!(
                    source_block_store.source_block_store_root,
                    Some(PathBuf::from("source-blocks"))
                );
                assert_eq!(
                    destination_block_store.destination_block_store_profile,
                    WritableBlockStoreProfile::Local
                );
                assert_eq!(
                    destination_block_store.destination_block_store_root,
                    Some(PathBuf::from("destination-blocks"))
                );
                assert!(!blind_write);
            }
            _ => panic!("expected copy command"),
        }
    }

    #[test]
    fn copy_command_parses_local_redb_source_and_destination_args() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "copy",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--source-block-store-profile",
            "local-redb",
            "--source-block-store-root",
            "source-blocks",
            "--destination-block-store-profile",
            "local-redb",
            "--destination-block-store-root",
            "destination-blocks",
        ])
        .unwrap();

        match cli.command {
            Command::Copy {
                source_block_store,
                destination_block_store,
                ..
            } => {
                assert_eq!(
                    source_block_store.source_block_store_profile,
                    ReadableBlockStoreProfile::LocalRedb
                );
                assert_eq!(
                    destination_block_store.destination_block_store_profile,
                    WritableBlockStoreProfile::LocalRedb
                );
                assert_eq!(
                    source_block_store.source_block_store_root,
                    Some(PathBuf::from("source-blocks"))
                );
                match source_block_store.try_environment_config().unwrap() {
                    EnvironmentConfig::LocalRedb {
                        block_store_root, ..
                    } => {
                        assert_eq!(block_store_root, PathBuf::from("source-blocks"));
                    }
                    _ => panic!("expected local-redb source environment"),
                }
                match destination_block_store.to_environment_config() {
                    EnvironmentConfig::LocalRedb {
                        block_store_root, ..
                    } => {
                        assert_eq!(block_store_root, PathBuf::from("destination-blocks"));
                    }
                    _ => panic!("expected local-redb destination environment"),
                }
            }
            _ => panic!("expected copy command"),
        }
    }

    #[test]
    fn copy_command_rejects_production_profile_without_overlay_args() {
        let error = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "copy",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--source-block-store-profile",
            "production",
            "--source-block-store-container-sas-url",
            "https://example.blob.core.windows.net/archive-sync?sig=test",
            "--destination-block-store-root",
            "destination-blocks",
        ])
        .unwrap_err()
        .to_string();

        assert!(error.contains("--source-block-store-filesystem-cache-root"));
        assert!(error.contains("--source-block-store-memory-cache-max-resident-blocks"));
    }

    #[test]
    fn copy_command_parses_production_v2_source_without_overlay_cache_fields() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "copy",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--source-block-store-profile",
            "production-v2",
            "--source-block-store-container-sas-url",
            "https://example.table.core.windows.net/archive-sync?sig=test",
            "--destination-block-store-root",
            "destination-blocks",
        ])
        .unwrap();

        match cli.command {
            Command::Copy {
                source_block_store,
                destination_block_store,
                ..
            } => {
                assert_eq!(
                    source_block_store.source_block_store_profile,
                    ReadableBlockStoreProfile::ProductionV2
                );
                assert_eq!(
                    destination_block_store.destination_block_store_profile,
                    WritableBlockStoreProfile::Local
                );
                let environment = source_block_store.try_environment_config().unwrap();
                match environment {
                    EnvironmentConfig::ProductionV2 { block_store, .. } => {
                        assert_eq!(
                            block_store.container_sas_url,
                            "https://example.table.core.windows.net/archive-sync?sig=test"
                        );
                        assert_eq!(block_store.filesystem_cache_root, None);
                        assert_eq!(block_store.memory_cache_max_resident_blocks, None);
                    }
                    EnvironmentConfig::Local { .. }
                    | EnvironmentConfig::LocalRedb { .. }
                    | EnvironmentConfig::LocalOverlay { .. }
                    | EnvironmentConfig::Production { .. } => {
                        panic!("expected production-v2 environment")
                    }
                }
            }
            _ => panic!("expected copy command"),
        }
    }

    #[test]
    fn copy_command_parses_gateway_http3_source_args() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "copy",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--source-block-store-profile",
            "gateway-http3",
            "--source-block-store-gateway-dns-name",
            "gateway.example.com",
            "--destination-block-store-root",
            "destination-blocks",
        ])
        .unwrap();

        match cli.command {
            Command::Copy {
                source_block_store,
                destination_block_store,
                ..
            } => {
                assert_eq!(
                    source_block_store.source_block_store_profile,
                    ReadableBlockStoreProfile::GatewayHttp3
                );
                assert_eq!(
                    source_block_store.source_block_store_gateway_dns_name,
                    Some("gateway.example.com".into())
                );
                assert_eq!(
                    destination_block_store.destination_block_store_profile,
                    WritableBlockStoreProfile::Local
                );
            }
            _ => panic!("expected copy command"),
        }
    }

    #[test]
    fn copy_command_parses_blind_write_flag() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "copy",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--blind-write",
            "--source-block-store-root",
            "source-blocks",
            "--destination-block-store-root",
            "destination-blocks",
        ])
        .unwrap();

        match cli.command {
            Command::Copy { blind_write, .. } => assert!(blind_write),
            _ => panic!("expected copy command"),
        }
    }

    #[test]
    fn copy_command_defaults_max_in_flight_destination_writes() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "copy",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--source-block-store-root",
            "source-blocks",
            "--destination-block-store-root",
            "destination-blocks",
        ])
        .unwrap();

        match cli.command {
            Command::Copy {
                max_in_flight_destination_writes,
                ..
            } => assert_eq!(
                max_in_flight_destination_writes,
                DEFAULT_MAX_IN_FLIGHT_DESTINATION_WRITES
            ),
            _ => panic!("expected copy command"),
        }
    }

    #[test]
    fn copy_command_parses_max_in_flight_destination_writes_override() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "copy",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--max-in-flight-destination-writes",
            "17",
            "--source-block-store-root",
            "source-blocks",
            "--destination-block-store-root",
            "destination-blocks",
        ])
        .unwrap();

        match cli.command {
            Command::Copy {
                max_in_flight_destination_writes,
                ..
            } => assert_eq!(max_in_flight_destination_writes, 17),
            _ => panic!("expected copy command"),
        }
    }

    #[test]
    fn copy_command_rejects_zero_max_in_flight_destination_writes() {
        let error = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "copy",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--max-in-flight-destination-writes",
            "0",
            "--source-block-store-root",
            "source-blocks",
            "--destination-block-store-root",
            "destination-blocks",
        ])
        .unwrap_err();

        let rendered = error.to_string();
        assert!(rendered.contains("--max-in-flight-destination-writes"));
    }

    #[test]
    fn normalize_embedding_base_url_accepts_full_embeddings_path() {
        assert_eq!(
            normalize_embedding_base_url("http://localhost:8080/v1/embeddings"),
            "http://localhost:8080"
        );
        assert_eq!(
            normalize_embedding_base_url("http://localhost:8080/v1/embeddings/"),
            "http://localhost:8080"
        );
        assert_eq!(
            normalize_embedding_base_url("http://localhost:8080"),
            "http://localhost:8080"
        );
    }

    #[tokio::test]
    async fn copy_liveness_heartbeat_emits_for_slow_operations() {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&messages);
        let start = std::time::Instant::now();

        let output = await_with_copy_liveness(
            async {
                sleep(Duration::from_millis(25)).await;
                7usize
            },
            Duration::from_millis(5),
            move || {
                let message = format_copy_liveness_message(
                    2,
                    start.elapsed(),
                    RootedBlockCopyProgressSnapshot {
                        destination_mode: CopyDestinationMode::ReadBeforeWrite,
                        read_source_block_count: 5,
                        copied_block_count: Some(2),
                        skipped_already_present_block_count: Some(3),
                        attempted_write_block_count: None,
                        failed_block_count: 0,
                    },
                );
                captured.lock().unwrap().push(message.clone());
                message
            },
        )
        .await;

        assert_eq!(output, 7);
        let messages = messages.lock().unwrap();
        assert!(!messages.is_empty());
        assert!(messages[0].contains("still running"));
        assert!(messages[0].contains("2 requested root(s)"));
        assert!(messages[0].contains("read 5"));
        assert!(messages[0].contains("copied 2"));
        assert!(messages[0].contains("skipped 3"));
    }

    #[tokio::test]
    async fn copy_liveness_heartbeat_emits_for_sync_blocking_operations() {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&messages);
        let start = std::time::Instant::now();

        let output = await_with_copy_liveness(
            async {
                std::thread::sleep(Duration::from_millis(25));
                13usize
            },
            Duration::from_millis(5),
            move || {
                let message = format_copy_liveness_message(
                    1,
                    start.elapsed(),
                    RootedBlockCopyProgressSnapshot {
                        destination_mode: CopyDestinationMode::BlindWrite,
                        read_source_block_count: 4,
                        copied_block_count: None,
                        skipped_already_present_block_count: None,
                        attempted_write_block_count: Some(4),
                        failed_block_count: 1,
                    },
                );
                captured.lock().unwrap().push(message.clone());
                message
            },
        )
        .await;

        assert_eq!(output, 13);
        let messages = messages.lock().unwrap();
        assert!(!messages.is_empty());
        assert!(messages[0].contains("attempted 4"));
        assert!(messages[0].contains("failed 1"));
    }

    #[tokio::test]
    async fn copy_liveness_heartbeat_stays_quiet_for_fast_operations() {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&messages);
        let start = std::time::Instant::now();

        let output =
            await_with_copy_liveness(async { 11usize }, Duration::from_millis(20), move || {
                let message = format_copy_liveness_message(
                    1,
                    start.elapsed(),
                    RootedBlockCopyProgressSnapshot {
                        destination_mode: CopyDestinationMode::BlindWrite,
                        read_source_block_count: 4,
                        copied_block_count: None,
                        skipped_already_present_block_count: None,
                        attempted_write_block_count: Some(4),
                        failed_block_count: 1,
                    },
                );
                captured.lock().unwrap().push(message.clone());
                message
            })
            .await;

        assert_eq!(output, 11);
        assert!(messages.lock().unwrap().is_empty());
    }

    #[test]
    fn rust_log_request_detects_non_empty_filter_values() {
        assert!(!rust_log_requested_with(None));
        assert!(!rust_log_requested_with(Some(OsStr::new(""))));
        assert!(!rust_log_requested_with(Some(OsStr::new("   "))));
        assert!(rust_log_requested_with(Some(OsStr::new(
            "azure_core=debug,reqwest=trace"
        ))));
    }
}
