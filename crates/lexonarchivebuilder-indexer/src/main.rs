use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use lexonarchivebuilder_indexer::block_store::ConfiguredBlockStore;
use lexonarchivebuilder_indexer::config::{
    EnvironmentConfig, LocalEmbeddingConfig, ProductionBlockStoreConfig, ProductionEmbeddingConfig,
};
use lexonarchivebuilder_indexer::embedding::ConfiguredEmbeddingProvider;
use lexonarchivebuilder_indexer::quality::{
    TnnRecallConfig, assess_rooted_tree_with_config, assess_tnn_falsification,
    default_falsification_report_path, default_report_path as default_quality_report_path,
    default_tnn_recall_sample_size, default_tnn_recall_seed, render_falsification_report_summary,
    render_report_summary, write_falsification_report, write_report as write_quality_report,
};
use lexonarchivebuilder_indexer::search::{
    default_report_path as default_search_report_path,
    default_traversal_width as default_search_traversal_width,
    render_report_summary as render_search_report_summary, search_rooted_tree,
    write_report as write_search_report,
};
use lexonarchivebuilder_indexer::tree_tools::parse_block_hash;
use lexonarchivebuilder_indexer::{
    ClusteringConfigOverrides, ExecutionStage, run_request_file_with_outputs, write_summary_file,
};

const DEFAULT_LOCAL_MODEL: &str = "all-MiniLM-L6-v2";
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_MAX_RETRIES: u32 = 5;
const DEFAULT_RETRY_DELAY_MS: u64 = 1_000;
const STRUCTURAL_FINDINGS_EXIT_CODE: i32 = 2;

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
        #[arg(long)]
        json_out: Option<PathBuf>,
        #[command(flatten)]
        block_store: BlockStoreArgs,
    },
    QualityFalsification {
        #[arg(long)]
        root_id: String,
        #[arg(long, default_value_t = default_tnn_recall_sample_size())]
        tnn_recall_sample_size: usize,
        #[arg(long, default_value_t = default_tnn_recall_seed())]
        tnn_recall_seed: u64,
        #[arg(long, default_value_t = default_search_traversal_width())]
        traversal_width: usize,
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
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum BlockStoreProfile {
    Local,
    Production,
}

#[derive(Debug, Args)]
struct BlockStoreArgs {
    #[arg(long, value_enum, default_value_t = BlockStoreProfile::Local)]
    block_store_profile: BlockStoreProfile,
    #[arg(long, required_if_eq("block_store_profile", "local"))]
    block_store_root: Option<PathBuf>,
    #[arg(long, required_if_eq("block_store_profile", "production"))]
    block_store_account_url: Option<String>,
    #[arg(long, required_if_eq("block_store_profile", "production"))]
    block_store_container: Option<String>,
    #[arg(long)]
    block_store_prefix: Option<String>,
}

impl BlockStoreArgs {
    fn to_environment_config(&self) -> EnvironmentConfig {
        match self.block_store_profile {
            BlockStoreProfile::Local => EnvironmentConfig::Local {
                block_store_root: self
                    .block_store_root
                    .clone()
                    .expect("local block_store_root is required by clap"),
                embedding: unused_local_embedding(),
            },
            BlockStoreProfile::Production => EnvironmentConfig::Production {
                block_store: ProductionBlockStoreConfig {
                    account_url: self
                        .block_store_account_url
                        .clone()
                        .expect("production account_url is required by clap"),
                    container: self
                        .block_store_container
                        .clone()
                        .expect("production container is required by clap"),
                    prefix: self.block_store_prefix.clone(),
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://unused.production.example".into(),
                    deployment: "unused".into(),
                    api_version: "2024-02-01".into(),
                    api_key_env: None,
                },
            },
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Run {
            request,
            summary_out,
            stage,
            clustering,
        } => {
            let summary =
                run_request_file_with_outputs(&request, stage, clustering, summary_out.as_deref())
                    .await
                    .with_context(|| format!("failed to run request {}", request.display()))?;
            let rendered =
                serde_json::to_string_pretty(&summary).context("failed to render batch summary")?;
            if let Some(output_path) = summary_out.as_ref() {
                write_summary_file(output_path, &summary)?;
            }
            println!("{rendered}");
        }
        Command::Quality {
            root_id,
            tnn_recall_sample_size,
            tnn_recall_seed,
            traversal_width,
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
        Command::QualityFalsification {
            root_id,
            tnn_recall_sample_size,
            tnn_recall_seed,
            traversal_width,
            json_out,
            block_store,
        } => {
            let root_id = parse_block_hash(&root_id)?;
            let store = configured_block_store(&block_store)?;
            let report = assess_tnn_falsification(
                &root_id,
                &store,
                TnnRecallConfig {
                    sample_size: tnn_recall_sample_size,
                    seed: tnn_recall_seed,
                    traversal_width,
                },
            )?;
            let output_path =
                json_out.unwrap_or_else(|| default_falsification_report_path(&root_id));
            write_falsification_report(&output_path, &report)?;
            println!("{}", render_falsification_report_summary(&report));
            println!("JSON report: {}", output_path.display());
        }
    }

    Ok(())
}

fn configured_block_store(args: &BlockStoreArgs) -> anyhow::Result<ConfiguredBlockStore> {
    ConfiguredBlockStore::from_environment(Path::new("."), &args.to_environment_config())
        .context("failed to configure block store")
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

#[cfg(test)]
mod tests {
    use super::*;

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
                assert_eq!(block_store.block_store_profile, BlockStoreProfile::Local);
                assert_eq!(block_store.block_store_root, Some(PathBuf::from("blocks")));
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
    fn quality_falsification_command_parses_local_block_store_args() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "quality-falsification",
            "--root-id",
            "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
            "--tnn-recall-sample-size",
            "17",
            "--tnn-recall-seed",
            "9",
            "--traversal-width",
            "16",
            "--block-store-root",
            "blocks",
        ])
        .unwrap();

        match cli.command {
            Command::QualityFalsification {
                root_id,
                tnn_recall_sample_size,
                tnn_recall_seed,
                traversal_width,
                block_store,
                ..
            } => {
                assert_eq!(
                    root_id,
                    "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
                );
                assert_eq!(tnn_recall_sample_size, 17);
                assert_eq!(tnn_recall_seed, 9);
                assert_eq!(traversal_width, 16);
                assert_eq!(block_store.block_store_profile, BlockStoreProfile::Local);
                assert_eq!(block_store.block_store_root, Some(PathBuf::from("blocks")));
            }
            _ => panic!("expected quality-falsification command"),
        }
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
}
