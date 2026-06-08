use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use lexonarchivebuilder_indexer::{
    ClusteringConfigOverrides, ExecutionStage, run_request_file_with_outputs, write_summary_file,
};

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
    }

    Ok(())
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
        }
    }

    #[test]
    fn run_command_parses_directional_pca_clustering_options() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-indexer",
            "run",
            "--request",
            "request.json",
            "--clustering-algorithm",
            "directional-pca",
            "--clustering-cluster-count",
            "3",
            "--clustering-random-seed",
            "7",
            "--clustering-retained-dimension-count",
            "1",
            "--clustering-variance-exponent",
            "1.5",
            "--clustering-temperature",
            "0.75",
            "--clustering-min-input-count",
            "2",
            "--clustering-min-effective-rank",
            "1",
            "--clustering-min-cumulative-variance",
            "0.25",
        ])
        .unwrap();

        match cli.command {
            Command::Run { clustering, .. } => {
                assert_eq!(
                    clustering.clustering_algorithm,
                    Some(lexonarchivebuilder_indexer::ClusteringAlgorithm::DirectionalPca)
                );
                assert_eq!(clustering.clustering_cluster_count, Some(3));
                assert_eq!(clustering.clustering_random_seed, Some(7));
                assert_eq!(clustering.clustering_retained_dimension_count, Some(1));
                assert_eq!(clustering.clustering_variance_exponent, Some(1.5));
                assert_eq!(clustering.clustering_temperature, Some(0.75));
                assert_eq!(clustering.clustering_min_input_count, Some(2));
                assert_eq!(clustering.clustering_min_effective_rank, Some(1));
                assert_eq!(clustering.clustering_min_cumulative_variance, Some(0.25));
            }
        }
    }
}
