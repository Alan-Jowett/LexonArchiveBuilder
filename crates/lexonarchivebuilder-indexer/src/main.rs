use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use lexonarchivebuilder_indexer::{
    ExecutionStage, run_request_file_with_stage, write_summary_file,
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
        } => {
            let summary = run_request_file_with_stage(&request, stage)
                .await
                .with_context(|| format!("failed to run request {}", request.display()))?;
            let rendered =
                serde_json::to_string_pretty(&summary).context("failed to render batch summary")?;
            if let Some(output_path) = summary_out {
                write_summary_file(&output_path, &summary)?;
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
}
