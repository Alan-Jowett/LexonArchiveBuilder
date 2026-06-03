use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use lexonfabric_indexer::{run_request_file, write_summary_file};

#[derive(Debug, Parser)]
#[command(author, version, about = "LexonFabric batch indexer MVP")]
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
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Run {
            request,
            summary_out,
        } => {
            let summary = run_request_file(&request)
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
