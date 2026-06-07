use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand};
use lexonarchivebuilder_mcp::{McpRuntime, serve_stdio};

#[derive(Debug, Parser)]
#[command(author, version, about = "LexonArchiveBuilder MCP server MVP")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Serve { config } => {
            let runtime = Arc::new(
                McpRuntime::from_config_file(&config)
                    .with_context(|| format!("failed to load MCP config {}", config.display()))?,
            );
            serve_stdio(runtime).await?;
        }
    }

    Ok(())
}
