// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use lexonarchivebuilder_block_gateway::{GatewayConfig, serve};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(author, version, about = "LexonArchiveBuilder immutable block gateway")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value = "0.0.0.0:443")]
        listen_addr: SocketAddr,
        #[arg(long, env = "LEXONARCHIVEBUILDER_BLOCK_GATEWAY_SAS_URL")]
        sas_url: String,
        #[arg(long)]
        certificate: PathBuf,
        #[arg(long)]
        private_key: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Serve {
            listen_addr,
            sas_url,
            certificate,
            private_key,
        } => {
            serve(GatewayConfig {
                listen_addr,
                sas_url,
                certificate_path: certificate,
                private_key_path: private_key,
            })
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn serve_command_binds_sas_url_environment_variable() {
        let command = Cli::command();
        let serve = command
            .get_subcommands()
            .find(|subcommand| subcommand.get_name() == "serve")
            .expect("serve subcommand should exist");
        let sas_url = serve
            .get_arguments()
            .find(|argument| argument.get_id().as_str() == "sas_url")
            .expect("sas_url argument should exist");

        assert_eq!(
            sas_url
                .get_env()
                .expect("sas_url should expose an environment variable")
                .to_string_lossy(),
            "LEXONARCHIVEBUILDER_BLOCK_GATEWAY_SAS_URL"
        );
    }
}
