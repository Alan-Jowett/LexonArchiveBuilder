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
    use std::sync::{Mutex, OnceLock};

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn serve_command_accepts_sas_url_from_environment() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("environment lock poisoned");
        let previous = std::env::var_os("LEXONARCHIVEBUILDER_BLOCK_GATEWAY_SAS_URL");
        // SAFETY: this test serializes environment access with ENV_LOCK and restores the prior
        // value before returning.
        unsafe {
            std::env::set_var(
                "LEXONARCHIVEBUILDER_BLOCK_GATEWAY_SAS_URL",
                "https://example.table.core.windows.net/archive?sig=test",
            );
        }
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-block-gateway",
            "serve",
            "--certificate",
            "cert.pem",
            "--private-key",
            "key.pem",
        ]);
        // SAFETY: this test serializes environment access with ENV_LOCK and restores the prior
        // value before releasing the lock.
        unsafe {
            if let Some(previous) = previous {
                std::env::set_var("LEXONARCHIVEBUILDER_BLOCK_GATEWAY_SAS_URL", previous);
            } else {
                std::env::remove_var("LEXONARCHIVEBUILDER_BLOCK_GATEWAY_SAS_URL");
            }
        }
        let cli = cli.expect("command should parse SAS URL from environment");

        match cli.command {
            Command::Serve { sas_url, .. } => assert_eq!(
                sas_url,
                "https://example.table.core.windows.net/archive?sig=test"
            ),
        }
    }
}
