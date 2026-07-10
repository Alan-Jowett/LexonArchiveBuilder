// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::anyhow;
use clap::{Parser, Subcommand};
use lexonarchivebuilder_block_gateway::{GatewayConfig, GatewayStorageProfile, serve};
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
        #[arg(long, value_enum)]
        storage_profile: GatewayStorageProfile,
        #[arg(
            long,
            env = "LEXONARCHIVEBUILDER_BLOCK_GATEWAY_SAS_URL",
            required_if_eq_any([
                ("storage_profile", "production"),
                ("storage_profile", "production-v2"),
            ])
        )]
        block_store_container_sas_url: Option<String>,
        #[arg(
            long,
            env = "LEXONARCHIVEBUILDER_BLOCK_GATEWAY_BLOCK_STORE_FILESYSTEM_CACHE_ROOT",
            required_if_eq("storage_profile", "production")
        )]
        block_store_filesystem_cache_root: Option<PathBuf>,
        #[arg(
            long,
            env = "LEXONARCHIVEBUILDER_BLOCK_GATEWAY_BLOCK_STORE_MEMORY_CACHE_MAX_RESIDENT_BLOCKS",
            required_if_eq("storage_profile", "production")
        )]
        block_store_memory_cache_max_resident_blocks: Option<usize>,
        #[arg(
            long,
            env = "LEXONARCHIVEBUILDER_BLOCK_GATEWAY_BLOCK_STORE_PREFIX",
            help = "Reserved for non-Azure block store backends. The approved Azure Table-backed gateway profiles reject non-empty prefixes."
        )]
        block_store_prefix: Option<String>,
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
            storage_profile,
            block_store_container_sas_url,
            block_store_filesystem_cache_root,
            block_store_memory_cache_max_resident_blocks,
            block_store_prefix,
            certificate,
            private_key,
        } => {
            serve(GatewayConfig {
                listen_addr,
                storage_profile,
                block_store_container_sas_url: block_store_container_sas_url.ok_or_else(|| {
                    anyhow!(
                        "block_store_container_sas_url is required for gateway storage profile {}",
                        storage_profile
                    )
                })?,
                block_store_filesystem_cache_root,
                block_store_memory_cache_max_resident_blocks,
                block_store_prefix,
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
    fn serve_command_binds_container_sas_url_environment_variable() {
        let command = Cli::command();
        let serve = command
            .get_subcommands()
            .find(|subcommand| subcommand.get_name() == "serve")
            .expect("serve subcommand should exist");
        let container_sas_url = serve
            .get_arguments()
            .find(|argument| argument.get_id().as_str() == "block_store_container_sas_url")
            .expect("block_store_container_sas_url argument should exist");

        assert_eq!(
            container_sas_url
                .get_env()
                .expect("block_store_container_sas_url should expose an environment variable")
                .to_string_lossy(),
            "LEXONARCHIVEBUILDER_BLOCK_GATEWAY_SAS_URL"
        );
    }

    #[test]
    fn serve_command_requires_explicit_storage_profile() {
        let result = Cli::try_parse_from([
            "lexonarchivebuilder-block-gateway",
            "serve",
            "--block-store-container-sas-url",
            "https://example.table.core.windows.net/table?sig=test",
            "--certificate",
            "cert.pem",
            "--private-key",
            "key.pem",
        ]);

        assert!(result.is_err(), "storage_profile should be required");
    }

    #[test]
    fn serve_command_requires_overlay_cache_arguments_for_production_profile() {
        let result = Cli::try_parse_from([
            "lexonarchivebuilder-block-gateway",
            "serve",
            "--storage-profile",
            "production",
            "--block-store-container-sas-url",
            "https://example.table.core.windows.net/table?sig=test",
            "--certificate",
            "cert.pem",
            "--private-key",
            "key.pem",
        ]);

        assert!(
            result.is_err(),
            "overlay-backed production profile should require cache arguments"
        );
    }

    #[test]
    fn serve_command_accepts_direct_profile_without_overlay_cache_arguments() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-block-gateway",
            "serve",
            "--storage-profile",
            "production-v2",
            "--block-store-container-sas-url",
            "https://example.table.core.windows.net/table?sig=test",
            "--certificate",
            "cert.pem",
            "--private-key",
            "key.pem",
        ])
        .expect("direct profile arguments should parse");

        let Command::Serve {
            storage_profile, ..
        } = cli.command;
        assert_eq!(storage_profile, GatewayStorageProfile::ProductionV2);
    }
}
