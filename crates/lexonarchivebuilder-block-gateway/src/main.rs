// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use anyhow::Context;
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use lexonarchivebuilder_block_gateway::{GatewayConfig, GatewayStorageProfile, serve};
use tracing_subscriber::EnvFilter;

const BLOCK_GATEWAY_SAS_URL_ENV: &str = "LEXONARCHIVEBUILDER_BLOCK_GATEWAY_SAS_URL";
const BLOCK_GATEWAY_BLOCK_STORE_FILESYSTEM_CACHE_ROOT_ENV: &str =
    "LEXONARCHIVEBUILDER_BLOCK_GATEWAY_BLOCK_STORE_FILESYSTEM_CACHE_ROOT";
const BLOCK_GATEWAY_BLOCK_STORE_MEMORY_CACHE_MAX_RESIDENT_BLOCKS_ENV: &str =
    "LEXONARCHIVEBUILDER_BLOCK_GATEWAY_BLOCK_STORE_MEMORY_CACHE_MAX_RESIDENT_BLOCKS";
const BLOCK_GATEWAY_BLOCK_STORE_PREFIX_ENV: &str =
    "LEXONARCHIVEBUILDER_BLOCK_GATEWAY_BLOCK_STORE_PREFIX";

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
        #[arg(long, required_if_eq("storage_profile", "local-redb"))]
        block_store_root: Option<PathBuf>,
        #[arg(long)]
        block_store_container_sas_url: Option<String>,
        #[arg(long)]
        block_store_filesystem_cache_root: Option<PathBuf>,
        #[arg(long)]
        block_store_memory_cache_max_resident_blocks: Option<usize>,
        #[arg(
            long,
            help = "Reserved for non-Azure block store backends. The approved Azure Table-backed gateway profiles reject non-empty prefixes."
        )]
        block_store_prefix: Option<String>,
        #[arg(long)]
        certificate: PathBuf,
        #[arg(long)]
        private_key: PathBuf,
    },
}

#[derive(Debug, Clone)]
struct GatewayServeArgs {
    listen_addr: SocketAddr,
    storage_profile: GatewayStorageProfile,
    block_store_root: Option<PathBuf>,
    block_store_container_sas_url: Option<String>,
    block_store_filesystem_cache_root: Option<PathBuf>,
    block_store_memory_cache_max_resident_blocks: Option<usize>,
    block_store_prefix: Option<String>,
    certificate_path: PathBuf,
    private_key_path: PathBuf,
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
            block_store_root,
            block_store_container_sas_url,
            block_store_filesystem_cache_root,
            block_store_memory_cache_max_resident_blocks,
            block_store_prefix,
            certificate,
            private_key,
        } => {
            let config = gateway_config_from_args(GatewayServeArgs {
                listen_addr,
                storage_profile,
                block_store_root,
                block_store_container_sas_url,
                block_store_filesystem_cache_root,
                block_store_memory_cache_max_resident_blocks,
                block_store_prefix,
                certificate_path: certificate,
                private_key_path: private_key,
            })?;
            serve(config).await
        }
    }
}

fn gateway_config_from_args(args: GatewayServeArgs) -> anyhow::Result<GatewayConfig> {
    gateway_config_from_args_with_env(
        args,
        |name| std::env::var(name).ok(),
        |name| std::env::var(name).ok(),
    )
}

fn gateway_config_from_args_with_env<ReadStringEnv, ReadPathEnv>(
    args: GatewayServeArgs,
    read_string_env: ReadStringEnv,
    read_path_env: ReadPathEnv,
) -> anyhow::Result<GatewayConfig>
where
    ReadStringEnv: Fn(&str) -> Option<String>,
    ReadPathEnv: Fn(&str) -> Option<String>,
{
    let block_store_container_sas_url = match args.storage_profile {
        GatewayStorageProfile::LocalRedb => args.block_store_container_sas_url.unwrap_or_default(),
        GatewayStorageProfile::Production | GatewayStorageProfile::ProductionV2 => args
            .block_store_container_sas_url
            .or_else(|| read_string_env(BLOCK_GATEWAY_SAS_URL_ENV))
            .unwrap_or_default(),
    };
    let block_store_filesystem_cache_root = match args.storage_profile {
        GatewayStorageProfile::Production => args.block_store_filesystem_cache_root.or_else(|| {
            read_path_env(BLOCK_GATEWAY_BLOCK_STORE_FILESYSTEM_CACHE_ROOT_ENV).map(PathBuf::from)
        }),
        GatewayStorageProfile::LocalRedb | GatewayStorageProfile::ProductionV2 => {
            args.block_store_filesystem_cache_root
        }
    };
    let block_store_memory_cache_max_resident_blocks = match args.storage_profile {
        GatewayStorageProfile::Production => match args.block_store_memory_cache_max_resident_blocks
        {
                Some(value) => Some(value),
                None => read_string_env(BLOCK_GATEWAY_BLOCK_STORE_MEMORY_CACHE_MAX_RESIDENT_BLOCKS_ENV)
                    .map(|value| {
                        value.parse::<usize>().with_context(|| {
                            format!(
                                "{BLOCK_GATEWAY_BLOCK_STORE_MEMORY_CACHE_MAX_RESIDENT_BLOCKS_ENV} must parse as an unsigned integer"
                            )
                        })
                    })
                    .transpose()?,
            },
        GatewayStorageProfile::LocalRedb | GatewayStorageProfile::ProductionV2 => {
            args.block_store_memory_cache_max_resident_blocks
        }
    };
    let block_store_prefix = match args.storage_profile {
        GatewayStorageProfile::LocalRedb => args.block_store_prefix,
        GatewayStorageProfile::Production | GatewayStorageProfile::ProductionV2 => args
            .block_store_prefix
            .or_else(|| read_string_env(BLOCK_GATEWAY_BLOCK_STORE_PREFIX_ENV)),
    };

    Ok(GatewayConfig {
        listen_addr: args.listen_addr,
        storage_profile: args.storage_profile,
        block_store_root: args.block_store_root,
        block_store_container_sas_url,
        block_store_filesystem_cache_root,
        block_store_memory_cache_max_resident_blocks,
        block_store_prefix,
        certificate_path: args.certificate_path,
        private_key_path: args.private_key_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_profile_reads_environment_defaults() {
        let config = gateway_config_from_args_with_env(
            GatewayServeArgs {
                listen_addr: "127.0.0.1:443".parse().unwrap(),
                storage_profile: GatewayStorageProfile::Production,
                block_store_root: None,
                block_store_container_sas_url: None,
                block_store_filesystem_cache_root: None,
                block_store_memory_cache_max_resident_blocks: None,
                block_store_prefix: None,
                certificate_path: PathBuf::from("cert.pem"),
                private_key_path: PathBuf::from("key.pem"),
            },
            |name| match name {
                BLOCK_GATEWAY_SAS_URL_ENV => {
                    Some("https://example.table.core.windows.net/table?sig=test".into())
                }
                BLOCK_GATEWAY_BLOCK_STORE_MEMORY_CACHE_MAX_RESIDENT_BLOCKS_ENV => Some("64".into()),
                _ => None,
            },
            |name| match name {
                BLOCK_GATEWAY_BLOCK_STORE_FILESYSTEM_CACHE_ROOT_ENV => Some("cache".into()),
                _ => None,
            },
        )
        .unwrap();

        assert_eq!(
            config.block_store_container_sas_url,
            "https://example.table.core.windows.net/table?sig=test"
        );
        assert_eq!(
            config.block_store_filesystem_cache_root,
            Some(PathBuf::from("cache"))
        );
        assert_eq!(
            config.block_store_memory_cache_max_resident_blocks,
            Some(64)
        );
    }

    #[test]
    fn local_redb_profile_ignores_production_environment_defaults() {
        let config = gateway_config_from_args_with_env(
            GatewayServeArgs {
                listen_addr: "127.0.0.1:443".parse().unwrap(),
                storage_profile: GatewayStorageProfile::LocalRedb,
                block_store_root: Some(PathBuf::from("blocks")),
                block_store_container_sas_url: None,
                block_store_filesystem_cache_root: None,
                block_store_memory_cache_max_resident_blocks: None,
                block_store_prefix: None,
                certificate_path: PathBuf::from("cert.pem"),
                private_key_path: PathBuf::from("key.pem"),
            },
            |_| Some("unexpected".into()),
            |_| Some("unexpected".into()),
        )
        .unwrap();

        assert_eq!(config.block_store_root, Some(PathBuf::from("blocks")));
        assert_eq!(config.block_store_container_sas_url, "");
        assert_eq!(config.block_store_filesystem_cache_root, None);
        assert_eq!(config.block_store_memory_cache_max_resident_blocks, None);
        assert_eq!(config.block_store_prefix, None);
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
    fn serve_command_allows_production_profile_without_inline_cache_arguments() {
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
            result.is_ok(),
            "production profile parsing should defer cache defaults/validation until runtime"
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

    #[test]
    fn serve_command_accepts_local_redb_profile_with_block_store_root() {
        let cli = Cli::try_parse_from([
            "lexonarchivebuilder-block-gateway",
            "serve",
            "--storage-profile",
            "local-redb",
            "--block-store-root",
            "blocks",
            "--certificate",
            "cert.pem",
            "--private-key",
            "key.pem",
        ])
        .expect("local-redb profile arguments should parse");

        let Command::Serve {
            storage_profile,
            block_store_root,
            ..
        } = cli.command;
        assert_eq!(storage_profile, GatewayStorageProfile::LocalRedb);
        assert_eq!(block_store_root, Some(PathBuf::from("blocks")));
    }
}
