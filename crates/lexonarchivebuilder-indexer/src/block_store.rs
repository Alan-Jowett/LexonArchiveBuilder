// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use lexonarchivebuilder_block_store_http3::Http3BlockStore;
use lexongraph_block::BlockHash;
use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};
use lexongraph_block_store_azure_sdk::AzureBlobBlockStore;
use lexongraph_block_store_azure_table_v2::AzureTableBlockStoreV2;
use lexongraph_block_store_fs::FilesystemBlockStore;
use lexongraph_block_store_memory::MemoryBlockStore;
use lexongraph_block_store_overlay::{OverlayBlockStore, OverlayStoreLayer, PassiveLayer};
use lexongraph_block_store_redb::RedbBlockStore;

use crate::config::{EnvironmentConfig, ProductionBlockStoreConfig};
use crate::paths::resolve_path;

#[derive(Clone, Debug)]
pub enum ConfiguredBlockStore {
    GatewayHttp3(Http3BlockStore),
    Local(FilesystemBlockStore),
    LocalRedb(RedbBlockStore),
    Overlay(Arc<OverlayBlockStore>),
    AzureTable(AzureTableBlockStoreV2),
}

impl ConfiguredBlockStore {
    pub fn gateway_http3_store(gateway_dns_name: &str) -> Result<Self, BlockStoreError> {
        Http3BlockStore::new(gateway_dns_name).map(Self::GatewayHttp3)
    }

    pub fn from_environment(
        request_dir: &Path,
        environment: &EnvironmentConfig,
    ) -> Result<Self, BlockStoreError> {
        match environment {
            EnvironmentConfig::Local {
                block_store_root, ..
            } => FilesystemBlockStore::new(resolve_path(request_dir, block_store_root))
                .map(Self::Local),
            EnvironmentConfig::LocalRedb {
                block_store_root, ..
            } => RedbBlockStore::new(resolve_path(request_dir, block_store_root))
                .map(Self::LocalRedb),
            EnvironmentConfig::LocalOverlay { block_store, .. }
            | EnvironmentConfig::Production { block_store, .. } => {
                Self::production_overlay_store(request_dir, block_store)
            }
            EnvironmentConfig::ProductionV2 { block_store, .. } => {
                Self::production_v2_store(block_store)
            }
        }
    }

    fn production_overlay_store(
        request_dir: &Path,
        config: &ProductionBlockStoreConfig,
    ) -> Result<Self, BlockStoreError> {
        config
            .validate_for_overlay()
            .map_err(|error| BlockStoreError::BackendFailure(error.to_string()))?;
        let azure_backing_store = AzureBlobBlockStore::new(&config.container_sas_url)?;
        let memory_cache = MemoryBlockStore::new(
            config
                .memory_cache_max_resident_blocks
                .expect("validated overlay caches always include a memory capacity"),
        )
        .map_err(|error| BlockStoreError::BackendFailure(error.to_string()))?;
        let filesystem_cache = FilesystemBlockStore::new(resolve_path(
            request_dir,
            config
                .filesystem_cache_root
                .as_ref()
                .expect("validated overlay caches always include a filesystem cache root"),
        ))?;
        let layers: Vec<Box<dyn OverlayStoreLayer>> = vec![
            Box::new(PassiveLayer::cache(memory_cache)),
            Box::new(PassiveLayer::cache(filesystem_cache)),
            Box::new(PassiveLayer::writable(azure_backing_store)),
        ];
        let overlay_store = OverlayBlockStore::new(layers)
            .map_err(|error| BlockStoreError::BackendFailure(error.to_string()))?;
        Ok(Self::Overlay(Arc::new(overlay_store)))
    }

    fn production_v2_store(config: &ProductionBlockStoreConfig) -> Result<Self, BlockStoreError> {
        config
            .validate_for_azure_table()
            .map_err(|error| BlockStoreError::BackendFailure(error.to_string()))?;
        AzureTableBlockStoreV2::new(&config.container_sas_url).map(Self::AzureTable)
    }
}

pub(crate) fn block_on_block_store_future<F>(future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(future))
            }
            tokio::runtime::RuntimeFlavor::CurrentThread => std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("failed to build tokio runtime for block-store bridge")
                            .block_on(future)
                    })
                    .join()
                    .expect("block-store bridge thread panicked")
            }),
            _ => unreachable!("unsupported tokio runtime flavor"),
        }
    } else {
        block_on_future(future)
    }
}

pub(crate) fn block_on_future<F>(future: F) -> F::Output
where
    F: Future,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(future))
            }
            tokio::runtime::RuntimeFlavor::CurrentThread => {
                panic!(
                    "block_on_future cannot run inside a current-thread Tokio runtime; \
                     use block_on_future_factory to construct the future inside a bridge thread"
                )
            }
            _ => unreachable!("unsupported tokio runtime flavor"),
        }
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for block-store bridge")
            .block_on(future)
    }
}

pub(crate) fn block_on_future_factory<F, Fut, T>(make_future: F) -> T
where
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = T>,
    T: Send,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(make_future()))
            }
            tokio::runtime::RuntimeFlavor::CurrentThread => std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("failed to build tokio runtime for future bridge")
                            .block_on(make_future())
                    })
                    .join()
                    .expect("future bridge thread panicked")
            }),
            _ => unreachable!("unsupported tokio runtime flavor"),
        }
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for future bridge")
            .block_on(make_future())
    }
}

#[async_trait]
impl BlockStore for ConfiguredBlockStore {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        match self {
            Self::GatewayHttp3(store) => store.put_block_bytes(block_id, block_bytes).await,
            Self::Local(store) => store.put_block_bytes(block_id, block_bytes).await,
            Self::LocalRedb(store) => store.put_block_bytes(block_id, block_bytes).await,
            Self::Overlay(store) => store.put_block_bytes(block_id, block_bytes).await,
            Self::AzureTable(store) => store.put_block_bytes(block_id, block_bytes).await,
        }
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        match self {
            Self::GatewayHttp3(store) => store.get_block_bytes(block_id).await,
            Self::Local(store) => store.get_block_bytes(block_id).await,
            Self::LocalRedb(store) => store.get_block_bytes(block_id).await,
            Self::Overlay(store) => store.get_block_bytes(block_id).await,
            Self::AzureTable(store) => store.get_block_bytes(block_id).await,
        }
    }

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
        match self {
            Self::GatewayHttp3(store) => store.iter_block_ids(),
            Self::Local(store) => store.iter_block_ids(),
            Self::LocalRedb(store) => store.iter_block_ids(),
            Self::Overlay(store) => store.iter_block_ids(),
            Self::AzureTable(store) => store.iter_block_ids(),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use lexongraph_block::{Block, Content, EmbeddingSpec, LeafBlock, LeafEntry, VERSION_1};
    use tempfile::tempdir;

    use super::*;
    use crate::config::ProductionEmbeddingConfig;

    fn put_block(store: &impl BlockStore, block: &Block) -> BlockHash {
        block_on_block_store_future(store.put(block)).unwrap()
    }

    #[test]
    fn local_filesystem_store_uses_upstream_layout() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();
        let block = sample_block();
        let block_id = put_block(&store, &block);
        let block_id_text = block_id.to_string();
        let expected_path = dir
            .path()
            .join("blocks")
            .join(&block_id_text[..2])
            .join(&block_id_text[2..4])
            .join(format!("{block_id_text}.cbor"));

        assert!(expected_path.is_file());
    }

    #[test]
    fn configured_production_store_returns_explicit_backend_failure() {
        let error = ConfiguredBlockStore::from_environment(
            Path::new("."),
            &EnvironmentConfig::Production {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.blob.core.windows.net/archive-sync?sig=test".into(),
                    prefix: Some("archive-sync".into()),
                    filesystem_cache_root: None,
                    memory_cache_max_resident_blocks: None,
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://unused.production.example".into(),
                    deployment: "unused".into(),
                    api_version: "2024-02-01".into(),
                    api_key_env: None,
                },
            },
        )
        .unwrap_err();

        assert!(matches!(error, BlockStoreError::BackendFailure(_)));
        assert!(
            error
                .to_string()
                .contains("overlay block_store.prefix is not supported")
        );
    }

    #[test]
    fn configured_local_store_delegates_iter_block_ids() {
        let dir = tempdir().unwrap();
        let store = ConfiguredBlockStore::Local(
            FilesystemBlockStore::new(dir.path().join("blocks")).unwrap(),
        );
        let block = sample_block();
        let block_id = put_block(&store, &block);

        let block_ids = block_on_block_store_future(async {
            store.iter_block_ids()?.try_collect::<Vec<_>>().await
        })
        .unwrap();

        assert_eq!(block_ids, vec![block_id]);
    }

    #[test]
    fn configured_local_redb_store_delegates_iter_block_ids() {
        let dir = tempdir().unwrap();
        let store = ConfiguredBlockStore::LocalRedb(
            RedbBlockStore::new(dir.path().join("blocks")).unwrap(),
        );
        let block = sample_block();
        let block_id = put_block(&store, &block);

        let block_ids = block_on_block_store_future(async {
            store.iter_block_ids()?.try_collect::<Vec<_>>().await
        })
        .unwrap();

        assert_eq!(block_ids, vec![block_id]);
    }

    #[test]
    fn configured_production_store_requires_overlay_layers() {
        let error = ConfiguredBlockStore::from_environment(
            Path::new("."),
            &EnvironmentConfig::Production {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.blob.core.windows.net/archive-sync?sig=test".into(),
                    prefix: None,
                    filesystem_cache_root: None,
                    memory_cache_max_resident_blocks: None,
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://unused.production.example".into(),
                    deployment: "unused".into(),
                    api_version: "2024-02-01".into(),
                    api_key_env: None,
                },
            },
        )
        .unwrap_err();

        assert!(matches!(error, BlockStoreError::BackendFailure(_)));
        assert!(
            error
                .to_string()
                .contains("overlay block_store.filesystem_cache_root is required")
        );
    }

    #[test]
    fn configured_production_store_accepts_overlay_cache_layers() {
        let store = ConfiguredBlockStore::from_environment(
            Path::new("."),
            &EnvironmentConfig::Production {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.blob.core.windows.net/archive-sync?sig=test".into(),
                    prefix: None,
                    filesystem_cache_root: Some("cache".into()),
                    memory_cache_max_resident_blocks: Some(64),
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://unused.production.example".into(),
                    deployment: "unused".into(),
                    api_version: "2024-02-01".into(),
                    api_key_env: None,
                },
            },
        )
        .unwrap();

        assert!(matches!(store, ConfiguredBlockStore::Overlay(_)));
    }

    #[test]
    fn configured_production_v2_store_accepts_direct_table_config() {
        let store = ConfiguredBlockStore::from_environment(
            Path::new("."),
            &EnvironmentConfig::ProductionV2 {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.table.core.windows.net/archive-sync?sig=test".into(),
                    prefix: None,
                    filesystem_cache_root: None,
                    memory_cache_max_resident_blocks: None,
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://unused.production.example".into(),
                    deployment: "unused".into(),
                    api_version: "2024-02-01".into(),
                    api_key_env: None,
                },
            },
        )
        .unwrap();

        assert!(matches!(store, ConfiguredBlockStore::AzureTable(_)));
    }

    #[test]
    fn configured_local_overlay_store_accepts_overlay_cache_layers() {
        let store = ConfiguredBlockStore::from_environment(
            Path::new("."),
            &EnvironmentConfig::LocalOverlay {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.blob.core.windows.net/archive-sync?sig=test".into(),
                    prefix: None,
                    filesystem_cache_root: Some("cache".into()),
                    memory_cache_max_resident_blocks: Some(64),
                },
                embedding: crate::config::LocalEmbeddingConfig {
                    base_url: "http://localhost:8080".into(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 30,
                    max_retries: 5,
                    retry_delay_ms: 1_000,
                },
            },
        )
        .unwrap();

        assert!(matches!(store, ConfiguredBlockStore::Overlay(_)));
    }

    fn sample_block() -> Block {
        Block::Leaf(LeafBlock {
            version: VERSION_1,
            level: 0,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            entries: vec![LeafEntry {
                embedding: vec![0, 0, 0, 0, 0, 0, 0, 0],
                metadata: vec![],
                content: Content {
                    media_type: "text/plain".into(),
                    body: b"ignored".to_vec(),
                },
            }],
            ext: None,
        })
    }
}
