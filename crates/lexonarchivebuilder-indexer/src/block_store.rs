// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use lexongraph_block::BlockHash;
use lexongraph_block_store::{BlockIdIterator, BlockStore, BlockStoreError};
use lexongraph_block_store_azure::AzureBlobBlockStore;
use lexongraph_block_store_fs::FilesystemBlockStore;
use lexongraph_block_store_memory::MemoryBlockStore;
use lexongraph_block_store_overlay::{OverlayBlockStore, OverlayStoreLayer, PassiveLayer};

use crate::config::{EnvironmentConfig, ProductionBlockStoreConfig};
use crate::paths::resolve_path;

const AZURE_BLOCK_WRITE_RETRY_ATTEMPTS: u32 = 5;
const AZURE_BLOCK_WRITE_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub enum ConfiguredBlockStore {
    Local(FilesystemBlockStore),
    Overlay(Arc<OverlayBlockStore>),
}

impl ConfiguredBlockStore {
    pub fn from_environment(
        request_dir: &Path,
        environment: &EnvironmentConfig,
    ) -> Result<Self, BlockStoreError> {
        match environment {
            EnvironmentConfig::Local {
                block_store_root, ..
            } => FilesystemBlockStore::new(resolve_path(request_dir, block_store_root))
                .map(Self::Local),
            EnvironmentConfig::LocalOverlay { block_store, .. }
            | EnvironmentConfig::Production { block_store, .. } => {
                Self::production_store(request_dir, block_store)
            }
        }
    }

    fn production_store(
        request_dir: &Path,
        config: &ProductionBlockStoreConfig,
    ) -> Result<Self, BlockStoreError> {
        config
            .validate()
            .map_err(|error| BlockStoreError::BackendFailure(error.to_string()))?;
        let azure_backing_store = RetryingBlockStore::new(
            FreshAzureBlobBlockStore::new(&config.container_sas_url)?,
            AZURE_BLOCK_WRITE_RETRY_ATTEMPTS,
            AZURE_BLOCK_WRITE_RETRY_DELAY,
        );
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
}

impl BlockStore for ConfiguredBlockStore {
    fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        match self {
            Self::Local(store) => store.put_block_bytes(block_id, block_bytes),
            Self::Overlay(store) => store.put_block_bytes(block_id, block_bytes),
        }
    }

    fn get_block_bytes(&self, block_id: &BlockHash) -> Result<Option<Vec<u8>>, BlockStoreError> {
        match self {
            Self::Local(store) => store.get_block_bytes(block_id),
            Self::Overlay(store) => store.get_block_bytes(block_id),
        }
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        match self {
            Self::Local(store) => store.iter_block_ids(),
            Self::Overlay(store) => store.iter_block_ids(),
        }
    }
}

#[derive(Clone, Debug)]
struct FreshAzureBlobBlockStore {
    container_sas_url: String,
}

impl FreshAzureBlobBlockStore {
    fn new(container_sas_url: &str) -> Result<Self, BlockStoreError> {
        AzureBlobBlockStore::new(container_sas_url)?;
        Ok(Self {
            container_sas_url: container_sas_url.to_string(),
        })
    }

    fn with_store<T>(
        &self,
        execute: impl FnOnce(&AzureBlobBlockStore) -> Result<T, BlockStoreError>,
    ) -> Result<T, BlockStoreError> {
        let store = AzureBlobBlockStore::new(&self.container_sas_url)?;
        execute(&store)
    }
}

impl BlockStore for FreshAzureBlobBlockStore {
    fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        self.with_store(|store| store.put_block_bytes(block_id, block_bytes))
    }

    fn get_block_bytes(&self, block_id: &BlockHash) -> Result<Option<Vec<u8>>, BlockStoreError> {
        self.with_store(|store| store.get_block_bytes(block_id))
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        let block_ids = self.with_store(|store| {
            store
                .iter_block_ids()?
                .collect::<Result<Vec<_>, BlockStoreError>>()
        })?;
        Ok(Box::new(block_ids.into_iter().map(Ok)))
    }
}

#[derive(Debug)]
struct RetryingBlockStore<S> {
    store: S,
    max_attempts: u32,
    retry_delay: Duration,
}

impl<S> RetryingBlockStore<S> {
    fn new(store: S, max_attempts: u32, retry_delay: Duration) -> Self {
        Self {
            store,
            max_attempts,
            retry_delay,
        }
    }
}

impl<S: BlockStore> BlockStore for RetryingBlockStore<S> {
    fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        let max_attempts = self.max_attempts.max(1);
        let mut last_error = None;
        for attempt in 1..=max_attempts {
            match self.store.put_block_bytes(block_id, block_bytes) {
                Ok(()) => return Ok(()),
                Err(error) if attempt < max_attempts && should_retry_block_store_write(&error) => {
                    last_error = Some(error);
                    std::thread::sleep(self.retry_delay);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            BlockStoreError::BackendFailure("Azure block write retry loop exhausted".into())
        }))
    }

    fn get_block_bytes(&self, block_id: &BlockHash) -> Result<Option<Vec<u8>>, BlockStoreError> {
        self.store.get_block_bytes(block_id)
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        self.store.iter_block_ids()
    }
}

fn should_retry_block_store_write(error: &BlockStoreError) -> bool {
    let BlockStoreError::BackendFailure(message) = error else {
        return false;
    };

    let message = message.to_ascii_lowercase();
    message.contains("error sending request")
        || message.contains("connection reset")
        || message.contains("connection refused")
        || message.contains("connection aborted")
        || message.contains("broken pipe")
        || message.contains("dns error")
        || message.contains("temporary failure")
        || message.contains("timed out")
        || message.contains("request timeout")
        || message.contains("too many requests")
        || message.contains("500 internal server error")
        || message.contains("502 bad gateway")
        || message.contains("503 service unavailable")
        || message.contains("504 gateway timeout")
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use lexongraph_block::{Block, Content, EmbeddingSpec, LeafBlock, LeafEntry, VERSION_1};
    use tempfile::tempdir;

    use super::*;
    use crate::config::ProductionEmbeddingConfig;

    #[test]
    fn local_filesystem_store_uses_upstream_layout() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();
        let block = sample_block();
        let block_id = store.put(&block).unwrap();
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
                .contains("production block_store.prefix is not supported")
        );
    }

    #[test]
    fn configured_local_store_delegates_iter_block_ids() {
        let dir = tempdir().unwrap();
        let store = ConfiguredBlockStore::Local(
            FilesystemBlockStore::new(dir.path().join("blocks")).unwrap(),
        );
        let block = sample_block();
        let block_id = store.put(&block).unwrap();

        let block_ids = store
            .iter_block_ids()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
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
                .contains("production block_store.filesystem_cache_root is required")
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

    #[test]
    fn retrying_block_store_retries_transient_backend_failures() {
        let attempts = Rc::new(Cell::new(0_u32));
        let attempts_for_put = Rc::clone(&attempts);
        let store = RetryingBlockStore::new(
            TestBlockStore {
                put_fn: Box::new(move |_, _| {
                    attempts_for_put.set(attempts_for_put.get() + 1);
                    if attempts_for_put.get() < 3 {
                        Err(BlockStoreError::BackendFailure(
                            "failed to publish block: error sending request".into(),
                        ))
                    } else {
                        Ok(())
                    }
                }),
            },
            3,
            Duration::ZERO,
        );

        store
            .put_block_bytes(&sample_block_id(), b"payload")
            .unwrap();
        assert_eq!(attempts.get(), 3);
    }

    #[test]
    fn retrying_block_store_does_not_retry_non_retryable_failures() {
        let attempts = Rc::new(Cell::new(0_u32));
        let attempts_for_put = Rc::clone(&attempts);
        let store = RetryingBlockStore::new(
            TestBlockStore {
                put_fn: Box::new(move |_, _| {
                    attempts_for_put.set(attempts_for_put.get() + 1);
                    Err(BlockStoreError::BackendFailure(
                        "failed to publish block: backend returned 403 Forbidden".into(),
                    ))
                }),
            },
            5,
            Duration::ZERO,
        );

        let error = store
            .put_block_bytes(&sample_block_id(), b"payload")
            .unwrap_err();
        assert_eq!(attempts.get(), 1);
        assert!(error.to_string().contains("403 Forbidden"));
    }

    #[test]
    fn retrying_block_store_stops_after_max_attempts() {
        let attempts = Rc::new(Cell::new(0_u32));
        let attempts_for_put = Rc::clone(&attempts);
        let store = RetryingBlockStore::new(
            TestBlockStore {
                put_fn: Box::new(move |_, _| {
                    attempts_for_put.set(attempts_for_put.get() + 1);
                    Err(BlockStoreError::BackendFailure(
                        "failed to publish block: error sending request".into(),
                    ))
                }),
            },
            4,
            Duration::ZERO,
        );

        let error = store
            .put_block_bytes(&sample_block_id(), b"payload")
            .unwrap_err();
        assert_eq!(attempts.get(), 4);
        assert!(error.to_string().contains("error sending request"));
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

    fn sample_block_id() -> BlockHash {
        lexongraph_block::serialize_block(&sample_block())
            .unwrap()
            .hash
    }

    type PutFn = dyn Fn(&BlockHash, &[u8]) -> Result<(), BlockStoreError>;

    struct TestBlockStore {
        put_fn: Box<PutFn>,
    }

    impl BlockStore for TestBlockStore {
        fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            (self.put_fn)(block_id, block_bytes)
        }

        fn get_block_bytes(
            &self,
            _block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            Ok(None)
        }

        fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
            Ok(Box::new(std::iter::empty()))
        }
    }
}
