use std::collections::HashSet;
use std::path::Path;

use lexongraph_block::{Block, BlockHash};
use lexongraph_block_store::{BlockIdIterator, BlockStore, BlockStoreError};
use lexongraph_block_store_azure::AzureBlobBlockStore;
use lexongraph_block_store_fs::FilesystemBlockStore;
use lexongraph_block_store_memory::MemoryBlockStore;

use crate::config::{EnvironmentConfig, ProductionBlockStoreConfig};
use crate::paths::resolve_path;

#[derive(Clone, Debug)]
pub enum ConfiguredBlockStore {
    Local(FilesystemBlockStore),
    Overlay(ProductionOverlayBlockStore),
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
            EnvironmentConfig::Production { block_store, .. } => {
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
        Ok(Self::Overlay(ProductionOverlayBlockStore {
            memory_cache,
            filesystem_cache,
            azure_backing_store,
        }))
    }
}

impl BlockStore for ConfiguredBlockStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        match self {
            Self::Local(store) => store.put(block),
            Self::Overlay(store) => store.put(block),
        }
    }

    fn get(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        match self {
            Self::Local(store) => store.get(block_id),
            Self::Overlay(store) => store.get(block_id),
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
pub struct ProductionOverlayBlockStore {
    memory_cache: MemoryBlockStore,
    filesystem_cache: FilesystemBlockStore,
    azure_backing_store: AzureBlobBlockStore,
}

impl ProductionOverlayBlockStore {
    fn refill_memory_cache(&self, block: &Block) {
        let _ = self.memory_cache.put(block);
    }

    fn refill_all_caches(&self, block: &Block) {
        self.refill_memory_cache(block);
        let _ = self.filesystem_cache.put(block);
    }
}

impl BlockStore for ProductionOverlayBlockStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        put_with_overlay_cache_warm(
            &self.azure_backing_store,
            &self.memory_cache,
            &self.filesystem_cache,
            block,
        )
    }

    fn get(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        if let Some(block) = self.memory_cache.get(block_id)? {
            return Ok(Some(block));
        }

        if let Some(block) = self.filesystem_cache.get(block_id)? {
            self.refill_memory_cache(&block.block);
            return Ok(Some(block));
        }

        let Some(block) = self.azure_backing_store.get(block_id)? else {
            return Ok(None);
        };
        self.refill_all_caches(&block.block);
        Ok(Some(block))
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        let mut seen = HashSet::new();
        let cached_block_ids = collect_unique_block_ids(&self.memory_cache, &mut seen)?
            .into_iter()
            .chain(collect_unique_block_ids(&self.filesystem_cache, &mut seen)?)
            .collect::<Vec<_>>();
        let backing_iter = self.azure_backing_store.iter_block_ids()?;
        Ok(Box::new(OverlayBlockIdIterator {
            cached_block_ids: cached_block_ids.into_iter(),
            seen,
            backing_iter,
        }))
    }
}

fn collect_unique_block_ids<S: BlockStore>(
    store: &S,
    seen: &mut HashSet<BlockHash>,
) -> Result<Vec<BlockHash>, BlockStoreError> {
    let mut block_ids = Vec::new();
    for block_id in store.iter_block_ids()? {
        let block_id = block_id?;
        if seen.insert(block_id) {
            block_ids.push(block_id);
        }
    }
    Ok(block_ids)
}

struct OverlayBlockIdIterator<'a> {
    cached_block_ids: std::vec::IntoIter<BlockHash>,
    seen: HashSet<BlockHash>,
    backing_iter: BlockIdIterator<'a>,
}

impl Iterator for OverlayBlockIdIterator<'_> {
    type Item = Result<BlockHash, BlockStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(block_id) = self.cached_block_ids.next() {
            return Some(Ok(block_id));
        }
        loop {
            let next = self.backing_iter.next()?;
            match next {
                Ok(block_id) => {
                    if self.seen.insert(block_id) {
                        return Some(Ok(block_id));
                    }
                }
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

fn put_with_overlay_cache_warm<B, M, F>(
    backing_store: &B,
    memory_cache: &M,
    filesystem_cache: &F,
    block: &Block,
) -> Result<BlockHash, BlockStoreError>
where
    B: BlockStore,
    M: BlockStore,
    F: BlockStore,
{
    let block_id = backing_store.put(block)?;
    let _ = memory_cache.put(block);
    let _ = filesystem_cache.put(block);
    Ok(block_id)
}

#[cfg(test)]
mod tests {
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
    fn overlay_block_id_iterator_streams_backing_store_without_duplicate_ids() {
        let block_a = sample_block();
        let block_b = second_sample_block();
        let id_store = MemoryBlockStore::new(4).unwrap();
        let block_a_id = id_store.put(&block_a).unwrap();
        let block_b_id = id_store.put(&block_b).unwrap();
        let iterator = OverlayBlockIdIterator {
            cached_block_ids: vec![block_a_id].into_iter(),
            seen: HashSet::from([block_a_id]),
            backing_iter: Box::new(vec![Ok(block_a_id), Ok(block_b_id)].into_iter()),
        };

        let block_ids = iterator.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(block_ids, vec![block_a_id, block_b_id]);
    }

    #[test]
    fn put_warms_overlay_cache_layers_after_backing_store_write() {
        let dir = tempdir().unwrap();
        let block = sample_block();
        let backing_store = MemoryBlockStore::new(4).unwrap();
        let memory_cache = MemoryBlockStore::new(4).unwrap();
        let filesystem_cache = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let block_id =
            put_with_overlay_cache_warm(&backing_store, &memory_cache, &filesystem_cache, &block)
                .unwrap();

        assert!(memory_cache.get(&block_id).unwrap().is_some());
        assert!(filesystem_cache.get(&block_id).unwrap().is_some());
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

    fn second_sample_block() -> Block {
        Block::Leaf(LeafBlock {
            version: VERSION_1,
            level: 0,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            entries: vec![LeafEntry {
                embedding: vec![1, 2, 3, 4, 5, 6, 7, 8],
                metadata: vec![],
                content: Content {
                    media_type: "text/plain".into(),
                    body: b"second".to_vec(),
                },
            }],
            ext: None,
        })
    }
}
