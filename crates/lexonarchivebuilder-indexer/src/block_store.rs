use std::path::Path;

use lexongraph_block::{Block, BlockHash};
use lexongraph_block_store::{BlockIdIterator, BlockStore, BlockStoreError};
use lexongraph_block_store_azure::AzureBlobBlockStore;
use lexongraph_block_store_fs::FilesystemBlockStore;

use crate::config::{EnvironmentConfig, ProductionBlockStoreConfig};
use crate::paths::resolve_path;

#[derive(Clone, Debug)]
pub enum ConfiguredBlockStore {
    Local(FilesystemBlockStore),
    AzureBlob(AzureBlobBlockStore),
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
                Self::production_store(block_store).map(Self::AzureBlob)
            }
        }
    }

    fn production_store(
        config: &ProductionBlockStoreConfig,
    ) -> Result<AzureBlobBlockStore, BlockStoreError> {
        config
            .validate()
            .map_err(|error| BlockStoreError::BackendFailure(error.to_string()))?;
        AzureBlobBlockStore::new(&config.container_sas_url)
    }
}

impl BlockStore for ConfiguredBlockStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        match self {
            Self::Local(store) => store.put(block),
            Self::AzureBlob(store) => store.put(block),
        }
    }

    fn get(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        match self {
            Self::Local(store) => store.get(block_id),
            Self::AzureBlob(store) => store.get(block_id),
        }
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        match self {
            Self::Local(store) => store.iter_block_ids(),
            Self::AzureBlob(store) => store.iter_block_ids(),
        }
    }
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
    fn configured_production_store_accepts_container_sas_url_without_prefix() {
        let store = ConfiguredBlockStore::from_environment(
            Path::new("."),
            &EnvironmentConfig::Production {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.blob.core.windows.net/archive-sync?sig=test".into(),
                    prefix: None,
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

        assert!(matches!(store, ConfiguredBlockStore::AzureBlob(_)));
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
