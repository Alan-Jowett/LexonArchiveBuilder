use std::path::Path;

use lexongraph_block::{Block, BlockHash};
use lexongraph_block_store::{BlockIdIterator, BlockStore, BlockStoreError};
use lexongraph_block_store_fs::FilesystemBlockStore;

use crate::config::EnvironmentConfig;
use crate::paths::resolve_path;

#[derive(Clone, Debug)]
pub struct AzureBlobBlockStoreStub;

#[derive(Clone, Debug)]
pub enum ConfiguredBlockStore {
    Local(FilesystemBlockStore),
    AzureBlob(AzureBlobBlockStoreStub),
}

impl AzureBlobBlockStoreStub {
    fn not_implemented() -> BlockStoreError {
        BlockStoreError::BackendFailure(
            "Azure Blob block storage is not implemented in the first MVP".into(),
        )
    }
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
            EnvironmentConfig::Production { .. } => Ok(Self::AzureBlob(AzureBlobBlockStoreStub)),
        }
    }
}

impl BlockStore for AzureBlobBlockStoreStub {
    fn put(&self, _: &Block) -> Result<BlockHash, BlockStoreError> {
        Err(Self::not_implemented())
    }

    fn get(
        &self,
        _: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        Err(Self::not_implemented())
    }

    fn iter_block_ids(&self) -> Result<BlockIdIterator<'_>, BlockStoreError> {
        Err(Self::not_implemented())
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
        let store = ConfiguredBlockStore::AzureBlob(AzureBlobBlockStoreStub);
        let block = Block::Leaf(LeafBlock {
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
        });
        let error = store.put(&block).unwrap_err();

        assert!(matches!(error, BlockStoreError::BackendFailure(_)));
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
