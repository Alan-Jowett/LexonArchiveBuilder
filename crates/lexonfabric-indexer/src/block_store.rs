use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use lexongraph_block::{Block, BlockHash, serialize_block};
use lexongraph_block_store::{BlockStore, BlockStoreError};

use crate::config::EnvironmentConfig;

#[derive(Clone, Debug)]
pub struct LocalFilesystemBlockStore {
    root: PathBuf,
}

#[derive(Clone, Debug)]
pub struct AzureBlobBlockStoreStub;

#[derive(Clone, Debug)]
pub enum ConfiguredBlockStore {
    Local(LocalFilesystemBlockStore),
    AzureBlob(AzureBlobBlockStoreStub),
}

impl LocalFilesystemBlockStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn path_for_hash(&self, hash: &BlockHash) -> PathBuf {
        self.root.join(hash.to_string())
    }
}

impl AzureBlobBlockStoreStub {
    fn not_implemented() -> BlockStoreError {
        BlockStoreError::BackendFailure(
            "Azure Blob block storage is not implemented in the first MVP".into(),
        )
    }
}

impl ConfiguredBlockStore {
    pub fn from_environment(request_dir: &Path, environment: &EnvironmentConfig) -> Self {
        match environment {
            EnvironmentConfig::Local {
                block_store_root, ..
            } => Self::Local(LocalFilesystemBlockStore::new(resolve_path(
                request_dir,
                block_store_root,
            ))),
            EnvironmentConfig::Production { .. } => Self::AzureBlob(AzureBlobBlockStoreStub),
        }
    }
}

impl BlockStore for LocalFilesystemBlockStore {
    fn put(&self, block: &Block) -> Result<BlockHash, BlockStoreError> {
        let serialized = serialize_block(block).map_err(BlockStoreError::ContractViolation)?;
        fs::create_dir_all(&self.root)
            .map_err(|error| BlockStoreError::BackendFailure(error.to_string()))?;
        let path = self.path_for_hash(&serialized.hash);
        if !path.exists() {
            self.write_block_atomically(&path, &serialized.bytes)?;
        }
        Ok(serialized.hash)
    }

    fn get(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<lexongraph_block::ValidatedBlock>, BlockStoreError> {
        let path = self.path_for_hash(block_id);
        if !path.exists() {
            return Ok(None);
        }

        let bytes =
            fs::read(&path).map_err(|error| BlockStoreError::BackendFailure(error.to_string()))?;
        lexongraph_block::deserialize_block(&bytes, block_id)
            .map(Some)
            .map_err(map_get_error)
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
}

impl LocalFilesystemBlockStore {
    fn write_block_atomically(&self, path: &Path, bytes: &[u8]) -> Result<(), BlockStoreError> {
        for attempt in 0..16u32 {
            let temp_path = path.with_extension(format!("tmp-{}-{attempt}", std::process::id()));
            let open_result = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp_path);

            let mut file = match open_result {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(BlockStoreError::BackendFailure(error.to_string()));
                }
            };

            if let Err(error) = file.write_all(bytes).and_then(|_| file.sync_all()) {
                let _ = fs::remove_file(&temp_path);
                return Err(BlockStoreError::BackendFailure(error.to_string()));
            }
            drop(file);

            if path.exists() {
                let _ = fs::remove_file(&temp_path);
                return Ok(());
            }

            match fs::rename(&temp_path, path) {
                Ok(()) => return Ok(()),
                Err(_error) if path.exists() => {
                    let _ = fs::remove_file(&temp_path);
                    return Ok(());
                }
                Err(error) => {
                    let _ = fs::remove_file(&temp_path);
                    return Err(BlockStoreError::BackendFailure(error.to_string()));
                }
            }
        }

        Err(BlockStoreError::BackendFailure(
            "failed to allocate a temporary path for atomic block writes".into(),
        ))
    }
}

fn map_get_error(error: lexongraph_block::BlockError) -> BlockStoreError {
    match error {
        lexongraph_block::BlockError::HashMismatch { expected, actual } => {
            BlockStoreError::IntegrityMismatch { expected, actual }
        }
        other => BlockStoreError::MalformedContent(other),
    }
}

fn resolve_path(request_dir: &Path, candidate: &Path) -> PathBuf {
    if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        request_dir.join(candidate)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use lexongraph_block::{Block, Content, EmbeddingSpec, LeafBlock, LeafEntry, VERSION_1};
    use lexongraph_block_store::conformance;
    use tempfile::tempdir;

    use super::*;

    struct LocalFilesystemHarness {
        root: PathBuf,
        counter: Rc<Cell<u32>>,
    }

    impl LocalFilesystemHarness {
        fn new(root: PathBuf) -> Self {
            Self {
                root,
                counter: Rc::new(Cell::new(0)),
            }
        }
    }

    impl conformance::BlockStoreFactory for LocalFilesystemHarness {
        type Store = LocalFilesystemBlockStore;

        fn fresh_store(&self) -> Self::Store {
            let next = self.counter.get();
            self.counter.set(next + 1);
            LocalFilesystemBlockStore::new(self.root.join(format!("store-{next}")))
        }
    }

    impl conformance::BlockStoreConformanceHarness for LocalFilesystemHarness {
        fn inject_raw_bytes(
            &self,
            store: &Self::Store,
            block_id: &BlockHash,
            bytes: &[u8],
        ) -> Result<(), String> {
            fs::create_dir_all(&store.root).map_err(|error| error.to_string())?;
            fs::write(store.path_for_hash(block_id), bytes).map_err(|error| error.to_string())?;
            Ok(())
        }
    }

    #[test]
    fn local_filesystem_store_passes_block_store_conformance() {
        let dir = tempdir().unwrap();
        let harness = LocalFilesystemHarness::new(dir.path().join("blocks"));

        conformance::run_full_suite(&harness).unwrap();
    }

    #[test]
    fn configured_production_store_returns_explicit_backend_failure() {
        let store = ConfiguredBlockStore::AzureBlob(AzureBlobBlockStoreStub);
        let block = Block::Leaf(LeafBlock {
            version: VERSION_1,
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
}
