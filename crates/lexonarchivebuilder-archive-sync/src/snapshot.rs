use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use lexongraph_block::{Block, BlockHash, Content, EmbeddingSpec, LeafBlock, LeafEntry, VERSION_1};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{WorkKind, WorkflowJournal, WorkflowJournalError, WorkflowStage};

const SNAPSHOT_MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum SourceSnapshotAcquisitionError {
    #[error("source URI must not be empty")]
    EmptySourceUri,
    #[error("acquisition timestamp must not be empty")]
    EmptyAcquiredAt,
    #[error(
        "workflow journal source URI `{journal_source_uri}` does not match requested source URI `{requested_source_uri}`"
    )]
    SourceUriMismatch {
        journal_source_uri: String,
        requested_source_uri: String,
    },
    #[error(
        "workflow journal has completed source acquisition but is missing corpus_manifest_identity"
    )]
    MissingCompletedManifestIdentity,
    #[error("workflow journal manifest block ID `{block_id}` is not a valid block hash")]
    InvalidManifestBlockId { block_id: String },
    #[error("workflow journal references missing manifest block `{block_id}`")]
    MissingManifestBlock { block_id: String },
    #[error("workflow journal manifest block `{block_id}` is not a leaf block")]
    ManifestBlockNotLeaf { block_id: String },
    #[error("workflow journal manifest block `{block_id}` is missing a content entry")]
    ManifestBlockMissingContent { block_id: String },
    #[error(
        "workflow journal manifest block `{block_id}` has unexpected media type `{media_type}`"
    )]
    ManifestBlockWrongMediaType {
        block_id: String,
        media_type: String,
    },
    #[error("failed to create snapshot root {path}: {source}")]
    CreateSnapshotRoot {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to enumerate snapshot root {path}: {source}")]
    EnumerateSnapshotRoot {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to read snapshot file {path}: {source}")]
    ReadSnapshotFile {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to store snapshot payload for {path}: {source}")]
    StoreSnapshotPayload {
        path: String,
        #[source]
        source: BlockStoreError,
    },
    #[error("failed to serialize source snapshot manifest: {source}")]
    SerializeManifest {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to parse source snapshot manifest block `{block_id}`: {source}")]
    ParseManifest {
        block_id: String,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "source snapshot manifest block `{block_id}` uses schema version {actual}, expected {expected}"
    )]
    UnsupportedManifestSchemaVersion {
        block_id: String,
        expected: u32,
        actual: u32,
    },
    #[error("failed to store source snapshot manifest: {source}")]
    StoreManifest {
        #[source]
        source: BlockStoreError,
    },
    #[error("failed to load source snapshot manifest block `{block_id}`: {source}")]
    LoadManifest {
        block_id: String,
        #[source]
        source: BlockStoreError,
    },
    #[error("failed to update workflow journal: {source}")]
    UpdateJournal {
        #[source]
        source: WorkflowJournalError,
    },
    #[error("rsync source acquisition failed: {detail}")]
    RsyncFailed { detail: String },
}

pub trait RsyncRunner {
    fn sync(
        &self,
        source_uri: &str,
        destination: &Path,
    ) -> Result<(), SourceSnapshotAcquisitionError>;
}

#[derive(Clone, Debug)]
pub struct CommandRsyncRunner {
    executable: PathBuf,
}

impl CommandRsyncRunner {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
        }
    }
}

impl Default for CommandRsyncRunner {
    fn default() -> Self {
        Self::new("rsync")
    }
}

impl RsyncRunner for CommandRsyncRunner {
    fn sync(
        &self,
        source_uri: &str,
        destination: &Path,
    ) -> Result<(), SourceSnapshotAcquisitionError> {
        let status = self
            .build_command(source_uri, destination)
            .status()
            .map_err(|error| SourceSnapshotAcquisitionError::RsyncFailed {
                detail: format!(
                    "failed to start {} for {} -> {}: {}",
                    self.executable.display(),
                    source_uri,
                    destination.display(),
                    error
                ),
            })?;

        if status.success() {
            Ok(())
        } else {
            Err(SourceSnapshotAcquisitionError::RsyncFailed {
                detail: format!(
                    "{} exited with status {} while syncing {} -> {}",
                    self.executable.display(),
                    status,
                    source_uri,
                    destination.display()
                ),
            })
        }
    }
}

impl CommandRsyncRunner {
    fn build_command(&self, source_uri: &str, destination: &Path) -> Command {
        let mut command = Command::new(&self.executable);
        command
            .arg("-a")
            .arg("--delete")
            .arg("--partial")
            .arg("--")
            .arg(source_uri)
            .arg(destination);
        command
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSnapshotManifestEntry {
    pub relative_path: String,
    pub block_id: String,
    pub byte_length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSnapshotManifest {
    pub schema_version: u32,
    pub source_uri: String,
    pub source_snapshot_id: String,
    pub acquired_at: String,
    pub entries: Vec<SourceSnapshotManifestEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcquiredSourceSnapshot {
    pub source_snapshot_id: String,
    pub manifest_block_id: String,
    pub manifest: Option<SourceSnapshotManifest>,
    pub reused_existing: bool,
}

pub fn acquire_source_snapshot<S: BlockStore, R: RsyncRunner>(
    store: &S,
    journal: &mut WorkflowJournal,
    source_uri: &str,
    snapshot_root: &Path,
    acquired_at: &str,
    runner: &R,
) -> Result<AcquiredSourceSnapshot, SourceSnapshotAcquisitionError> {
    let source_uri = source_uri.trim();
    if source_uri.is_empty() {
        return Err(SourceSnapshotAcquisitionError::EmptySourceUri);
    }
    let acquired_at = acquired_at.trim();
    if acquired_at.is_empty() {
        return Err(SourceSnapshotAcquisitionError::EmptyAcquiredAt);
    }
    if journal.source_snapshot.source_uri != source_uri {
        return Err(SourceSnapshotAcquisitionError::SourceUriMismatch {
            journal_source_uri: journal.source_snapshot.source_uri.clone(),
            requested_source_uri: source_uri.to_string(),
        });
    }

    if journal.source_snapshot.acquisition_completed_at.is_some() {
        let manifest_block_id = journal
            .source_snapshot
            .corpus_manifest_identity
            .clone()
            .ok_or(SourceSnapshotAcquisitionError::MissingCompletedManifestIdentity)?;
        let manifest = load_source_snapshot_manifest(store, &manifest_block_id)?;
        journal.source_snapshot.source_snapshot_id = manifest.source_snapshot_id.clone();
        if matches!(
            journal.current_stage,
            WorkflowStage::SourceAcquisition | WorkflowStage::MailboxAdmission
        ) {
            for entry in &manifest.entries {
                journal
                    .queue_work(WorkKind::MailboxAdmission, entry.relative_path.clone())
                    .map_err(|source| SourceSnapshotAcquisitionError::UpdateJournal { source })?;
            }
        }
        if journal.current_stage == WorkflowStage::SourceAcquisition {
            journal.set_stage(WorkflowStage::MailboxAdmission);
        }
        return Ok(AcquiredSourceSnapshot {
            source_snapshot_id: manifest.source_snapshot_id.clone(),
            manifest_block_id,
            manifest: Some(manifest),
            reused_existing: true,
        });
    }

    fs::create_dir_all(snapshot_root).map_err(|source| {
        SourceSnapshotAcquisitionError::CreateSnapshotRoot {
            path: snapshot_root.display().to_string(),
            source,
        }
    })?;

    journal.set_stage(WorkflowStage::SourceAcquisition);
    journal.source_snapshot.acquisition_started_at = Some(acquired_at.to_string());
    journal.source_snapshot.acquisition_completed_at = None;
    journal.source_snapshot.corpus_manifest_identity = None;
    journal.source_snapshot.additional_provenance.clear();

    runner.sync(source_uri, snapshot_root)?;

    let entries = collect_snapshot_entries(store, snapshot_root, snapshot_root)?;
    let source_snapshot_id = derive_source_snapshot_id(source_uri, &entries)
        .map_err(|source| SourceSnapshotAcquisitionError::SerializeManifest { source })?;
    let manifest = SourceSnapshotManifest {
        schema_version: SNAPSHOT_MANIFEST_SCHEMA_VERSION,
        source_uri: source_uri.to_string(),
        source_snapshot_id: source_snapshot_id.clone(),
        acquired_at: acquired_at.to_string(),
        entries,
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|source| SourceSnapshotAcquisitionError::SerializeManifest { source })?;
    let manifest_block_id = store
        .put(&snapshot_block("application/json", manifest_bytes))
        .map_err(|source| SourceSnapshotAcquisitionError::StoreManifest { source })?
        .to_string();

    journal.source_snapshot.source_snapshot_id = source_snapshot_id.clone();
    journal.source_snapshot.acquisition_completed_at = Some(acquired_at.to_string());
    journal.source_snapshot.corpus_manifest_identity = Some(manifest_block_id.clone());
    journal
        .source_snapshot
        .additional_provenance
        .insert("storage_seam".into(), "block-store".into());
    journal
        .source_snapshot
        .additional_provenance
        .insert("manifest_block_id".into(), manifest_block_id.clone());
    journal
        .source_snapshot
        .additional_provenance
        .insert("entry_count".into(), manifest.entries.len().to_string());
    for entry in &manifest.entries {
        journal
            .queue_work(WorkKind::MailboxAdmission, entry.relative_path.clone())
            .map_err(|source| SourceSnapshotAcquisitionError::UpdateJournal { source })?;
    }
    journal.set_stage(WorkflowStage::MailboxAdmission);

    Ok(AcquiredSourceSnapshot {
        source_snapshot_id,
        manifest_block_id,
        manifest: Some(manifest),
        reused_existing: false,
    })
}

fn collect_snapshot_entries<S: BlockStore>(
    store: &S,
    snapshot_root: &Path,
    current: &Path,
) -> Result<Vec<SourceSnapshotManifestEntry>, SourceSnapshotAcquisitionError> {
    let mut entries = Vec::new();
    let directory_entries = fs::read_dir(current).map_err(|source| {
        SourceSnapshotAcquisitionError::EnumerateSnapshotRoot {
            path: current.display().to_string(),
            source,
        }
    })?;
    let mut children = Vec::new();
    for child in directory_entries {
        let child =
            child.map_err(
                |source| SourceSnapshotAcquisitionError::EnumerateSnapshotRoot {
                    path: current.display().to_string(),
                    source,
                },
            )?;
        children.push(child.path());
    }
    children.sort();

    for child in children {
        let file_type = fs::symlink_metadata(&child)
            .map_err(
                |source| SourceSnapshotAcquisitionError::EnumerateSnapshotRoot {
                    path: child.display().to_string(),
                    source,
                },
            )?
            .file_type();
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            entries.extend(collect_snapshot_entries(store, snapshot_root, &child)?);
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let relative_path = normalize_relative_path(snapshot_root, &child);
        let bytes = fs::read(&child).map_err(|source| {
            SourceSnapshotAcquisitionError::ReadSnapshotFile {
                path: child.display().to_string(),
                source,
            }
        })?;
        let byte_length = bytes.len() as u64;
        let block_id = store
            .put(&snapshot_block("application/octet-stream", bytes))
            .map_err(
                |source| SourceSnapshotAcquisitionError::StoreSnapshotPayload {
                    path: child.display().to_string(),
                    source,
                },
            )?
            .to_string();
        entries.push(SourceSnapshotManifestEntry {
            relative_path,
            block_id,
            byte_length,
        });
    }

    Ok(entries)
}

fn derive_source_snapshot_id(
    source_uri: &str,
    entries: &[SourceSnapshotManifestEntry],
) -> Result<String, serde_json::Error> {
    #[derive(Serialize)]
    struct SnapshotIdentity<'a> {
        source_uri: &'a str,
        entries: &'a [SourceSnapshotManifestEntry],
    }

    let bytes = serde_json::to_vec(&SnapshotIdentity {
        source_uri,
        entries,
    })?;
    let digest = Sha256::digest(bytes);
    Ok(hex_encode(&digest))
}

fn load_source_snapshot_manifest<S: BlockStore>(
    store: &S,
    manifest_block_id: &str,
) -> Result<SourceSnapshotManifest, SourceSnapshotAcquisitionError> {
    let manifest_hash = parse_block_hash(manifest_block_id)?;
    let validated = store
        .get(&manifest_hash)
        .map_err(|source| SourceSnapshotAcquisitionError::LoadManifest {
            block_id: manifest_block_id.to_string(),
            source,
        })?
        .ok_or_else(|| SourceSnapshotAcquisitionError::MissingManifestBlock {
            block_id: manifest_block_id.to_string(),
        })?;
    let Block::Leaf(block) = validated.block else {
        return Err(SourceSnapshotAcquisitionError::ManifestBlockNotLeaf {
            block_id: manifest_block_id.to_string(),
        });
    };
    let entry = block.entries.first().ok_or_else(|| {
        SourceSnapshotAcquisitionError::ManifestBlockMissingContent {
            block_id: manifest_block_id.to_string(),
        }
    })?;
    if entry.content.media_type != "application/json" {
        return Err(
            SourceSnapshotAcquisitionError::ManifestBlockWrongMediaType {
                block_id: manifest_block_id.to_string(),
                media_type: entry.content.media_type.clone(),
            },
        );
    }
    let manifest: SourceSnapshotManifest =
        serde_json::from_slice(&entry.content.body).map_err(|source| {
            SourceSnapshotAcquisitionError::ParseManifest {
                block_id: manifest_block_id.to_string(),
                source,
            }
        })?;
    if manifest.schema_version != SNAPSHOT_MANIFEST_SCHEMA_VERSION {
        return Err(
            SourceSnapshotAcquisitionError::UnsupportedManifestSchemaVersion {
                block_id: manifest_block_id.to_string(),
                expected: SNAPSHOT_MANIFEST_SCHEMA_VERSION,
                actual: manifest.schema_version,
            },
        );
    }
    Ok(manifest)
}

fn snapshot_block(media_type: &str, body: Vec<u8>) -> Block {
    Block::Leaf(LeafBlock {
        version: VERSION_1,
        level: 0,
        embedding_spec: EmbeddingSpec {
            dims: 0,
            encoding: "f32le".into(),
        },
        entries: vec![LeafEntry {
            embedding: Vec::new(),
            metadata: Vec::new(),
            content: Content {
                media_type: media_type.into(),
                body,
            },
        }],
        ext: None,
    })
}

fn normalize_relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_block_hash(value: &str) -> Result<BlockHash, SourceSnapshotAcquisitionError> {
    if value.len() != BlockHash::LEN * 2 {
        return Err(SourceSnapshotAcquisitionError::InvalidManifestBlockId {
            block_id: value.to_string(),
        });
    }

    let mut bytes = [0u8; BlockHash::LEN];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(chunk[0]).ok_or_else(|| {
            SourceSnapshotAcquisitionError::InvalidManifestBlockId {
                block_id: value.to_string(),
            }
        })?;
        let low = decode_hex_nibble(chunk[1]).ok_or_else(|| {
            SourceSnapshotAcquisitionError::InvalidManifestBlockId {
                block_id: value.to_string(),
            }
        })?;
        bytes[index] = (high << 4) | low;
    }

    Ok(BlockHash::from_bytes(bytes))
}

fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use lexongraph_block_store_fs::FilesystemBlockStore;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        EffectiveIndexingConfigurationState, GenerationState, SourceSnapshotState,
        WorkflowJournalInit,
    };

    struct CopyTreeRsyncRunner {
        source_root: PathBuf,
        calls: Cell<usize>,
    }

    impl CopyTreeRsyncRunner {
        fn new(source_root: PathBuf) -> Self {
            Self {
                source_root,
                calls: Cell::new(0),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.get()
        }
    }

    impl RsyncRunner for CopyTreeRsyncRunner {
        fn sync(&self, _: &str, destination: &Path) -> Result<(), SourceSnapshotAcquisitionError> {
            self.calls.set(self.calls.get() + 1);
            copy_tree(&self.source_root, destination).map_err(|source| {
                SourceSnapshotAcquisitionError::CreateSnapshotRoot {
                    path: destination.display().to_string(),
                    source,
                }
            })
        }
    }

    struct FailingRsyncRunner;

    impl RsyncRunner for FailingRsyncRunner {
        fn sync(&self, _: &str, _: &Path) -> Result<(), SourceSnapshotAcquisitionError> {
            Err(SourceSnapshotAcquisitionError::RsyncFailed {
                detail: "simulated rsync failure".into(),
            })
        }
    }

    #[test]
    fn acquisition_persists_snapshot_payloads_and_manifest_and_updates_journal() {
        let temp = tempdir().unwrap();
        let source_root = temp.path().join("source");
        fs::create_dir_all(source_root.join("ietf")).unwrap();
        fs::write(source_root.join("ietf").join("2026-01.mbox"), b"mailbox-a").unwrap();
        fs::write(source_root.join("ietf").join("2026-02.mbox"), b"mailbox-b").unwrap();

        let store = FilesystemBlockStore::new(temp.path().join("blocks")).unwrap();
        let runner = CopyTreeRsyncRunner::new(source_root.clone());
        let mut journal = sample_journal();

        let snapshot = acquire_source_snapshot(
            &store,
            &mut journal,
            "rsync://example.invalid/mailman",
            &temp.path().join("mirror"),
            "2026-06-19T22:00:00Z",
            &runner,
        )
        .unwrap();

        assert!(!snapshot.reused_existing);
        let manifest = snapshot
            .manifest
            .expect("fresh acquisition should return manifest");
        assert_eq!(manifest.entries.len(), 2);
        assert_eq!(runner.call_count(), 1);
        assert_eq!(journal.current_stage, WorkflowStage::MailboxAdmission);
        assert_eq!(
            journal.source_snapshot.corpus_manifest_identity.as_deref(),
            Some(snapshot.manifest_block_id.as_str())
        );
        assert_eq!(
            journal
                .work
                .mailbox_admission
                .pending
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                "ietf/2026-01.mbox".to_string(),
                "ietf/2026-02.mbox".to_string()
            ]
        );
        assert!(
            manifest
                .entries
                .iter()
                .all(|entry| !entry.block_id.is_empty() && entry.byte_length > 0)
        );
    }

    #[test]
    fn acquisition_is_idempotent_when_same_corpus_is_reacquired() {
        let temp = tempdir().unwrap();
        let source_root = temp.path().join("source");
        fs::create_dir_all(&source_root).unwrap();
        fs::write(source_root.join("2026-01.mbox"), b"mailbox-a").unwrap();

        let store = FilesystemBlockStore::new(temp.path().join("blocks")).unwrap();
        let runner = CopyTreeRsyncRunner::new(source_root.clone());

        let mut first_journal = sample_journal();
        let first = acquire_source_snapshot(
            &store,
            &mut first_journal,
            "rsync://example.invalid/mailman",
            &temp.path().join("mirror-a"),
            "2026-06-19T22:00:00Z",
            &runner,
        )
        .unwrap();

        let mut second_journal = sample_journal();
        let second = acquire_source_snapshot(
            &store,
            &mut second_journal,
            "rsync://example.invalid/mailman",
            &temp.path().join("mirror-b"),
            "2026-06-19T22:15:00Z",
            &runner,
        )
        .unwrap();

        assert_eq!(first.source_snapshot_id, second.source_snapshot_id);
    }

    #[test]
    fn completed_source_acquisition_is_reused_without_running_rsync_again() {
        let temp = tempdir().unwrap();
        let source_root = temp.path().join("source");
        fs::create_dir_all(source_root.join("ietf")).unwrap();
        fs::write(source_root.join("ietf").join("2026-01.mbox"), b"mailbox-a").unwrap();
        let store = FilesystemBlockStore::new(temp.path().join("blocks")).unwrap();
        let runner = CopyTreeRsyncRunner::new(source_root.clone());
        let mut first_journal = sample_journal();
        let first_snapshot = acquire_source_snapshot(
            &store,
            &mut first_journal,
            "rsync://example.invalid/mailman",
            &temp.path().join("mirror-a"),
            "2026-06-19T22:00:00Z",
            &runner,
        )
        .unwrap();
        let mut journal = sample_journal();
        journal.current_stage = WorkflowStage::MailboxAdmission;
        journal.source_snapshot.source_snapshot_id = first_snapshot.source_snapshot_id.clone();
        journal.source_snapshot.acquisition_completed_at = Some("2026-06-19T22:00:00Z".into());
        journal.source_snapshot.corpus_manifest_identity =
            Some(first_snapshot.manifest_block_id.clone());

        let snapshot = acquire_source_snapshot(
            &store,
            &mut journal,
            "rsync://example.invalid/mailman",
            &temp.path().join("mirror"),
            "2026-06-19T22:15:00Z",
            &runner,
        )
        .unwrap();

        assert!(snapshot.reused_existing);
        assert_eq!(
            snapshot
                .manifest
                .as_ref()
                .map(|manifest| manifest.source_snapshot_id.as_str()),
            Some(first_snapshot.source_snapshot_id.as_str())
        );
        assert_eq!(runner.call_count(), 1);
    }

    #[test]
    fn completed_manifest_reuse_recovers_mailbox_work_without_rerunning_rsync() {
        let temp = tempdir().unwrap();
        let source_root = temp.path().join("source");
        fs::create_dir_all(source_root.join("ietf")).unwrap();
        fs::write(source_root.join("ietf").join("2026-01.mbox"), b"mailbox-a").unwrap();

        let store = FilesystemBlockStore::new(temp.path().join("blocks")).unwrap();
        let runner = CopyTreeRsyncRunner::new(source_root.clone());
        let mut first_journal = sample_journal();
        let first_snapshot = acquire_source_snapshot(
            &store,
            &mut first_journal,
            "rsync://example.invalid/mailman",
            &temp.path().join("mirror-a"),
            "2026-06-19T22:00:00Z",
            &runner,
        )
        .unwrap();

        let mut resumed_journal = sample_journal();
        resumed_journal.source_snapshot.source_snapshot_id = "stale-source-snapshot".into();
        resumed_journal.source_snapshot.acquisition_completed_at =
            Some("2026-06-19T22:00:00Z".into());
        resumed_journal.source_snapshot.corpus_manifest_identity =
            Some(first_snapshot.manifest_block_id.clone());

        let snapshot = acquire_source_snapshot(
            &store,
            &mut resumed_journal,
            "rsync://example.invalid/mailman",
            &temp.path().join("mirror-b"),
            "2026-06-19T22:15:00Z",
            &runner,
        )
        .unwrap();

        assert!(snapshot.reused_existing);
        assert_eq!(runner.call_count(), 1);
        assert_eq!(
            resumed_journal.current_stage,
            WorkflowStage::MailboxAdmission
        );
        assert_eq!(
            resumed_journal
                .work
                .mailbox_admission
                .pending
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["ietf/2026-01.mbox".to_string()]
        );
        assert_eq!(
            resumed_journal.source_snapshot.source_snapshot_id,
            first_snapshot.source_snapshot_id
        );
    }

    #[test]
    fn completed_manifest_reuse_after_mailbox_stage_keeps_journal_consistent() {
        let temp = tempdir().unwrap();
        let source_root = temp.path().join("source");
        fs::create_dir_all(source_root.join("ietf")).unwrap();
        fs::write(source_root.join("ietf").join("2026-01.mbox"), b"mailbox-a").unwrap();

        let store = FilesystemBlockStore::new(temp.path().join("blocks")).unwrap();
        let runner = CopyTreeRsyncRunner::new(source_root.clone());
        let mut first_journal = sample_journal();
        let first_snapshot = acquire_source_snapshot(
            &store,
            &mut first_journal,
            "rsync://example.invalid/mailman",
            &temp.path().join("mirror-a"),
            "2026-06-19T22:00:00Z",
            &runner,
        )
        .unwrap();

        let mut resumed_journal = sample_journal();
        resumed_journal.current_stage = WorkflowStage::ChunkDerivation;
        resumed_journal.source_snapshot.source_snapshot_id = "stale-source-snapshot".into();
        resumed_journal.source_snapshot.acquisition_completed_at =
            Some("2026-06-19T22:00:00Z".into());
        resumed_journal.source_snapshot.corpus_manifest_identity =
            Some(first_snapshot.manifest_block_id.clone());
        resumed_journal
            .work
            .mailbox_admission
            .completed
            .insert("ietf/2026-01.mbox".into());

        let snapshot = acquire_source_snapshot(
            &store,
            &mut resumed_journal,
            "rsync://example.invalid/mailman",
            &temp.path().join("mirror-b"),
            "2026-06-19T22:15:00Z",
            &runner,
        )
        .unwrap();

        assert!(snapshot.reused_existing);
        assert_eq!(runner.call_count(), 1);
        assert_eq!(
            resumed_journal.current_stage,
            WorkflowStage::ChunkDerivation
        );
        assert!(resumed_journal.work.mailbox_admission.pending.is_empty());
        assert_eq!(
            resumed_journal.source_snapshot.source_snapshot_id,
            first_snapshot.source_snapshot_id
        );
        resumed_journal.validate().unwrap();
    }

    #[test]
    fn fresh_acquisition_replaces_stale_provenance_keys() {
        let temp = tempdir().unwrap();
        let source_root = temp.path().join("source");
        fs::create_dir_all(source_root.join("ietf")).unwrap();
        fs::write(source_root.join("ietf").join("2026-01.mbox"), b"mailbox-a").unwrap();

        let store = FilesystemBlockStore::new(temp.path().join("blocks")).unwrap();
        let runner = CopyTreeRsyncRunner::new(source_root);
        let mut journal = sample_journal();
        journal
            .source_snapshot
            .additional_provenance
            .insert("stale".into(), "value".into());

        acquire_source_snapshot(
            &store,
            &mut journal,
            "rsync://example.invalid/mailman",
            &temp.path().join("mirror"),
            "2026-06-19T22:00:00Z",
            &runner,
        )
        .unwrap();

        assert!(
            !journal
                .source_snapshot
                .additional_provenance
                .contains_key("stale")
        );
        assert!(
            !journal
                .source_snapshot
                .additional_provenance
                .contains_key("snapshot_root")
        );
    }

    #[test]
    fn acquisition_failure_leaves_journal_in_source_acquisition_stage() {
        let temp = tempdir().unwrap();
        let store = FilesystemBlockStore::new(temp.path().join("blocks")).unwrap();
        let mut journal = sample_journal();

        let error = acquire_source_snapshot(
            &store,
            &mut journal,
            "rsync://example.invalid/mailman",
            &temp.path().join("mirror"),
            "2026-06-19T22:00:00Z",
            &FailingRsyncRunner,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            SourceSnapshotAcquisitionError::RsyncFailed { .. }
        ));
        assert_eq!(journal.current_stage, WorkflowStage::SourceAcquisition);
        assert_eq!(journal.source_snapshot.acquisition_completed_at, None);
    }

    #[test]
    fn command_rsync_runner_adds_partial_resume_flag() {
        let runner = CommandRsyncRunner::new("rsync");
        let command = runner.build_command(
            "rsync://example.invalid/mailman",
            Path::new("C:\\snapshot-root"),
        );
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(
            args,
            vec![
                "-a".to_string(),
                "--delete".to_string(),
                "--partial".to_string(),
                "--".to_string(),
                "rsync://example.invalid/mailman".to_string(),
                "C:\\snapshot-root".to_string(),
            ]
        );
    }

    #[test]
    fn snapshot_blocks_use_zero_dimensional_artifact_embeddings() {
        let Block::Leaf(block) = snapshot_block("application/json", b"{}".to_vec()) else {
            panic!("snapshot blocks should be leaves");
        };
        let entry = block
            .entries
            .first()
            .expect("snapshot block should have content");

        assert_eq!(block.embedding_spec.dims, 0);
        assert_eq!(entry.embedding, Vec::<u8>::new());
    }

    #[test]
    fn manifest_loader_rejects_unsupported_schema_versions() {
        let temp = tempdir().unwrap();
        let store = FilesystemBlockStore::new(temp.path().join("blocks")).unwrap();
        let manifest_block_id = store
            .put(&snapshot_block(
                "application/json",
                serde_json::to_vec(&SourceSnapshotManifest {
                    schema_version: SNAPSHOT_MANIFEST_SCHEMA_VERSION + 1,
                    source_uri: "rsync://example.invalid/mailman".into(),
                    source_snapshot_id: "snapshot-123".into(),
                    acquired_at: "2026-06-19T22:00:00Z".into(),
                    entries: Vec::new(),
                })
                .unwrap(),
            ))
            .unwrap()
            .to_string();

        let error = load_source_snapshot_manifest(&store, &manifest_block_id).unwrap_err();

        assert!(matches!(
            error,
            SourceSnapshotAcquisitionError::UnsupportedManifestSchemaVersion { .. }
        ));
    }

    fn sample_journal() -> WorkflowJournal {
        WorkflowJournal::new(WorkflowJournalInit {
            current_stage: WorkflowStage::SourceAcquisition,
            generation: GenerationState {
                generation_id: "generation-123".into(),
                journal_id: "journal-123".into(),
                started_at: "2026-06-19T21:55:00Z".into(),
                completed_at: None,
            },
            source_snapshot: SourceSnapshotState {
                source_snapshot_id: "pending-source-snapshot".into(),
                source_uri: "rsync://example.invalid/mailman".into(),
                acquisition_started_at: None,
                acquisition_completed_at: None,
                corpus_manifest_identity: None,
                additional_provenance: Default::default(),
            },
            effective_indexing_configuration: EffectiveIndexingConfigurationState {
                effective_indexing_configuration_id: "indexing-config-123".into(),
                chunking_policy_id: "chunking-123".into(),
                embedding_provider_or_model_id: "embedding-123".into(),
                delegated_published_root_generation_id: "published-root-123".into(),
                additional_root_affecting_inputs: Default::default(),
            },
        })
        .unwrap()
    }

    fn copy_tree(source: &Path, destination: &Path) -> io::Result<()> {
        fs::create_dir_all(destination)?;
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let source_path = entry.path();
            let destination_path = destination.join(entry.file_name());
            if source_path.is_dir() {
                copy_tree(&source_path, &destination_path)?;
            } else {
                if let Some(parent) = destination_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::copy(source_path, destination_path)?;
            }
        }
        Ok(())
    }
}
