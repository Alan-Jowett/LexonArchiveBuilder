use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use lexongraph_block::{Block, Content, EmbeddingSpec, LeafBlock, LeafEntry, VERSION_1};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{WorkKind, WorkflowJournal, WorkflowJournalError, WorkflowStage};

const SNAPSHOT_MANIFEST_SCHEMA_VERSION: u32 = 1;
const SNAPSHOT_EMBEDDING_BYTES: [u8; 4] = [0, 0, 0, 0];

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
    #[error("failed to store source snapshot manifest: {source}")]
    StoreManifest {
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
        let status = Command::new(&self.executable)
            .arg("-a")
            .arg("--delete")
            .arg(source_uri)
            .arg(destination)
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

    if journal.current_stage != WorkflowStage::SourceAcquisition
        && journal.source_snapshot.acquisition_completed_at.is_some()
    {
        let manifest_block_id = journal
            .source_snapshot
            .corpus_manifest_identity
            .clone()
            .ok_or(SourceSnapshotAcquisitionError::MissingCompletedManifestIdentity)?;
        return Ok(AcquiredSourceSnapshot {
            source_snapshot_id: journal.source_snapshot.source_snapshot_id.clone(),
            manifest_block_id,
            manifest: None,
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
    journal
        .source_snapshot
        .additional_provenance
        .insert("snapshot_root".into(), snapshot_root.display().to_string());
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
        if child.is_dir() {
            entries.extend(collect_snapshot_entries(store, snapshot_root, &child)?);
            continue;
        }
        if !child.is_file() {
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

fn snapshot_block(media_type: &str, body: Vec<u8>) -> Block {
    Block::Leaf(LeafBlock {
        version: VERSION_1,
        level: 0,
        embedding_spec: EmbeddingSpec {
            dims: 1,
            encoding: "f32le".into(),
        },
        entries: vec![LeafEntry {
            embedding: SNAPSHOT_EMBEDDING_BYTES.to_vec(),
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
        let store = FilesystemBlockStore::new(temp.path().join("blocks")).unwrap();
        let runner = CopyTreeRsyncRunner::new(temp.path().join("source"));
        let mut journal = sample_journal();
        journal.current_stage = WorkflowStage::MailboxAdmission;
        journal.source_snapshot.source_snapshot_id = "snapshot-123".into();
        journal.source_snapshot.acquisition_completed_at = Some("2026-06-19T22:00:00Z".into());
        journal.source_snapshot.corpus_manifest_identity = Some("manifest-123".into());

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
        assert_eq!(snapshot.manifest, None);
        assert_eq!(runner.call_count(), 0);
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
