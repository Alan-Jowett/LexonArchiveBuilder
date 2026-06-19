use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use thiserror::Error;

pub const WORKFLOW_JOURNAL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum WorkflowJournalError {
    #[error("workflow journal field `{field}` must not be empty")]
    EmptyField { field: &'static str },
    #[error("workflow journal schema version {actual} is not supported; expected {expected}")]
    UnsupportedSchemaVersion { expected: u32, actual: u32 },
    #[error("workflow journal file {path} could not be read: {source}")]
    ReadJournal {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("workflow journal file {path} could not be parsed: {source}")]
    ParseJournal {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("workflow journal file {path} could not be written: {source}")]
    WriteJournal {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("workflow journal file {path} could not be serialized: {source}")]
    SerializeJournal {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("workflow journal file {path} could not be persisted: {source}")]
    PersistJournal {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("workflow journal authority must remain workflow-owned")]
    InvalidWorkflowAuthority,
    #[error("subordinate journal `{implementation}` must not claim workflow authority")]
    InvalidSubordinateAuthority { implementation: String },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum JournalAuthority {
    #[default]
    Workflow,
    Subordinate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowStage {
    SourceAcquisition,
    MailboxAdmission,
    ChunkDerivation,
    Embedding,
    PublishedRootGeneration,
    RootHistoryPublication,
    TerminalSuccess,
    TerminalFailure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkKind {
    MailboxAdmission,
    Chunking,
    Embedding,
    Indexing,
    Publication,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationState {
    pub generation_id: String,
    pub journal_id: String,
    pub started_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSnapshotState {
    pub source_snapshot_id: String,
    pub source_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acquisition_started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acquisition_completed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub corpus_manifest_identity: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub additional_provenance: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveIndexingConfigurationState {
    pub effective_indexing_configuration_id: String,
    pub chunking_policy_id: String,
    pub embedding_provider_or_model_id: String,
    pub delegated_published_root_generation_id: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub additional_root_affecting_inputs: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkInventory {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub pending: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub completed: BTreeSet<String>,
}

impl WorkInventory {
    fn queue(&mut self, item_id: String) {
        if !self.completed.contains(&item_id) {
            self.pending.insert(item_id);
        }
    }

    fn checkpoint(&mut self, item_id: String) -> bool {
        self.pending.remove(&item_id);
        self.completed.insert(item_id)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkInventories {
    #[serde(default)]
    pub mailbox_admission: WorkInventory,
    #[serde(default)]
    pub chunking: WorkInventory,
    #[serde(default)]
    pub embedding: WorkInventory,
    #[serde(default)]
    pub indexing: WorkInventory,
    #[serde(default)]
    pub publication: WorkInventory,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_set_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub work_set_frozen: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_root_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_root_recorded_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_history_entry_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_history_recorded_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_change_explanation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub stage: WorkflowStage,
    pub work_kind: WorkKind,
    pub item_id: String,
    pub recorded_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubordinateJournalState {
    pub authority: JournalAuthority,
    pub implementation: String,
    pub location: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_observed_generation_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TerminalOutcomeKind {
    Success,
    NonRecoverableFailure,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalOutcome {
    pub kind: TerminalOutcomeKind,
    pub recorded_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowJournal {
    pub schema_version: u32,
    pub authority: JournalAuthority,
    pub current_stage: WorkflowStage,
    pub generation: GenerationState,
    pub source_snapshot: SourceSnapshotState,
    pub effective_indexing_configuration: EffectiveIndexingConfigurationState,
    #[serde(default)]
    pub work: WorkInventories,
    #[serde(default)]
    pub audit: AuditState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checkpoints: Vec<CheckpointRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subordinate_journals: Vec<SubordinateJournalState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_outcome: Option<TerminalOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowJournalInit {
    pub current_stage: WorkflowStage,
    pub generation: GenerationState,
    pub source_snapshot: SourceSnapshotState,
    pub effective_indexing_configuration: EffectiveIndexingConfigurationState,
}

impl WorkflowJournal {
    pub fn new(init: WorkflowJournalInit) -> Result<Self, WorkflowJournalError> {
        validate_generation_state(&init.generation)?;
        validate_source_snapshot_state(&init.source_snapshot)?;
        validate_effective_indexing_configuration(&init.effective_indexing_configuration)?;

        Ok(Self {
            schema_version: WORKFLOW_JOURNAL_SCHEMA_VERSION,
            authority: JournalAuthority::Workflow,
            current_stage: init.current_stage,
            generation: init.generation,
            source_snapshot: init.source_snapshot,
            effective_indexing_configuration: init.effective_indexing_configuration,
            work: WorkInventories::default(),
            audit: AuditState::default(),
            checkpoints: Vec::new(),
            subordinate_journals: Vec::new(),
            terminal_outcome: None,
        })
    }

    pub fn validate(&self) -> Result<(), WorkflowJournalError> {
        if self.schema_version != WORKFLOW_JOURNAL_SCHEMA_VERSION {
            return Err(WorkflowJournalError::UnsupportedSchemaVersion {
                expected: WORKFLOW_JOURNAL_SCHEMA_VERSION,
                actual: self.schema_version,
            });
        }
        if self.authority != JournalAuthority::Workflow {
            return Err(WorkflowJournalError::InvalidWorkflowAuthority);
        }
        validate_generation_state(&self.generation)?;
        validate_source_snapshot_state(&self.source_snapshot)?;
        validate_effective_indexing_configuration(&self.effective_indexing_configuration)?;
        for subordinate in &self.subordinate_journals {
            require_non_empty(
                "subordinate_journal.implementation",
                &subordinate.implementation,
            )?;
            require_non_empty("subordinate_journal.location", &subordinate.location)?;
            if subordinate.authority != JournalAuthority::Subordinate {
                return Err(WorkflowJournalError::InvalidSubordinateAuthority {
                    implementation: subordinate.implementation.clone(),
                });
            }
        }
        if let Some(work_set_id) = &self.audit.work_set_id {
            require_non_empty("audit.work_set_id", work_set_id)?;
        }
        if let Some(root_id) = &self.audit.published_root_id {
            require_non_empty("audit.published_root_id", root_id)?;
        }
        if let Some(entry_id) = &self.audit.root_history_entry_id {
            require_non_empty("audit.root_history_entry_id", entry_id)?;
        }
        Ok(())
    }

    pub fn set_stage(&mut self, stage: WorkflowStage) {
        self.current_stage = stage;
    }

    pub fn queue_work(
        &mut self,
        work_kind: WorkKind,
        item_id: impl Into<String>,
    ) -> Result<(), WorkflowJournalError> {
        let item_id = require_non_empty_owned("work.item_id", item_id.into())?;
        self.inventory_mut(work_kind).queue(item_id);
        Ok(())
    }

    pub fn record_checkpoint(
        &mut self,
        stage: WorkflowStage,
        work_kind: WorkKind,
        item_id: impl Into<String>,
        recorded_at: impl Into<String>,
    ) -> Result<bool, WorkflowJournalError> {
        let item_id = require_non_empty_owned("checkpoint.item_id", item_id.into())?;
        let recorded_at = require_non_empty_owned("checkpoint.recorded_at", recorded_at.into())?;
        let inserted = self.inventory_mut(work_kind).checkpoint(item_id.clone());
        if inserted {
            self.current_stage = stage;
            self.checkpoints.push(CheckpointRecord {
                stage,
                work_kind,
                item_id,
                recorded_at,
            });
        }
        Ok(inserted)
    }

    pub fn freeze_work_set(
        &mut self,
        work_set_id: impl Into<String>,
    ) -> Result<(), WorkflowJournalError> {
        self.audit.work_set_id = Some(require_non_empty_owned(
            "audit.work_set_id",
            work_set_id.into(),
        )?);
        self.audit.work_set_frozen = true;
        Ok(())
    }

    pub fn record_published_root(
        &mut self,
        root_id: impl Into<String>,
        recorded_at: impl Into<String>,
    ) -> Result<(), WorkflowJournalError> {
        self.audit.published_root_id = Some(require_non_empty_owned(
            "audit.published_root_id",
            root_id.into(),
        )?);
        self.audit.published_root_recorded_at = Some(require_non_empty_owned(
            "audit.published_root_recorded_at",
            recorded_at.into(),
        )?);
        Ok(())
    }

    pub fn record_root_history_entry(
        &mut self,
        entry_id: impl Into<String>,
        recorded_at: impl Into<String>,
    ) -> Result<(), WorkflowJournalError> {
        self.audit.root_history_entry_id = Some(require_non_empty_owned(
            "audit.root_history_entry_id",
            entry_id.into(),
        )?);
        self.audit.root_history_recorded_at = Some(require_non_empty_owned(
            "audit.root_history_recorded_at",
            recorded_at.into(),
        )?);
        Ok(())
    }

    pub fn record_root_change_explanation(
        &mut self,
        explanation: impl Into<String>,
    ) -> Result<(), WorkflowJournalError> {
        self.audit.root_change_explanation = Some(require_non_empty_owned(
            "audit.root_change_explanation",
            explanation.into(),
        )?);
        Ok(())
    }

    pub fn upsert_subordinate_journal(
        &mut self,
        implementation: impl Into<String>,
        location: impl Into<String>,
        last_observed_generation_id: Option<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<(), WorkflowJournalError> {
        let implementation =
            require_non_empty_owned("subordinate_journal.implementation", implementation.into())?;
        let location = require_non_empty_owned("subordinate_journal.location", location.into())?;
        if let Some(existing) = self
            .subordinate_journals
            .iter_mut()
            .find(|entry| entry.implementation == implementation && entry.location == location)
        {
            existing.authority = JournalAuthority::Subordinate;
            existing.last_observed_generation_id = last_observed_generation_id;
            existing.metadata = metadata;
            return Ok(());
        }
        self.subordinate_journals.push(SubordinateJournalState {
            authority: JournalAuthority::Subordinate,
            implementation,
            location,
            last_observed_generation_id,
            metadata,
        });
        Ok(())
    }

    pub fn record_terminal_success(
        &mut self,
        recorded_at: impl Into<String>,
    ) -> Result<(), WorkflowJournalError> {
        let recorded_at =
            require_non_empty_owned("terminal_outcome.recorded_at", recorded_at.into())?;
        self.current_stage = WorkflowStage::TerminalSuccess;
        self.generation.completed_at = Some(recorded_at.clone());
        self.terminal_outcome = Some(TerminalOutcome {
            kind: TerminalOutcomeKind::Success,
            recorded_at,
            detail: None,
        });
        Ok(())
    }

    pub fn record_terminal_failure(
        &mut self,
        recorded_at: impl Into<String>,
        detail: impl Into<String>,
    ) -> Result<(), WorkflowJournalError> {
        let recorded_at =
            require_non_empty_owned("terminal_outcome.recorded_at", recorded_at.into())?;
        let detail = require_non_empty_owned("terminal_outcome.detail", detail.into())?;
        self.current_stage = WorkflowStage::TerminalFailure;
        self.generation.completed_at = Some(recorded_at.clone());
        self.terminal_outcome = Some(TerminalOutcome {
            kind: TerminalOutcomeKind::NonRecoverableFailure,
            recorded_at,
            detail: Some(detail),
        });
        Ok(())
    }

    fn inventory_mut(&mut self, work_kind: WorkKind) -> &mut WorkInventory {
        match work_kind {
            WorkKind::MailboxAdmission => &mut self.work.mailbox_admission,
            WorkKind::Chunking => &mut self.work.chunking,
            WorkKind::Embedding => &mut self.work.embedding,
            WorkKind::Indexing => &mut self.work.indexing,
            WorkKind::Publication => &mut self.work.publication,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorkflowJournalStore {
    path: PathBuf,
}

impl WorkflowJournalStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<Option<WorkflowJournal>, WorkflowJournalError> {
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&self.path).map_err(|source| WorkflowJournalError::ReadJournal {
            path: self.path.display().to_string(),
            source,
        })?;
        let journal: WorkflowJournal = serde_json::from_slice(&bytes).map_err(|source| {
            WorkflowJournalError::ParseJournal {
                path: self.path.display().to_string(),
                source,
            }
        })?;
        journal.validate()?;
        Ok(Some(journal))
    }

    pub fn save(&self, journal: &WorkflowJournal) -> Result<(), WorkflowJournalError> {
        journal.validate()?;
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|source| WorkflowJournalError::WriteJournal {
            path: self.path.display().to_string(),
            source,
        })?;

        let mut temp =
            NamedTempFile::new_in(parent).map_err(|source| WorkflowJournalError::WriteJournal {
                path: self.path.display().to_string(),
                source,
            })?;
        serde_json::to_writer_pretty(temp.as_file_mut(), journal).map_err(|source| {
            WorkflowJournalError::SerializeJournal {
                path: self.path.display().to_string(),
                source,
            }
        })?;
        use std::io::Write as _;
        temp.as_file_mut().write_all(b"\n").map_err(|source| {
            WorkflowJournalError::WriteJournal {
                path: self.path.display().to_string(),
                source,
            }
        })?;
        temp.as_file_mut()
            .sync_all()
            .map_err(|source| WorkflowJournalError::WriteJournal {
                path: self.path.display().to_string(),
                source,
            })?;
        temp.persist(&self.path)
            .map_err(|source| WorkflowJournalError::PersistJournal {
                path: self.path.display().to_string(),
                source: source.error,
            })?;
        sync_parent_directory(parent, &self.path)?;
        Ok(())
    }
}

fn sync_parent_directory(parent: &Path, journal_path: &Path) -> Result<(), WorkflowJournalError> {
    #[cfg(unix)]
    {
        let directory =
            fs::File::open(parent).map_err(|source| WorkflowJournalError::WriteJournal {
                path: journal_path.display().to_string(),
                source,
            })?;
        directory
            .sync_all()
            .map_err(|source| WorkflowJournalError::WriteJournal {
                path: journal_path.display().to_string(),
                source,
            })?;
    }

    #[cfg(not(unix))]
    {
        let _ = parent;
        let _ = journal_path;
    }

    Ok(())
}

fn validate_generation_state(generation: &GenerationState) -> Result<(), WorkflowJournalError> {
    require_non_empty("generation.generation_id", &generation.generation_id)?;
    require_non_empty("generation.journal_id", &generation.journal_id)?;
    require_non_empty("generation.started_at", &generation.started_at)?;
    if let Some(completed_at) = &generation.completed_at {
        require_non_empty("generation.completed_at", completed_at)?;
    }
    Ok(())
}

fn validate_source_snapshot_state(
    source_snapshot: &SourceSnapshotState,
) -> Result<(), WorkflowJournalError> {
    require_non_empty(
        "source_snapshot.source_snapshot_id",
        &source_snapshot.source_snapshot_id,
    )?;
    require_non_empty("source_snapshot.source_uri", &source_snapshot.source_uri)?;
    if let Some(started_at) = &source_snapshot.acquisition_started_at {
        require_non_empty("source_snapshot.acquisition_started_at", started_at)?;
    }
    if let Some(completed_at) = &source_snapshot.acquisition_completed_at {
        require_non_empty("source_snapshot.acquisition_completed_at", completed_at)?;
    }
    if let Some(corpus_manifest_identity) = &source_snapshot.corpus_manifest_identity {
        require_non_empty(
            "source_snapshot.corpus_manifest_identity",
            corpus_manifest_identity,
        )?;
    }
    Ok(())
}

fn validate_effective_indexing_configuration(
    config: &EffectiveIndexingConfigurationState,
) -> Result<(), WorkflowJournalError> {
    require_non_empty(
        "effective_indexing_configuration.effective_indexing_configuration_id",
        &config.effective_indexing_configuration_id,
    )?;
    require_non_empty(
        "effective_indexing_configuration.chunking_policy_id",
        &config.chunking_policy_id,
    )?;
    require_non_empty(
        "effective_indexing_configuration.embedding_provider_or_model_id",
        &config.embedding_provider_or_model_id,
    )?;
    require_non_empty(
        "effective_indexing_configuration.delegated_published_root_generation_id",
        &config.delegated_published_root_generation_id,
    )?;
    Ok(())
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), WorkflowJournalError> {
    if value.trim().is_empty() {
        return Err(WorkflowJournalError::EmptyField { field });
    }
    Ok(())
}

fn require_non_empty_owned(
    field: &'static str,
    value: String,
) -> Result<String, WorkflowJournalError> {
    require_non_empty(field, &value)?;
    Ok(value)
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_generation_state() -> GenerationState {
        GenerationState {
            generation_id: "gen-2026-06-19-001".into(),
            journal_id: "journal-2026-06-19-001".into(),
            started_at: "2026-06-19T09:00:00Z".into(),
            completed_at: None,
        }
    }

    fn sample_source_snapshot_state() -> SourceSnapshotState {
        SourceSnapshotState {
            source_snapshot_id: "snapshot-abc123".into(),
            source_uri: "rsync.ietf.org::mailman-archive/".into(),
            acquisition_started_at: Some("2026-06-19T09:00:05Z".into()),
            acquisition_completed_at: Some("2026-06-19T09:02:00Z".into()),
            corpus_manifest_identity: Some("manifest-001".into()),
            additional_provenance: BTreeMap::from([("mirror-status".into(), "complete".into())]),
        }
    }

    fn sample_indexing_configuration_state() -> EffectiveIndexingConfigurationState {
        EffectiveIndexingConfigurationState {
            effective_indexing_configuration_id: "cfg-xyz789".into(),
            chunking_policy_id: "mailbox-chunk-v1".into(),
            embedding_provider_or_model_id: "azure-openai:text-embedding-3-large".into(),
            delegated_published_root_generation_id: "streaming-indexer-profile-v1".into(),
            additional_root_affecting_inputs: BTreeMap::from([(
                "block-size-target".into(),
                "65536".into(),
            )]),
        }
    }

    fn sample_journal() -> WorkflowJournal {
        WorkflowJournal::new(WorkflowJournalInit {
            current_stage: WorkflowStage::SourceAcquisition,
            generation: sample_generation_state(),
            source_snapshot: sample_source_snapshot_state(),
            effective_indexing_configuration: sample_indexing_configuration_state(),
        })
        .unwrap()
    }

    #[test]
    fn workflow_journal_round_trip_preserves_required_resume_and_audit_fields() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal
            .queue_work(WorkKind::MailboxAdmission, "mailbox:ietf-1")
            .unwrap();
        journal
            .queue_work(WorkKind::Embedding, "chunk:ietf-1:0")
            .unwrap();
        journal.freeze_work_set("work-set-001").unwrap();
        journal
            .record_published_root(
                "root-00000000000000000000000000000001",
                "2026-06-19T09:10:00Z",
            )
            .unwrap();
        journal
            .record_root_history_entry("entry-001", "2026-06-19T09:10:01Z")
            .unwrap();
        journal
            .upsert_subordinate_journal(
                "lexonarchivebuilder-indexer-replay",
                "C:\\data\\blocks.replay-journal",
                Some(journal.generation.generation_id.clone()),
                BTreeMap::from([("status".into(), "available".into())]),
            )
            .unwrap();

        store.save(&journal).unwrap();
        let loaded = store.load().unwrap().unwrap();

        assert_eq!(loaded.current_stage, WorkflowStage::SourceAcquisition);
        assert_eq!(loaded.source_snapshot.source_snapshot_id, "snapshot-abc123");
        assert_eq!(loaded.generation.generation_id, "gen-2026-06-19-001");
        assert_eq!(
            loaded
                .effective_indexing_configuration
                .effective_indexing_configuration_id,
            "cfg-xyz789"
        );
        assert!(
            loaded
                .work
                .mailbox_admission
                .pending
                .contains("mailbox:ietf-1")
        );
        assert!(loaded.work.embedding.pending.contains("chunk:ietf-1:0"));
        assert_eq!(loaded.audit.work_set_id.as_deref(), Some("work-set-001"));
        assert_eq!(
            loaded.audit.published_root_id.as_deref(),
            Some("root-00000000000000000000000000000001")
        );
        assert_eq!(loaded.subordinate_journals.len(), 1);
    }

    #[test]
    fn checkpointing_completed_work_is_idempotent() {
        let mut journal = sample_journal();
        journal
            .queue_work(WorkKind::MailboxAdmission, "mailbox:ietf-2")
            .unwrap();

        let inserted = journal
            .record_checkpoint(
                WorkflowStage::MailboxAdmission,
                WorkKind::MailboxAdmission,
                "mailbox:ietf-2",
                "2026-06-19T09:03:00Z",
            )
            .unwrap();
        let duplicate = journal
            .record_checkpoint(
                WorkflowStage::MailboxAdmission,
                WorkKind::MailboxAdmission,
                "mailbox:ietf-2",
                "2026-06-19T09:03:05Z",
            )
            .unwrap();
        journal
            .queue_work(WorkKind::MailboxAdmission, "mailbox:ietf-2")
            .unwrap();

        assert!(inserted);
        assert!(!duplicate);
        assert!(journal.work.mailbox_admission.pending.is_empty());
        assert_eq!(journal.work.mailbox_admission.completed.len(), 1);
        assert_eq!(journal.checkpoints.len(), 1);
    }

    #[test]
    fn terminal_outcomes_preserve_generation_identity() {
        let mut success = sample_journal();
        let mut failure = sample_journal();

        success
            .record_terminal_success("2026-06-19T09:20:00Z")
            .unwrap();
        failure
            .record_terminal_failure(
                "2026-06-19T09:20:30Z",
                "azure root-history append permanently failed",
            )
            .unwrap();

        assert_eq!(
            success.generation.generation_id,
            failure.generation.generation_id
        );
        assert_eq!(
            success.terminal_outcome.as_ref().unwrap().kind,
            TerminalOutcomeKind::Success
        );
        assert_eq!(
            failure.terminal_outcome.as_ref().unwrap().kind,
            TerminalOutcomeKind::NonRecoverableFailure
        );
    }

    #[test]
    fn subordinate_journal_state_remains_non_authoritative_after_round_trip() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("nested").join("journal.json"));
        let mut journal = sample_journal();
        journal
            .upsert_subordinate_journal(
                "lexonarchivebuilder-indexer-replay",
                "C:\\data\\blocks.replay-journal",
                Some("gen-2026-06-19-001".into()),
                BTreeMap::from([("segment-count".into(), "3".into())]),
            )
            .unwrap();

        store.save(&journal).unwrap();
        let loaded = store.load().unwrap().unwrap();

        assert_eq!(loaded.authority, JournalAuthority::Workflow);
        assert_eq!(
            loaded.subordinate_journals[0].authority,
            JournalAuthority::Subordinate
        );
        assert_eq!(
            loaded.subordinate_journals[0].implementation,
            "lexonarchivebuilder-indexer-replay"
        );
    }

    #[test]
    fn duplicate_checkpoint_does_not_regress_current_stage() {
        let mut journal = sample_journal();
        journal
            .queue_work(WorkKind::Embedding, "chunk:ietf-3:0")
            .unwrap();
        journal
            .record_checkpoint(
                WorkflowStage::Embedding,
                WorkKind::Embedding,
                "chunk:ietf-3:0",
                "2026-06-19T09:04:00Z",
            )
            .unwrap();
        journal.set_stage(WorkflowStage::PublishedRootGeneration);

        let inserted = journal
            .record_checkpoint(
                WorkflowStage::Embedding,
                WorkKind::Embedding,
                "chunk:ietf-3:0",
                "2026-06-19T09:04:05Z",
            )
            .unwrap();

        assert!(!inserted);
        assert_eq!(
            journal.current_stage,
            WorkflowStage::PublishedRootGeneration
        );
        assert_eq!(journal.checkpoints.len(), 1);
    }

    #[test]
    fn load_rejects_unsupported_schema_version() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.schema_version = WORKFLOW_JOURNAL_SCHEMA_VERSION + 1;
        store.save(&sample_journal()).unwrap();
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::UnsupportedSchemaVersion {
                expected: WORKFLOW_JOURNAL_SCHEMA_VERSION,
                actual
            } if actual == WORKFLOW_JOURNAL_SCHEMA_VERSION + 1
        ));
    }
}
