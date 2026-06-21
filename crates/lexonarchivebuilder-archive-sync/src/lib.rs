use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use thiserror::Error;

pub mod snapshot;

pub const WORKFLOW_JOURNAL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum WorkflowJournalError {
    #[error("workflow journal field `{field}` must not be empty")]
    EmptyField { field: &'static str },
    #[error("workflow journal field `{field}` is required")]
    MissingField { field: &'static str },
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
    #[error("workflow journal state is inconsistent: {detail}")]
    InconsistentState { detail: &'static str },
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

fn work_kind_pending_after_stage(current_stage: WorkflowStage, work_kind: WorkKind) -> bool {
    match work_kind {
        WorkKind::MailboxAdmission => matches!(
            current_stage,
            WorkflowStage::ChunkDerivation
                | WorkflowStage::Embedding
                | WorkflowStage::PublishedRootGeneration
                | WorkflowStage::RootHistoryPublication
                | WorkflowStage::TerminalSuccess
        ),
        WorkKind::Chunking => matches!(
            current_stage,
            WorkflowStage::Embedding
                | WorkflowStage::PublishedRootGeneration
                | WorkflowStage::RootHistoryPublication
                | WorkflowStage::TerminalSuccess
        ),
        WorkKind::Embedding => matches!(
            current_stage,
            WorkflowStage::PublishedRootGeneration
                | WorkflowStage::RootHistoryPublication
                | WorkflowStage::TerminalSuccess
        ),
        WorkKind::Indexing | WorkKind::Publication => false,
    }
}

fn workflow_stage_rank(stage: WorkflowStage) -> u8 {
    match stage {
        WorkflowStage::SourceAcquisition => 0,
        WorkflowStage::MailboxAdmission => 1,
        WorkflowStage::ChunkDerivation => 2,
        WorkflowStage::Embedding => 3,
        WorkflowStage::PublishedRootGeneration => 4,
        WorkflowStage::RootHistoryPublication => 5,
        WorkflowStage::TerminalSuccess | WorkflowStage::TerminalFailure => 6,
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

        let journal = Self {
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
        };
        journal.validate()?;
        Ok(journal)
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
        validate_work_inventory(
            "work.mailbox_admission.pending",
            "work.mailbox_admission.completed",
            &self.work.mailbox_admission,
        )?;
        validate_work_inventory(
            "work.chunking.pending",
            "work.chunking.completed",
            &self.work.chunking,
        )?;
        validate_work_inventory(
            "work.embedding.pending",
            "work.embedding.completed",
            &self.work.embedding,
        )?;
        validate_work_inventory(
            "work.indexing.pending",
            "work.indexing.completed",
            &self.work.indexing,
        )?;
        validate_work_inventory(
            "work.publication.pending",
            "work.publication.completed",
            &self.work.publication,
        )?;
        let mut subordinate_keys = BTreeSet::new();
        for subordinate in &self.subordinate_journals {
            require_non_empty(
                "subordinate_journal.implementation",
                &subordinate.implementation,
            )?;
            require_non_empty("subordinate_journal.location", &subordinate.location)?;
            require_optional_non_empty(
                "subordinate_journal.last_observed_generation_id",
                &subordinate.last_observed_generation_id,
            )?;
            if subordinate.authority != JournalAuthority::Subordinate {
                return Err(WorkflowJournalError::InvalidSubordinateAuthority {
                    implementation: subordinate.implementation.clone(),
                });
            }
            let key = (
                subordinate.implementation.as_str(),
                subordinate.location.as_str(),
            );
            if !subordinate_keys.insert(key) {
                return Err(WorkflowJournalError::InconsistentState {
                    detail: "subordinate_journals must not contain duplicate implementation/location pairs",
                });
            }
        }
        if self.audit.work_set_frozen && self.audit.work_set_id.is_none() {
            return Err(WorkflowJournalError::MissingField {
                field: "audit.work_set_id",
            });
        }
        require_optional_non_empty("audit.work_set_id", &self.audit.work_set_id)?;
        require_optional_non_empty("audit.published_root_id", &self.audit.published_root_id)?;
        require_optional_non_empty(
            "audit.published_root_recorded_at",
            &self.audit.published_root_recorded_at,
        )?;
        require_optional_non_empty(
            "audit.root_history_entry_id",
            &self.audit.root_history_entry_id,
        )?;
        require_optional_non_empty(
            "audit.root_history_recorded_at",
            &self.audit.root_history_recorded_at,
        )?;
        require_optional_non_empty(
            "audit.root_change_explanation",
            &self.audit.root_change_explanation,
        )?;
        require_paired_optional_fields(
            "audit.published_root_id",
            &self.audit.published_root_id,
            "audit.published_root_recorded_at",
            &self.audit.published_root_recorded_at,
        )?;
        require_paired_optional_fields(
            "audit.root_history_entry_id",
            &self.audit.root_history_entry_id,
            "audit.root_history_recorded_at",
            &self.audit.root_history_recorded_at,
        )?;
        let mut checkpoint_keys = BTreeSet::new();
        let mut most_advanced_checkpoint_stage_rank: Option<u8> = None;
        for checkpoint in &self.checkpoints {
            validate_checkpoint_record(checkpoint)?;
            let key = (
                work_kind_key(checkpoint.work_kind),
                checkpoint.item_id.as_str(),
            );
            if !checkpoint_keys.insert(key) {
                return Err(WorkflowJournalError::InconsistentState {
                    detail: "checkpoints must not contain duplicate work_kind/item_id pairs",
                });
            }
            let inventory = self.inventory(checkpoint.work_kind);
            if !inventory.completed.contains(&checkpoint.item_id) {
                return Err(WorkflowJournalError::InconsistentState {
                    detail: "checkpointed item must appear in the completed work inventory",
                });
            }
            if inventory.pending.contains(&checkpoint.item_id) {
                return Err(WorkflowJournalError::InconsistentState {
                    detail: "checkpointed item must not remain pending in the work inventory",
                });
            }
            let checkpoint_stage_rank = workflow_stage_rank(checkpoint.stage);
            most_advanced_checkpoint_stage_rank = Some(match most_advanced_checkpoint_stage_rank {
                Some(current) => current.max(checkpoint_stage_rank),
                None => checkpoint_stage_rank,
            });
        }
        if let Some(most_advanced_checkpoint_stage_rank) = most_advanced_checkpoint_stage_rank
            && workflow_stage_rank(self.current_stage) < most_advanced_checkpoint_stage_rank
        {
            return Err(WorkflowJournalError::InconsistentState {
                detail: "current_stage must not precede recorded checkpoint stages",
            });
        }
        validate_terminal_state(
            self.current_stage,
            &self.terminal_outcome,
            &self.generation.completed_at,
        )?;
        Self::validate_stage_ordered_state(self.current_stage, &self.work, &self.audit)?;
        Ok(())
    }

    fn validate_stage_ordered_state(
        current_stage: WorkflowStage,
        work: &WorkInventories,
        audit: &AuditState,
    ) -> Result<(), WorkflowJournalError> {
        if work_kind_pending_after_stage(current_stage, WorkKind::MailboxAdmission)
            && !work.mailbox_admission.pending.is_empty()
        {
            return Err(WorkflowJournalError::InconsistentState {
                detail: "later workflow stages require no pending mailbox admission work",
            });
        }
        if work_kind_pending_after_stage(current_stage, WorkKind::Chunking)
            && !work.chunking.pending.is_empty()
        {
            return Err(WorkflowJournalError::InconsistentState {
                detail: "later workflow stages require no pending chunking work",
            });
        }
        let publication_barrier_reached = matches!(
            current_stage,
            WorkflowStage::PublishedRootGeneration
                | WorkflowStage::RootHistoryPublication
                | WorkflowStage::TerminalSuccess
        );
        if publication_barrier_reached && !audit.work_set_frozen {
            return Err(WorkflowJournalError::InconsistentState {
                detail: "published-root generation and later success stages require a frozen work set",
            });
        }
        if publication_barrier_reached && !work.embedding.pending.is_empty() {
            return Err(WorkflowJournalError::InconsistentState {
                detail: "published-root generation and later success stages require no pending embedding work",
            });
        }
        if matches!(
            current_stage,
            WorkflowStage::RootHistoryPublication | WorkflowStage::TerminalSuccess
        ) && audit.published_root_id.is_none()
        {
            return Err(WorkflowJournalError::MissingField {
                field: "audit.published_root_id",
            });
        }
        if current_stage == WorkflowStage::TerminalSuccess && audit.root_history_entry_id.is_none()
        {
            return Err(WorkflowJournalError::MissingField {
                field: "audit.root_history_entry_id",
            });
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
        if matches!(
            stage,
            WorkflowStage::TerminalSuccess | WorkflowStage::TerminalFailure
        ) {
            return Err(WorkflowJournalError::InconsistentState {
                detail: "checkpoints must not record terminal stages",
            });
        }
        let item_id = require_non_empty_owned("checkpoint.item_id", item_id.into())?;
        let recorded_at = require_non_empty_owned("checkpoint.recorded_at", recorded_at.into())?;
        let inserted = self.inventory_mut(work_kind).checkpoint(item_id.clone());
        if inserted {
            if workflow_stage_rank(stage) > workflow_stage_rank(self.current_stage) {
                self.current_stage = stage;
            }
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
        if let Some(last_observed_generation_id) = &last_observed_generation_id {
            require_non_empty(
                "subordinate_journal.last_observed_generation_id",
                last_observed_generation_id,
            )?;
        }
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

    fn inventory(&self, work_kind: WorkKind) -> &WorkInventory {
        match work_kind {
            WorkKind::MailboxAdmission => &self.work.mailbox_admission,
            WorkKind::Chunking => &self.work.chunking,
            WorkKind::Embedding => &self.work.embedding,
            WorkKind::Indexing => &self.work.indexing,
            WorkKind::Publication => &self.work.publication,
        }
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
        if !journal_path_exists(&self.path)? {
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
        let parent = journal_parent_directory(&self.path);
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
        persist_overwriting_if_needed(temp, &self.path)?;
        sync_parent_directory(parent, &self.path)?;
        Ok(())
    }
}

fn journal_path_exists(path: &Path) -> Result<bool, WorkflowJournalError> {
    path.try_exists()
        .map_err(|source| WorkflowJournalError::ReadJournal {
            path: path.display().to_string(),
            source,
        })
}

fn journal_parent_directory(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

fn persist_overwriting_if_needed(
    temp: NamedTempFile,
    destination: &Path,
) -> Result<(), WorkflowJournalError> {
    #[cfg(windows)]
    {
        if journal_path_exists(destination)? {
            remove_existing_destination_file(destination)?;
        }
    }

    temp.persist(destination)
        .map_err(|source| WorkflowJournalError::PersistJournal {
            path: destination.display().to_string(),
            source: source.error,
        })?;
    Ok(())
}

#[cfg(windows)]
fn remove_existing_destination_file(destination: &Path) -> Result<(), WorkflowJournalError> {
    match fs::remove_file(destination) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(WorkflowJournalError::PersistJournal {
            path: destination.display().to_string(),
            source,
        }),
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

fn validate_checkpoint_record(checkpoint: &CheckpointRecord) -> Result<(), WorkflowJournalError> {
    if matches!(
        checkpoint.stage,
        WorkflowStage::TerminalSuccess | WorkflowStage::TerminalFailure
    ) {
        return Err(WorkflowJournalError::InconsistentState {
            detail: "checkpoint.stage must not be terminal",
        });
    }
    require_non_empty("checkpoint.item_id", &checkpoint.item_id)?;
    require_non_empty("checkpoint.recorded_at", &checkpoint.recorded_at)?;
    Ok(())
}

fn validate_work_inventory(
    pending_field: &'static str,
    completed_field: &'static str,
    inventory: &WorkInventory,
) -> Result<(), WorkflowJournalError> {
    for item_id in &inventory.pending {
        require_non_empty(pending_field, item_id)?;
        if inventory.completed.contains(item_id) {
            return Err(WorkflowJournalError::InconsistentState {
                detail: "work inventory item must not appear in both pending and completed",
            });
        }
    }
    for item_id in &inventory.completed {
        require_non_empty(completed_field, item_id)?;
    }
    Ok(())
}

fn work_kind_key(work_kind: WorkKind) -> &'static str {
    match work_kind {
        WorkKind::MailboxAdmission => "mailbox-admission",
        WorkKind::Chunking => "chunking",
        WorkKind::Embedding => "embedding",
        WorkKind::Indexing => "indexing",
        WorkKind::Publication => "publication",
    }
}

fn validate_terminal_outcome(
    terminal_outcome: &TerminalOutcome,
) -> Result<(), WorkflowJournalError> {
    require_non_empty(
        "terminal_outcome.recorded_at",
        &terminal_outcome.recorded_at,
    )?;
    require_optional_non_empty("terminal_outcome.detail", &terminal_outcome.detail)?;
    Ok(())
}

fn validate_terminal_state(
    current_stage: WorkflowStage,
    terminal_outcome: &Option<TerminalOutcome>,
    completed_at: &Option<String>,
) -> Result<(), WorkflowJournalError> {
    let stage_is_terminal = matches!(
        current_stage,
        WorkflowStage::TerminalSuccess | WorkflowStage::TerminalFailure
    );

    if stage_is_terminal && terminal_outcome.is_none() {
        return Err(WorkflowJournalError::MissingField {
            field: "terminal_outcome",
        });
    }
    if !stage_is_terminal && terminal_outcome.is_some() {
        return Err(WorkflowJournalError::InconsistentState {
            detail: "non-terminal stage must not carry a terminal outcome",
        });
    }
    if !stage_is_terminal && completed_at.is_some() {
        return Err(WorkflowJournalError::InconsistentState {
            detail: "non-terminal stage must not carry generation.completed_at",
        });
    }
    if (stage_is_terminal || terminal_outcome.is_some()) && completed_at.is_none() {
        return Err(WorkflowJournalError::MissingField {
            field: "generation.completed_at",
        });
    }
    if let Some(terminal_outcome) = terminal_outcome {
        validate_terminal_outcome(terminal_outcome)?;
        if let Some(completed_at) = completed_at
            && completed_at != &terminal_outcome.recorded_at
        {
            return Err(WorkflowJournalError::InconsistentState {
                detail: "generation.completed_at must match terminal_outcome.recorded_at",
            });
        }
        match (current_stage, terminal_outcome.kind) {
            (WorkflowStage::TerminalSuccess, TerminalOutcomeKind::Success) => {}
            (WorkflowStage::TerminalFailure, TerminalOutcomeKind::NonRecoverableFailure) => {}
            (WorkflowStage::TerminalSuccess, TerminalOutcomeKind::NonRecoverableFailure) => {
                return Err(WorkflowJournalError::InconsistentState {
                    detail: "terminal success stage requires a success terminal outcome",
                });
            }
            (WorkflowStage::TerminalFailure, TerminalOutcomeKind::Success) => {
                return Err(WorkflowJournalError::InconsistentState {
                    detail: "terminal failure stage requires a non-recoverable failure terminal outcome",
                });
            }
            (WorkflowStage::SourceAcquisition, _)
            | (WorkflowStage::MailboxAdmission, _)
            | (WorkflowStage::ChunkDerivation, _)
            | (WorkflowStage::Embedding, _)
            | (WorkflowStage::PublishedRootGeneration, _)
            | (WorkflowStage::RootHistoryPublication, _) => {
                return Err(WorkflowJournalError::InconsistentState {
                    detail: "non-terminal stage must not carry a terminal outcome",
                });
            }
        }
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

fn require_optional_non_empty(
    field: &'static str,
    value: &Option<String>,
) -> Result<(), WorkflowJournalError> {
    if let Some(value) = value {
        require_non_empty(field, value)?;
    }
    Ok(())
}

fn require_paired_optional_fields(
    left_field: &'static str,
    left_value: &Option<String>,
    right_field: &'static str,
    right_value: &Option<String>,
) -> Result<(), WorkflowJournalError> {
    match (left_value.is_some(), right_value.is_some()) {
        (true, false) => Err(WorkflowJournalError::MissingField { field: right_field }),
        (false, true) => Err(WorkflowJournalError::MissingField { field: left_field }),
        _ => Ok(()),
    }
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
    fn workflow_journal_new_rejects_terminal_stage_without_terminal_outcome() {
        let error = WorkflowJournal::new(WorkflowJournalInit {
            current_stage: WorkflowStage::TerminalSuccess,
            generation: sample_generation_state(),
            source_snapshot: sample_source_snapshot_state(),
            effective_indexing_configuration: sample_indexing_configuration_state(),
        })
        .unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::MissingField {
                field: "terminal_outcome"
            }
        ));
    }

    #[test]
    #[cfg(windows)]
    fn remove_existing_destination_file_tolerates_not_found() {
        let temp = tempdir().unwrap();
        let destination = temp.path().join("missing-journal.json");

        remove_existing_destination_file(&destination).unwrap();
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
    fn repeated_save_overwrites_existing_journal() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut first = sample_journal();
        first
            .queue_work(WorkKind::MailboxAdmission, "mailbox:ietf-1")
            .unwrap();
        store.save(&first).unwrap();

        let mut second = sample_journal();
        second.freeze_work_set("work-set-002").unwrap();
        second
            .record_published_root(
                "root-00000000000000000000000000000002",
                "2026-06-19T09:24:59Z",
            )
            .unwrap();
        second
            .record_root_history_entry("entry-002", "2026-06-19T09:24:59Z")
            .unwrap();
        second
            .record_terminal_success("2026-06-19T09:25:00Z")
            .unwrap();
        store.save(&second).unwrap();

        let loaded = store.load().unwrap().unwrap();

        assert!(
            !loaded
                .work
                .mailbox_admission
                .pending
                .contains("mailbox:ietf-1")
        );
        assert_eq!(
            loaded.terminal_outcome.as_ref().map(|outcome| outcome.kind),
            Some(TerminalOutcomeKind::Success)
        );
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
    fn record_checkpoint_rejects_terminal_stage() {
        let mut journal = sample_journal();
        journal
            .queue_work(WorkKind::Publication, "publication:ietf-1")
            .unwrap();

        let error = journal
            .record_checkpoint(
                WorkflowStage::TerminalSuccess,
                WorkKind::Publication,
                "publication:ietf-1",
                "2026-06-19T09:30:00Z",
            )
            .unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::InconsistentState {
                detail: "checkpoints must not record terminal stages"
            }
        ));
    }

    #[test]
    fn late_checkpoint_does_not_regress_current_stage() {
        let mut journal = sample_journal();
        journal.set_stage(WorkflowStage::PublishedRootGeneration);
        journal
            .queue_work(WorkKind::MailboxAdmission, "mailbox:ietf-9")
            .unwrap();

        let inserted = journal
            .record_checkpoint(
                WorkflowStage::MailboxAdmission,
                WorkKind::MailboxAdmission,
                "mailbox:ietf-9",
                "2026-06-19T09:06:00Z",
            )
            .unwrap();

        assert!(inserted);
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

    #[test]
    fn journal_parent_directory_uses_current_directory_for_bare_filename() {
        assert_eq!(
            journal_parent_directory(Path::new("journal.json")),
            Path::new(".")
        );
        assert_eq!(
            journal_parent_directory(&Path::new("nested").join("journal.json")),
            Path::new("nested")
        );
    }

    #[test]
    fn load_rejects_incomplete_frozen_work_set_state() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.audit.work_set_frozen = true;
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::MissingField {
                field: "audit.work_set_id"
            }
        ));
    }

    #[test]
    fn load_rejects_empty_optional_string_fields() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.audit.published_root_id = Some("root-1".into());
        journal.audit.published_root_recorded_at = Some(String::new());
        journal.subordinate_journals.push(SubordinateJournalState {
            authority: JournalAuthority::Subordinate,
            implementation: "lexonarchivebuilder-indexer-replay".into(),
            location: "C:\\data\\blocks.replay-journal".into(),
            last_observed_generation_id: Some(String::new()),
            metadata: BTreeMap::new(),
        });
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::EmptyField {
                field: "subordinate_journal.last_observed_generation_id"
            }
        ));
    }

    #[test]
    fn upsert_subordinate_journal_rejects_empty_generation_id() {
        let mut journal = sample_journal();

        let error = journal
            .upsert_subordinate_journal(
                "lexonarchivebuilder-indexer-replay",
                "C:\\data\\blocks.replay-journal",
                Some(String::new()),
                BTreeMap::new(),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::EmptyField {
                field: "subordinate_journal.last_observed_generation_id"
            }
        ));
    }

    #[test]
    fn load_rejects_invalid_checkpoint_and_terminal_outcome_fields() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.checkpoints.push(CheckpointRecord {
            stage: WorkflowStage::Embedding,
            work_kind: WorkKind::Embedding,
            item_id: String::new(),
            recorded_at: "2026-06-19T09:04:00Z".into(),
        });
        journal.terminal_outcome = Some(TerminalOutcome {
            kind: TerminalOutcomeKind::NonRecoverableFailure,
            recorded_at: "2026-06-19T09:30:00Z".into(),
            detail: Some(String::new()),
        });
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::EmptyField {
                field: "checkpoint.item_id"
            }
        ));
    }

    #[test]
    fn load_rejects_unpaired_published_root_audit_fields() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.audit.published_root_id = Some("root-1".into());
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::MissingField {
                field: "audit.published_root_recorded_at"
            }
        ));
    }

    #[test]
    fn load_rejects_unpaired_root_history_audit_fields() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.audit.root_history_recorded_at = Some("2026-06-19T09:10:01Z".into());
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::MissingField {
                field: "audit.root_history_entry_id"
            }
        ));
    }

    #[test]
    fn load_rejects_inconsistent_terminal_state() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.current_stage = WorkflowStage::TerminalSuccess;
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::MissingField {
                field: "terminal_outcome"
            }
        ));
    }

    #[test]
    fn load_rejects_terminal_outcome_on_non_terminal_stage() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.terminal_outcome = Some(TerminalOutcome {
            kind: TerminalOutcomeKind::Success,
            recorded_at: "2026-06-19T09:20:00Z".into(),
            detail: None,
        });
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::InconsistentState {
                detail: "non-terminal stage must not carry a terminal outcome"
            }
        ));
    }

    #[test]
    fn load_rejects_completed_at_on_non_terminal_stage_without_terminal_outcome() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.generation.completed_at = Some("2026-06-19T09:20:00Z".into());
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::InconsistentState { detail: _ }
        ));
    }

    #[test]
    fn load_rejects_regressed_current_stage_relative_to_checkpoint() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.current_stage = WorkflowStage::MailboxAdmission;
        journal
            .work
            .embedding
            .completed
            .insert("chunk:ietf-1:0".into());
        journal.checkpoints.push(CheckpointRecord {
            stage: WorkflowStage::Embedding,
            work_kind: WorkKind::Embedding,
            item_id: "chunk:ietf-1:0".into(),
            recorded_at: "2026-06-19T09:04:00Z".into(),
        });
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::InconsistentState {
                detail: "current_stage must not precede recorded checkpoint stages"
            }
        ));
    }

    #[test]
    fn load_rejects_duplicate_subordinate_journal_keys() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        let entry = SubordinateJournalState {
            authority: JournalAuthority::Subordinate,
            implementation: "lexonarchivebuilder-indexer-replay".into(),
            location: "C:\\data\\blocks.replay-journal".into(),
            last_observed_generation_id: Some("gen-2026-06-19-001".into()),
            metadata: BTreeMap::new(),
        };
        journal.subordinate_journals.push(entry.clone());
        journal.subordinate_journals.push(entry);
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::InconsistentState { detail: _ }
        ));
    }

    #[test]
    fn load_rejects_duplicate_checkpoint_identity() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.checkpoints.push(CheckpointRecord {
            stage: WorkflowStage::Embedding,
            work_kind: WorkKind::Embedding,
            item_id: "chunk:ietf-1:0".into(),
            recorded_at: "2026-06-19T09:04:00Z".into(),
        });
        journal.checkpoints.push(CheckpointRecord {
            stage: WorkflowStage::PublishedRootGeneration,
            work_kind: WorkKind::Embedding,
            item_id: "chunk:ietf-1:0".into(),
            recorded_at: "2026-06-19T09:05:00Z".into(),
        });
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::InconsistentState { detail: _ }
        ));
    }

    #[test]
    fn load_rejects_invalid_work_inventory_item_ids() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.work.embedding.pending.insert(String::new());
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::EmptyField {
                field: "work.embedding.pending"
            }
        ));
    }

    #[test]
    fn load_rejects_work_inventory_overlap() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal
            .work
            .embedding
            .pending
            .insert("chunk:ietf-11:0".into());
        journal
            .work
            .embedding
            .completed
            .insert("chunk:ietf-11:0".into());
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::InconsistentState {
                detail: "work inventory item must not appear in both pending and completed"
            }
        ));
    }

    #[test]
    fn load_rejects_checkpoint_missing_completed_inventory_entry() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.checkpoints.push(CheckpointRecord {
            stage: WorkflowStage::Embedding,
            work_kind: WorkKind::Embedding,
            item_id: "chunk:ietf-12:0".into(),
            recorded_at: "2026-06-19T09:04:00Z".into(),
        });
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::InconsistentState {
                detail: "checkpointed item must appear in the completed work inventory"
            }
        ));
    }

    #[test]
    fn load_rejects_mismatched_terminal_completion_timestamps() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.current_stage = WorkflowStage::TerminalSuccess;
        journal.generation.completed_at = Some("2026-06-19T09:20:00Z".into());
        journal.terminal_outcome = Some(TerminalOutcome {
            kind: TerminalOutcomeKind::Success,
            recorded_at: "2026-06-19T09:20:01Z".into(),
            detail: None,
        });
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::InconsistentState { detail: _ }
        ));
    }

    #[test]
    fn load_rejects_published_root_stage_with_pending_embeddings() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.current_stage = WorkflowStage::PublishedRootGeneration;
        journal.freeze_work_set("work-set-001").unwrap();
        journal
            .queue_work(WorkKind::Embedding, "chunk:ietf-1:0")
            .unwrap();
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::InconsistentState {
                detail: "published-root generation and later success stages require no pending embedding work"
            }
        ));
    }

    #[test]
    fn load_rejects_chunk_derivation_stage_with_pending_mailbox_admission() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.current_stage = WorkflowStage::ChunkDerivation;
        journal
            .queue_work(WorkKind::MailboxAdmission, "mailbox:ietf-7")
            .unwrap();
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::InconsistentState {
                detail: "later workflow stages require no pending mailbox admission work"
            }
        ));
    }

    #[test]
    fn load_rejects_embedding_stage_with_pending_chunking() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.current_stage = WorkflowStage::Embedding;
        journal
            .queue_work(WorkKind::Chunking, "mailbox:ietf-8")
            .unwrap();
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::InconsistentState {
                detail: "later workflow stages require no pending chunking work"
            }
        ));
    }

    #[test]
    fn load_rejects_terminal_success_without_published_root_audit() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.freeze_work_set("work-set-001").unwrap();
        journal
            .record_terminal_success("2026-06-19T09:20:00Z")
            .unwrap();
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::MissingField {
                field: "audit.published_root_id"
            }
        ));
    }

    #[test]
    fn load_rejects_terminal_success_without_root_history_audit() {
        let temp = tempdir().unwrap();
        let store = WorkflowJournalStore::new(temp.path().join("journal.json"));
        let mut journal = sample_journal();
        journal.freeze_work_set("work-set-001").unwrap();
        journal
            .record_published_root(
                "root-00000000000000000000000000000003",
                "2026-06-19T09:19:59Z",
            )
            .unwrap();
        journal
            .record_terminal_success("2026-06-19T09:20:00Z")
            .unwrap();
        let serialized = serde_json::to_string_pretty(&journal).unwrap();
        fs::write(store.path(), format!("{serialized}\n")).unwrap();

        let error = store.load().unwrap_err();

        assert!(matches!(
            error,
            WorkflowJournalError::MissingField {
                field: "audit.root_history_entry_id"
            }
        ));
    }
}
