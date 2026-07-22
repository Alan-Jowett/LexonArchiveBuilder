// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::future::Future;
use std::io::{self, BufReader, BufWriter, Cursor, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use azure_data_tables::clients::TableServiceClientBuilder;
use azure_data_tables::prelude::{Filter, TableClient, Top};
use azure_storage::StorageCredentials;
use ciborium::Value;
use futures::StreamExt;
use lexongraph_block::{
    Block, BlockError, BlockHash, DecodedBlock, EmbeddingSpec, LeafEntry, SerializedBlock,
    VERSION_1, VersionedBlock, build_leaf_block, deserialize_block, v2,
};
use lexongraph_block_store::{BlockStore, BlockStoreError, BlockStoreExt};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_streaming_indexer::{
    BuiltInPlanningDirection, ContentResolver, IndexItem, PlanningStage, PublishedIndexingProfile,
    PublishedPlanningStrategy, StreamingIndexerError, StreamingIndexingPhase, StreamingIndexingRun,
    StreamingIndexingRunV2, StreamingIndexingStatus, StreamingIndexingStatusObserver,
    StreamingIndexingStatusState, StreamingIndexingSuspectedStallReason,
    StreamingIndexingTrainerSubphase, StreamingV2PendingPartitionStatus,
    published_indexing_profile,
};
use reqwest::StatusCode;
use reqwest::Url;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::{Instant as TokioInstant, MissedTickBehavior, interval_at};

use crate::block_store::{
    ConfiguredBlockStore, block_on_block_store_future, block_on_future_factory,
};
#[cfg(test)]
use crate::config::MUTABLE_REF_ROOT_DIR;
use crate::config::{
    BatchItemConfig, BatchRequest, BatchSummary, ClusteringConfigOverrides, ConfigError,
    ConfiguredClustering, ExecutionStage, MutableRefStoreLocation, PUBLISHED_PROFILE_V0_7_0,
    metadata_to_text_map,
};
use crate::custom_blocks::{
    REPLAY_JOURNAL_BLOCK_TYPE, REPLAY_JOURNAL_MEDIA_TYPE, custom_block_payload,
};
use crate::embedding::{ConfiguredEmbeddingProvider, ConfiguredEmbeddingProviderError};
use crate::mailbox::{MailboxExpansionError, expand_mailbox_item_with_stats};
use crate::paths::resolve_path;
use crate::resolver::{
    ContentRef, LocalFilesystemContentResolver, LocalFilesystemContentResolverError,
    ReplayIdentity, normalize_document_identity_path,
};
use crate::tree_tools::{decode_embedding_values, parse_block_hash};

type ProgressReporter = Arc<dyn Fn(String) + Send + Sync + 'static>;

pub const INGESTION_ONLY_ROOT_ID_PLACEHOLDER: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
const PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const EXTERNALIZED_REPLAY_PREFETCH_FUTURE_BATCHES: usize = 2;
const REPLAY_JOURNAL_SCHEMA_VERSION: u64 = 1;
const REPLAY_JOURNAL_BLOCK_MAX_BYTES: usize = 64 * 1024 * 1024;
const REPLAY_ORDER_ENTRY_BYTES: usize = 64;
const REPLAY_ORDER_FLUSH_ENTRY_LIMIT: usize = 65_536;
const REPLAY_ORDER_MERGE_FAN_IN: usize = 64;
const AZURE_BLOB_API_VERSION: &str = "2023-11-03";
const MUTABLE_REF_STORE_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const MUTABLE_REF_STORE_HTTP_RETRY_ATTEMPTS: usize = 3;
const MUTABLE_REF_STORE_HTTP_RETRY_DELAY: Duration = Duration::from_millis(500);
const MUTABLE_REF_TABLE_SCHEMA_VERSION: i32 = 1;
const UNKNOWN_BLOCKED_ON_SUMMARY: &str = "unknown";
#[cfg(test)]
const TEST_REF_NAME: &str = "test-branch";

#[derive(Clone, Copy)]
struct RuntimeIo<'a> {
    mutable_ref_store: Option<&'a MutableRefStoreLocation>,
    mutable_ref_metadata: Option<&'a BTreeMap<String, String>>,
    planning_telemetry: Option<&'a PlanningTelemetryContext>,
    progress: &'a ProgressReporter,
}

#[derive(Debug, Default)]
struct StagedBlocks {
    block_ids: Vec<BlockHash>,
    blocks: Vec<SerializedBlock>,
}

#[derive(Debug, Default)]
struct ConstructedBlocks {
    block_ids: Vec<BlockHash>,
    blocks: Vec<SerializedBlock>,
}

#[derive(Clone, Debug)]
struct ReplayBatch {
    items: Vec<IndexItem<ContentRef>>,
    #[allow(dead_code)]
    audit_records: Vec<ReplayJournalRecord>,
    completion_message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum ReplayJournalRecord {
    ReplayInput {
        step_kind: ReplayJournalStepKind,
        block_id: String,
        metadata: Vec<(String, String)>,
        content_ref: ReplayJournalContentRef,
    },
    IndexingOutcome {
        step_kind: ReplayJournalStepKind,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        input_block_ids: Vec<String>,
        #[serde(default, skip_serializing_if = "usize_is_zero")]
        input_block_count: usize,
        generated_block_ids: Vec<String>,
        root_block_id: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ReplayJournalStepKind {
    Embedding,
    Indexing,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct MutableRefStoreState {
    current_root_block_id: Option<String>,
    replay_journal_head_block_id: Option<String>,
    metadata: Option<BTreeMap<String, String>>,
}

#[derive(Clone, Debug, Default)]
struct MutableRefStoreUpdate {
    current_root_block_id: Option<String>,
    replay_journal_head_block_id: Option<String>,
    metadata: Option<BTreeMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReplayJournalBlockBody {
    schema_version: u64,
    previous_block_id: Option<String>,
    entries: Vec<ReplayJournalRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum ReplayJournalContentRef {
    Document {
        path: String,
    },
    Inline {
        media_type: String,
        body: Vec<u8>,
    },
    EmailChunk {
        email_artifact_ref: String,
        chunk_index: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ClusteringFailureDiagnostics {
    stage: ExecutionStage,
    embedding_spec: ClusteringFailureEmbeddingSpec,
    block_size_target: usize,
    clustering: EffectiveClusteringDiagnostics,
    embedding_health: EmbeddingHealthDiagnostics,
    failing_subset: Option<FailingSubsetDiagnostics>,
    input_count: usize,
    inputs: Vec<ClusteringFailureInput>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ClusteringFailureEmbeddingSpec {
    dims: u64,
    encoding: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct EffectiveClusteringDiagnostics {
    profile_version: String,
    planning_algorithm_id: String,
    planning_direction: Option<String>,
    packing_strategy_id: Option<String>,
    hierarchy_strategy_id: String,
    summary_policy_id: String,
    cluster_count: Option<u32>,
    random_seed: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum ClusteringFailureInput {
    Document {
        logical_id: String,
        source_path: String,
    },
    Inline {
        logical_id: String,
        media_type: String,
    },
    EmailChunk {
        logical_id: String,
        email_artifact_ref: String,
        chunk_index: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct EmbeddingHealthDiagnostics {
    available_embedding_count: usize,
    missing_embedding_count: usize,
    embedding_lookup_error_count: usize,
    undecodable_embedding_count: usize,
    non_finite_embedding_count: usize,
    zero_vector_count: usize,
    repeated_embedding_count: usize,
    unique_embedding_count: usize,
    repeated_embedding_group_count: usize,
    max_repeated_embedding_occurrence: Option<usize>,
    min_l2_norm: Option<f64>,
    max_l2_norm: Option<f64>,
    mean_l2_norm: Option<f64>,
    non_zero_variance_dimension_count: Option<usize>,
    max_component_variance: Option<f64>,
    top_repeated_embedding_groups: Vec<RepeatedEmbeddingGroupDiagnostics>,
    embedding_lookup_error_sample: Vec<EmbeddingLookupErrorDiagnostics>,
    suspicious_input_sample: Vec<SuspiciousClusteringFailureInput>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct RepeatedEmbeddingGroupDiagnostics {
    embedding_fingerprint: String,
    occurrence_count: usize,
    sample_inputs: Vec<RepeatedEmbeddingSampleDiagnostics>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct RepeatedEmbeddingSampleDiagnostics {
    input: ClusteringFailureInput,
    content_fingerprint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct SuspiciousClusteringFailureInput {
    input: ClusteringFailureInput,
    reasons: Vec<String>,
    embedding_fingerprint: Option<String>,
    l2_norm: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct EmbeddingLookupErrorDiagnostics {
    input: ClusteringFailureInput,
    content_fingerprint: Option<String>,
    error: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct FailingSubsetDiagnostics {
    phase: FailingSubsetPhaseDiagnostics,
    provenance: FailingSubsetProvenance,
    basis: String,
    upstream_active_item_count: usize,
    upstream_completed_unit_count: usize,
    upstream_phase_total_unit_count: Option<usize>,
    repository_visible_subset: RepositoryVisibleSubsetDiagnostics,
    embedding_health: EmbeddingHealthDiagnostics,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "phase", rename_all = "kebab-case")]
enum FailingSubsetPhaseDiagnostics {
    PlanningPass { pass_number: usize },
    HierarchyPlanning { stage: String },
    FinalMaterializationReplay,
    BottomUpAssembly { layer_index: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum FailingSubsetProvenance {
    Exact,
    NarrowestProvable,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum RepositoryVisibleSubsetDiagnostics {
    SameAsTopLevelAttempt { input_count: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubmissionProgressKind {
    Embedding,
    Replay,
}

impl SubmissionProgressKind {
    fn started_message(
        self,
        batch_number: usize,
        total_batches: usize,
        batch_item_count: usize,
        completed_items: usize,
        total_items: usize,
    ) -> String {
        match self {
            Self::Embedding => format!(
                "Embedding batch {batch_number} of {total_batches} started for {batch_item_count} delegated item(s); completed {completed_items} of {total_items} delegated item(s)"
            ),
            Self::Replay => format!(
                "Submitting replay batch {batch_number} of {total_batches} for {batch_item_count} delegated item(s); completed {completed_items} of {total_items} delegated item(s)"
            ),
        }
    }

    fn heartbeat_message(
        self,
        batch_number: usize,
        total_batches: usize,
        batch_item_count: usize,
        completed_items: usize,
        total_items: usize,
        elapsed_ms: u128,
    ) -> String {
        match self {
            Self::Embedding => format!(
                "Embedding batch {batch_number} of {total_batches} still running after {elapsed_ms} ms for {batch_item_count} delegated item(s); completed {completed_items} of {total_items} delegated item(s)"
            ),
            Self::Replay => format!(
                "Submitting replay batch {batch_number} of {total_batches} still running after {elapsed_ms} ms for {batch_item_count} delegated item(s); completed {completed_items} of {total_items} delegated item(s)"
            ),
        }
    }

    fn completion_message(
        self,
        batch_number: usize,
        total_batches: usize,
        completed_items: usize,
        total_items: usize,
    ) -> String {
        match self {
            Self::Embedding => format!(
                "Embedded batch {batch_number} of {total_batches}; completed {completed_items} of {total_items} delegated item(s)"
            ),
            Self::Replay => format!(
                "Submitted replay batch {batch_number} of {total_batches}; completed {completed_items} of {total_items} delegated item(s)"
            ),
        }
    }

    fn handoff_message(self, total_batches: usize, total_items: usize) -> String {
        match self {
            Self::Embedding => format!(
                "Submitted all {total_batches} embedding batch(es); waiting for planning pass completion over {total_items} delegated item(s)"
            ),
            Self::Replay => format!(
                "Submitted all {total_batches} replay batch(es); waiting for planning pass completion over {total_items} delegated item(s)"
            ),
        }
    }
}

impl ReplayOrderEntry {
    fn new(block_id: BlockHash, digest: BlockHash) -> Self {
        Self {
            block_id: block_id.into_bytes(),
            digest: digest.into_bytes(),
        }
    }

    fn block_hash(self) -> BlockHash {
        BlockHash::from_bytes(self.block_id)
    }

    fn digest_hash(self) -> BlockHash {
        BlockHash::from_bytes(self.digest)
    }

    fn write_to(self, writer: &mut impl Write) -> io::Result<()> {
        let mut bytes = [0u8; REPLAY_ORDER_ENTRY_BYTES];
        bytes[..32].copy_from_slice(&self.block_id);
        bytes[32..].copy_from_slice(&self.digest);
        writer.write_all(&bytes)
    }

    fn read_from(reader: &mut impl Read) -> io::Result<Option<Self>> {
        let mut bytes = [0u8; REPLAY_ORDER_ENTRY_BYTES];
        if reader.read(&mut bytes[..1])? == 0 {
            return Ok(None);
        }
        reader.read_exact(&mut bytes[1..])?;
        let mut block_id = [0u8; 32];
        block_id.copy_from_slice(&bytes[..32]);
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&bytes[32..]);
        Ok(Some(Self { block_id, digest }))
    }
}

impl Ord for ReplayOrderEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.block_id
            .cmp(&other.block_id)
            .then_with(|| self.digest.cmp(&other.digest))
    }
}

impl PartialOrd for ReplayOrderEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReplayOrderCursor {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .entry
            .cmp(&self.entry)
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl PartialOrd for ReplayOrderCursor {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl ReplayOrderStorage {
    fn new(scratch_dir: tempfile::TempDir, entries_path: PathBuf, total_items: usize) -> Self {
        Self {
            inner: Arc::new(ReplayOrderStorageInner {
                _scratch_dir: scratch_dir,
                entries_path,
                total_items,
            }),
        }
    }

    fn total_items(&self) -> usize {
        self.inner.total_items
    }

    fn open_reader(&self) -> Result<ReplayOrderReader, RuntimeError> {
        let file = File::open(&self.inner.entries_path).map_err(|source| {
            RuntimeError::ReadReplayOrderScratch {
                path: self.inner.entries_path.display().to_string(),
                source,
            }
        })?;
        Ok(ReplayOrderReader {
            path: self.inner.entries_path.clone(),
            reader: BufReader::new(file),
            remaining_items: self.inner.total_items,
        })
    }

    fn read_all_entries(&self) -> Result<Vec<ReplayOrderEntry>, RuntimeError> {
        let mut reader = self.open_reader()?;
        reader.read_next_entries(self.total_items())
    }

    #[cfg(test)]
    fn replay_input_block_ids(&self) -> Result<Vec<String>, RuntimeError> {
        Ok(self
            .read_all_entries()?
            .into_iter()
            .map(|entry| entry.block_hash().to_string())
            .collect())
    }
}

impl ReplayOrderReader {
    fn read_next_entries(
        &mut self,
        max_items: usize,
    ) -> Result<Vec<ReplayOrderEntry>, RuntimeError> {
        let target = self.remaining_items.min(max_items);
        let mut entries = Vec::with_capacity(target);
        while entries.len() < target {
            let Some(entry) = ReplayOrderEntry::read_from(&mut self.reader).map_err(|source| {
                RuntimeError::ReadReplayOrderScratch {
                    path: self.path.display().to_string(),
                    source,
                }
            })?
            else {
                return Err(RuntimeError::ReadReplayOrderScratch {
                    path: self.path.display().to_string(),
                    source: io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "replay-order scratch file ended before all expected entries were read",
                    ),
                });
            };
            entries.push(entry);
        }
        self.remaining_items = self.remaining_items.saturating_sub(entries.len());
        Ok(entries)
    }
}

fn map_replay_order_runtime_error_to_provider_error(
    error: RuntimeError,
) -> StoredLeafEmbeddingProviderError {
    match error {
        RuntimeError::ReadReplayOrderScratch { path, source } => {
            StoredLeafEmbeddingProviderError::ReadReplayOrderScratch { path, source }
        }
        other => StoredLeafEmbeddingProviderError::InvalidStoredEmbeddingBlock {
            block_id: "<replay-order>".into(),
            message: other.to_string(),
        },
    }
}

#[derive(Clone, Debug)]
struct StreamingStageConfig {
    stage: ExecutionStage,
    clustering: ConfiguredClustering,
    block_size_target: usize,
    submission_progress_kind: SubmissionProgressKind,
    planner_state_root: Option<PathBuf>,
}

type ReplayedLeaf = (IndexItem<ContentRef>, Vec<u8>);
type EmbeddingCache = HashMap<[u8; 32], Vec<u8>>;

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct StoredLeafEmbeddingProvider {
    embeddings_by_input_hash: Arc<EmbeddingCache>,
}

#[derive(Debug)]
struct ExternalizedReplayState {
    replay_order: ReplayOrderStorage,
    total_items: usize,
    batch_size: usize,
    materialization_max_concurrency: usize,
    block_store: ConfiguredBlockStore,
    embedding_spec: EmbeddingSpec,
    current_batch_embeddings: Arc<Mutex<EmbeddingCache>>,
}

#[derive(Clone, Debug)]
struct ExternalizedStoredLeafEmbeddingProvider {
    block_store: ConfiguredBlockStore,
    embedding_spec: EmbeddingSpec,
    replay_order: ReplayOrderStorage,
    current_batch_embeddings: Arc<Mutex<EmbeddingCache>>,
    fallback_embeddings: Arc<Mutex<Option<EmbeddingCache>>>,
}

#[derive(Debug)]
struct ExternalizedReplayBatchIterator {
    replay_order_reader: ReplayOrderReader,
    batch_size: usize,
    materialization_max_concurrency: usize,
    block_store: ConfiguredBlockStore,
    embedding_spec: EmbeddingSpec,
    current_batch_embeddings: Arc<Mutex<EmbeddingCache>>,
}

#[derive(Debug)]
struct ExternalizedReplayFinalizeSource {
    inner: ExternalizedReplayBatchIterator,
}

#[derive(Debug)]
struct ExternalizedReplayFinalizeIterator {
    inner: ExternalizedReplayBatchIterator,
}

#[derive(Clone, Debug)]
struct ReplayOrderStorage {
    inner: Arc<ReplayOrderStorageInner>,
}

#[derive(Debug)]
struct ReplayOrderStorageInner {
    _scratch_dir: tempfile::TempDir,
    entries_path: PathBuf,
    total_items: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReplayOrderEntry {
    block_id: [u8; 32],
    digest: [u8; 32],
}

#[derive(Debug, Eq, PartialEq)]
struct ReplayOrderCursor {
    entry: ReplayOrderEntry,
    run_index: usize,
}

#[derive(Debug)]
struct ReplayOrderReader {
    path: PathBuf,
    reader: BufReader<File>,
    remaining_items: usize,
}

#[derive(Debug)]
struct ReplayBatchLoad {
    batch: ReplayBatch,
    embeddings_by_input_hash: Vec<([u8; 32], Vec<u8>)>,
}

#[derive(Debug)]
struct ReplayBatchEntryLoad {
    item: IndexItem<ContentRef>,
    audit_record: ReplayJournalRecord,
    input_hash: [u8; 32],
    embedding: Vec<u8>,
}

type ExternalizedReplayBatchPrefetchHandle =
    JoinHandle<Result<(ExternalizedReplayBatchIterator, VecDeque<ReplayBatchLoad>), RuntimeError>>;

#[derive(Clone, Copy, Debug)]
struct ValidateOnlyResolver;

#[derive(Clone, Copy, Debug)]
struct ValidateOnlyEmbeddingProvider;

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct RecordingEmbeddingProvider<EP> {
    inner: EP,
    embeddings_by_input_hash: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
}

#[derive(Debug, Error)]
enum StoredLeafEmbeddingProviderError {
    #[error("no stored embedding was available for the requested replay input")]
    MissingStoredEmbedding,
    #[error("failed to read stored embedding block {block_id}: {source}")]
    ReadStoredEmbeddingBlock {
        block_id: String,
        #[source]
        source: BlockStoreError,
    },
    #[error("stored embedding block {block_id} is invalid: {message}")]
    InvalidStoredEmbeddingBlock { block_id: String, message: String },
    #[error("failed to read replay-order scratch file {path}: {source}")]
    ReadReplayOrderScratch {
        path: String,
        #[source]
        source: io::Error,
    },
}

#[cfg(test)]
#[derive(Debug, Error)]
enum AutoSizingBuiltInPlanningError {
    #[error("{0}")]
    DeriveClusterCount(String),
}

trait ClusteringFailureEmbeddingSource {
    fn embedding_for_hash(&self, input_hash: &[u8; 32]) -> Result<Option<Vec<u8>>, String>;
}

impl StagedBlocks {
    fn extend_constructed(&mut self, constructed: &ConstructedBlocks) {
        self.block_ids.extend(constructed.block_ids.iter().copied());
        self.blocks.extend(constructed.blocks.iter().cloned());
    }

    fn into_summary(self, root_id: String) -> BatchSummary {
        let mut block_ids = self
            .block_ids
            .into_iter()
            .map(|block_id| block_id.to_string())
            .collect::<Vec<_>>();
        block_ids.sort();
        block_ids.dedup();
        BatchSummary {
            root_id,
            block_count: block_ids.len(),
            block_ids,
        }
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("failed to read request file {path}: {source}")]
    ReadRequest {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse request file {path}: {source}")]
    ParseRequest {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Provider(#[from] ConfiguredEmbeddingProviderError),
    #[error(transparent)]
    Mailbox(#[from] MailboxExpansionError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error("failed to deserialize staged block {block_id}: {source}")]
    DeserializeStagedBlock {
        block_id: String,
        #[source]
        source: BlockError,
    },
    #[error("failed to construct leaf block {block_id}: {source}")]
    ConstructLeafBlock {
        block_id: String,
        #[source]
        source: BlockError,
    },
    #[error("staged block hash mismatch: expected {expected}, store returned {actual}")]
    StagedBlockHashMismatch { expected: String, actual: String },
    #[error(transparent)]
    StreamingIndexer(#[from] StreamingIndexerError),
    #[error("{source}")]
    ClusteringFailure {
        #[source]
        source: StreamingIndexerError,
        diagnostics: Box<ClusteringFailureDiagnostics>,
    },
    #[error(transparent)]
    Resolver(#[from] LocalFilesystemContentResolverError),
    #[error("delegated indexing produced no blocks")]
    EmptyDelegatedOutput,
    #[error("the configured block store contains no clustering-eligible blocks")]
    NoClusterableBlocks,
    #[error(
        "block store iteration returned block id {block_id}, but no block content was available"
    )]
    MissingIteratedBlock { block_id: String },
    #[error("failed to serialize iterated block {block_id}: {source}")]
    SerializeIteratedBlock {
        block_id: String,
        #[source]
        source: BlockError,
    },
    #[error("iterated block hash mismatch: expected {expected}, rebuilt block produced {actual}")]
    IteratedBlockHashMismatch { expected: String, actual: String },
    #[error(
        "iterated block {block_id} does not contain replay metadata for a supported content item"
    )]
    MissingReplayMetadata { block_id: String },
    #[error("leaf-indexing worker task failed: {0}")]
    LeafTaskJoin(#[from] JoinError),
    #[error("blocking mutable-ref task failed: {0}")]
    BlockingMutableRefTaskJoin(JoinError),
    #[error("blocking replay-prefetch task failed: {0}")]
    BlockingReplayPrefetchTaskJoin(JoinError),
    #[error("failed to write batch summary {path}: {source}")]
    WriteSummary {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to render batch summary: {0}")]
    RenderSummary(#[from] serde_json::Error),
    #[error("failed to write clustering diagnostics {path}: {source}")]
    WriteClusteringDiagnostics {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to render clustering diagnostics: {source}")]
    RenderClusteringDiagnostics {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to prepare mutable ref store {path}: {source}")]
    PrepareMutableRefStore {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to prepare planner state root {path}: {source}")]
    PreparePlannerStateRoot {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to prepare replay-order scratch root {path}: {source}")]
    PrepareReplayOrderScratchRoot {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("clustering run is missing the prepared replay-order scratch root")]
    MissingReplayOrderScratchRoot,
    #[error("v2 clustering run is missing the prepared planner-state root")]
    MissingPlannerStateRoot,
    #[error("failed to write replay-order scratch file {path}: {source}")]
    WriteReplayOrderScratch {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to read replay-order scratch file {path}: {source}")]
    ReadReplayOrderScratch {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to write mutable ref store {path}: {source}")]
    WriteMutableRefStore {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to read mutable ref store {path}: {source}")]
    ReadMutableRefStore {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to encode mutable ref store {path}: {message}")]
    EncodeMutableRefStore { path: String, message: String },
    #[error("failed to decode mutable ref store {path}: {message}")]
    DecodeMutableRefStore { path: String, message: String },
    #[error("replay journal head reference is missing from mutable ref store {path}")]
    MissingReplayJournalHead { path: String },
    #[error("replay journal head {block_id} is invalid: {message}")]
    InvalidReplayJournalHead { block_id: String, message: String },
    #[error("failed to append replay journal block {block_id}: {source}")]
    WriteReplayJournal {
        block_id: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to read replay journal block {block_id}: {source}")]
    ReadReplayJournal {
        block_id: String,
        #[source]
        source: io::Error,
    },
    #[error("parallel replay batch materialization worker panicked: {message}")]
    ParallelReplayBatchWorkerPanic { message: String },
    #[error("parallel replay batch materialization did not complete entry {entry_index}")]
    ParallelReplayBatchMaterializationIncomplete { entry_index: usize },
}

impl RuntimeError {
    pub fn clustering_failure_diagnostics(&self) -> Option<&ClusteringFailureDiagnostics> {
        match self {
            Self::ClusteringFailure { diagnostics, .. } => Some(diagnostics),
            _ => None,
        }
    }
}

impl EmbeddingProvider for StoredLeafEmbeddingProvider {
    type Error = StoredLeafEmbeddingProviderError;

    async fn embed(
        &self,
        input: &EmbeddingInput,
        _: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        let key = hash_embedding_input(input).into_bytes();
        self.embeddings_by_input_hash
            .get(&key)
            .cloned()
            .ok_or(StoredLeafEmbeddingProviderError::MissingStoredEmbedding)
    }
}

impl ContentResolver<ContentRef> for ValidateOnlyResolver {
    type Error = io::Error;

    fn resolve(&self, _content_ref: &ContentRef) -> Result<lexongraph_block::Content, Self::Error> {
        Err(io::Error::other(
            "validate-only resolver should not be used for content resolution",
        ))
    }

    fn fingerprint(&self, _content_ref: &ContentRef) -> Result<BlockHash, Self::Error> {
        Err(io::Error::other(
            "validate-only resolver should not be used for fingerprinting",
        ))
    }
}

impl EmbeddingProvider for ValidateOnlyEmbeddingProvider {
    type Error = io::Error;

    async fn embed(
        &self,
        _input: &EmbeddingInput,
        _spec: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        Err(io::Error::other(
            "validate-only embedding provider should not be asked to embed",
        ))
    }
}

impl ClusteringFailureEmbeddingSource for StoredLeafEmbeddingProvider {
    fn embedding_for_hash(&self, input_hash: &[u8; 32]) -> Result<Option<Vec<u8>>, String> {
        Ok(self.embeddings_by_input_hash.get(input_hash).cloned())
    }
}

impl EmbeddingProvider for ExternalizedStoredLeafEmbeddingProvider {
    type Error = StoredLeafEmbeddingProviderError;

    async fn embed(
        &self,
        input: &EmbeddingInput,
        _: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        let key = hash_embedding_input(input).into_bytes();
        self.load_embedding_for_hash(&key)?
            .ok_or(StoredLeafEmbeddingProviderError::MissingStoredEmbedding)
    }
}

impl ClusteringFailureEmbeddingSource for ExternalizedStoredLeafEmbeddingProvider {
    fn embedding_for_hash(&self, input_hash: &[u8; 32]) -> Result<Option<Vec<u8>>, String> {
        self.load_embedding_for_hash(input_hash)
            .map_err(|error| error.to_string())
    }
}

#[allow(dead_code)]
impl<EP> RecordingEmbeddingProvider<EP> {
    fn new(inner: EP) -> Self {
        Self {
            inner,
            embeddings_by_input_hash: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<EP> ClusteringFailureEmbeddingSource for RecordingEmbeddingProvider<EP> {
    fn embedding_for_hash(&self, input_hash: &[u8; 32]) -> Result<Option<Vec<u8>>, String> {
        Ok(lock_unpoisoned(&self.embeddings_by_input_hash)
            .get(input_hash)
            .cloned())
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn usize_is_zero(value: &usize) -> bool {
    *value == 0
}

impl ExternalizedReplayState {
    fn total_batches(&self) -> usize {
        if self.total_items == 0 {
            0
        } else {
            self.total_items.div_ceil(self.batch_size)
        }
    }

    fn batch_iterator(&self) -> Result<ExternalizedReplayBatchIterator, RuntimeError> {
        Ok(ExternalizedReplayBatchIterator {
            replay_order_reader: self.replay_order.open_reader()?,
            batch_size: self.batch_size,
            materialization_max_concurrency: self.materialization_max_concurrency,
            block_store: self.block_store.clone(),
            embedding_spec: self.embedding_spec.clone(),
            current_batch_embeddings: Arc::clone(&self.current_batch_embeddings),
        })
    }

    fn finalize_source(&self) -> Result<ExternalizedReplayFinalizeSource, RuntimeError> {
        Ok(ExternalizedReplayFinalizeSource {
            inner: self.batch_iterator()?,
        })
    }

    #[cfg(test)]
    fn replay_input_block_ids(&self) -> Result<Vec<String>, RuntimeError> {
        self.replay_order.replay_input_block_ids()
    }

    fn collect_replay_batches(&self) -> Result<Vec<ReplayBatch>, RuntimeError> {
        let mut iterator = self.batch_iterator()?;
        let mut batches = Vec::new();
        while let Some(batch) = iterator.next_batch()? {
            batches.push(batch);
        }
        annotate_submission_progress_batches(&mut batches, SubmissionProgressKind::Replay);
        Ok(batches)
    }
}

impl ExternalizedStoredLeafEmbeddingProvider {
    fn load_embedding_for_hash(
        &self,
        input_hash: &[u8; 32],
    ) -> Result<Option<Vec<u8>>, StoredLeafEmbeddingProviderError> {
        if let Some(embedding) = lock_unpoisoned(&self.current_batch_embeddings)
            .get(input_hash)
            .cloned()
        {
            return Ok(Some(embedding));
        }

        if self.replay_order.total_items() > EXTERNALIZED_CLUSTERING_DIAGNOSTIC_INPUT_LIMIT {
            return Ok(None);
        }

        {
            let cache = lock_unpoisoned(&self.fallback_embeddings);
            if let Some(cache) = cache.as_ref() {
                return Ok(cache.get(input_hash).cloned());
            }
        }

        let mut fallback_embeddings = HashMap::new();
        for entry in self
            .replay_order
            .read_all_entries()
            .map_err(map_replay_order_runtime_error_to_provider_error)?
        {
            let block_hash = entry.block_hash();
            let validated = block_on_block_store_future(self.block_store.get(&block_hash))
                .map_err(
                    |source| StoredLeafEmbeddingProviderError::ReadStoredEmbeddingBlock {
                        block_id: block_hash.to_string(),
                        source,
                    },
                )?;
            let Some(validated) = validated else {
                continue;
            };
            let Some(candidate_hash) =
                replay_embedding_input_hash(&validated, &self.embedding_spec).map_err(|error| {
                    StoredLeafEmbeddingProviderError::InvalidStoredEmbeddingBlock {
                        block_id: block_hash.to_string(),
                        message: error.to_string(),
                    }
                })?
            else {
                return Err(
                    StoredLeafEmbeddingProviderError::InvalidStoredEmbeddingBlock {
                        block_id: block_hash.to_string(),
                        message: "stored block does not contain a replayable leaf embedding".into(),
                    },
                );
            };
            let replayed = replay_item_from_validated_block(&validated, &self.embedding_spec)
                .map_err(
                    |error| StoredLeafEmbeddingProviderError::InvalidStoredEmbeddingBlock {
                        block_id: block_hash.to_string(),
                        message: error.to_string(),
                    },
                )?
                .ok_or_else(
                    || StoredLeafEmbeddingProviderError::InvalidStoredEmbeddingBlock {
                        block_id: block_hash.to_string(),
                        message: "stored block does not contain a replayable leaf embedding".into(),
                    },
                )?;
            fallback_embeddings.insert(candidate_hash.into_bytes(), replayed.1);
        }

        let result = fallback_embeddings.get(input_hash).cloned();
        *lock_unpoisoned(&self.fallback_embeddings) = Some(fallback_embeddings);
        Ok(result)
    }
}

impl ExternalizedReplayBatchIterator {
    fn clear_current_batch_embeddings(&self) {
        lock_unpoisoned(&self.current_batch_embeddings).clear();
    }

    fn publish_batch_embeddings(&self, embeddings_by_input_hash: &[([u8; 32], Vec<u8>)]) {
        let mut cache = lock_unpoisoned(&self.current_batch_embeddings);
        cache.clear();
        cache.extend(embeddings_by_input_hash.iter().cloned());
    }

    fn load_next_batch(&mut self) -> Result<Option<ReplayBatchLoad>, RuntimeError> {
        let entries = self
            .replay_order_reader
            .read_next_entries(self.batch_size)?;
        if entries.is_empty() {
            return Ok(None);
        }
        Ok(Some(replay_batch_from_entries(
            &entries,
            &self.block_store,
            &self.embedding_spec,
            self.materialization_max_concurrency,
        )?))
    }

    fn next_batch(&mut self) -> Result<Option<ReplayBatch>, RuntimeError> {
        let Some(batch) = self.load_next_batch()? else {
            self.clear_current_batch_embeddings();
            return Ok(None);
        };
        self.publish_batch_embeddings(&batch.embeddings_by_input_hash);
        Ok(Some(batch.batch))
    }
}

fn spawn_externalized_replay_batch_prefetches(
    iterator: ExternalizedReplayBatchIterator,
    batch_count: usize,
) -> ExternalizedReplayBatchPrefetchHandle {
    tokio::task::spawn_blocking(move || {
        let mut iterator = iterator;
        let mut prefetched_batches = VecDeque::with_capacity(batch_count);
        for _ in 0..batch_count {
            let Some(next) = iterator.load_next_batch()? else {
                break;
            };
            prefetched_batches.push_back(next);
        }
        Ok((iterator, prefetched_batches))
    })
}

async fn await_externalized_replay_batch_prefetch(
    pending_prefetch: &mut Option<ExternalizedReplayBatchPrefetchHandle>,
    iterator: &mut Option<ExternalizedReplayBatchIterator>,
    prefetched_batches: &mut VecDeque<ReplayBatchLoad>,
) -> Result<(), RuntimeError> {
    let Some(prefetch) = pending_prefetch.take() else {
        return Ok(());
    };
    let (next_iterator, mut newly_prefetched_batches) = prefetch
        .await
        .map_err(RuntimeError::BlockingReplayPrefetchTaskJoin)??;
    *iterator = Some(next_iterator);
    prefetched_batches.append(&mut newly_prefetched_batches);
    Ok(())
}

async fn take_next_externalized_replay_batch(
    iterator: &mut Option<ExternalizedReplayBatchIterator>,
    prefetched_batches: &mut VecDeque<ReplayBatchLoad>,
    pending_prefetch: &mut Option<ExternalizedReplayBatchPrefetchHandle>,
) -> Result<Option<ReplayBatchLoad>, RuntimeError> {
    if let Some(next_batch) = prefetched_batches.pop_front() {
        return Ok(Some(next_batch));
    }
    if iterator.is_none() {
        await_externalized_replay_batch_prefetch(pending_prefetch, iterator, prefetched_batches)
            .await?;
    }
    if let Some(next_batch) = prefetched_batches.pop_front() {
        return Ok(Some(next_batch));
    }
    match iterator.as_mut() {
        Some(iterator) => iterator.load_next_batch(),
        None => Ok(None),
    }
}

impl IntoIterator for ExternalizedReplayFinalizeSource {
    type Item = Vec<IndexItem<ContentRef>>;
    type IntoIter = ExternalizedReplayFinalizeIterator;

    fn into_iter(self) -> Self::IntoIter {
        ExternalizedReplayFinalizeIterator { inner: self.inner }
    }
}

impl Iterator for ExternalizedReplayFinalizeIterator {
    type Item = Vec<IndexItem<ContentRef>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next_batch()
            .expect("in-memory replay state should remain readable during finalization")
            .map(|batch| batch.items)
    }
}

fn replay_batch_from_entries(
    entries: &[ReplayOrderEntry],
    store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
    materialization_max_concurrency: usize,
) -> Result<ReplayBatchLoad, RuntimeError> {
    let mut items = Vec::with_capacity(entries.len());
    let mut audit_records = Vec::with_capacity(entries.len());
    let mut embeddings_by_input_hash = Vec::with_capacity(entries.len());
    if replay_batch_materialization_worker_count(entries.len(), materialization_max_concurrency)
        <= 1
    {
        for entry in entries.iter().copied() {
            let loaded_entry = load_replay_batch_entry(entry, store, embedding_spec)?;
            embeddings_by_input_hash.push((loaded_entry.input_hash, loaded_entry.embedding));
            audit_records.push(loaded_entry.audit_record);
            items.push(loaded_entry.item);
        }
    } else {
        for loaded_entry in load_replay_batch_entries_in_parallel(
            entries,
            store,
            embedding_spec,
            materialization_max_concurrency,
        )? {
            embeddings_by_input_hash.push((loaded_entry.input_hash, loaded_entry.embedding));
            audit_records.push(loaded_entry.audit_record);
            items.push(loaded_entry.item);
        }
    }
    Ok(ReplayBatchLoad {
        batch: ReplayBatch {
            items,
            audit_records,
            completion_message: None,
        },
        embeddings_by_input_hash,
    })
}

fn replay_batch_materialization_worker_count(
    entry_count: usize,
    materialization_max_concurrency: usize,
) -> usize {
    entry_count.min(materialization_max_concurrency.max(1)).min(
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1),
    )
}

fn load_replay_batch_entries_in_parallel(
    entries: &[ReplayOrderEntry],
    store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
    materialization_max_concurrency: usize,
) -> Result<Vec<ReplayBatchEntryLoad>, RuntimeError> {
    let worker_count =
        replay_batch_materialization_worker_count(entries.len(), materialization_max_concurrency);
    let next_index = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut completed = (0..entries.len()).map(|_| None).collect::<Vec<_>>();
    std::thread::scope(|scope| -> Result<(), RuntimeError> {
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let mut worker_handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let result_tx = result_tx.clone();
            let store = store.clone();
            let embedding_spec = embedding_spec.clone();
            let next_index = Arc::clone(&next_index);
            worker_handles.push(scope.spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build tokio runtime for replay batch materialization");
                loop {
                    let index = next_index.fetch_add(1, Ordering::Relaxed);
                    if index >= entries.len() {
                        break;
                    }
                    let result = load_replay_batch_entry_with_runtime(
                        entries[index],
                        &store,
                        &embedding_spec,
                        &runtime,
                    );
                    let _ = result_tx.send((index, result));
                }
            }));
        }
        drop(result_tx);
        for (index, result) in result_rx {
            completed[index] = Some(result);
        }
        for worker_handle in worker_handles {
            if let Err(payload) = worker_handle.join() {
                return Err(RuntimeError::ParallelReplayBatchWorkerPanic {
                    message: thread_panic_message(payload),
                });
            }
        }
        Ok(())
    })?;
    let mut ordered = Vec::with_capacity(entries.len());
    for (entry_index, entry) in completed.into_iter().enumerate() {
        ordered.push(
            entry.ok_or(RuntimeError::ParallelReplayBatchMaterializationIncomplete {
                entry_index,
            })??,
        );
    }
    Ok(ordered)
}

fn thread_panic_message(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

fn load_replay_batch_entry(
    entry: ReplayOrderEntry,
    store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
) -> Result<ReplayBatchEntryLoad, RuntimeError> {
    let block_id = entry.block_hash();
    let Some(validated) = block_on_block_store_future(store.get(&block_id))? else {
        return Err(RuntimeError::ReadReplayJournal {
            block_id: block_id.to_string(),
            source: io::Error::new(
                ErrorKind::NotFound,
                "journal entry references missing block",
            ),
        });
    };
    load_replay_batch_entry_from_validated(entry, validated, embedding_spec)
}

fn load_replay_batch_entry_with_runtime(
    entry: ReplayOrderEntry,
    store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
    runtime: &tokio::runtime::Runtime,
) -> Result<ReplayBatchEntryLoad, RuntimeError> {
    let block_id = entry.block_hash();
    let Some(validated) = runtime.block_on(store.get(&block_id))? else {
        return Err(RuntimeError::ReadReplayJournal {
            block_id: block_id.to_string(),
            source: io::Error::new(
                ErrorKind::NotFound,
                "journal entry references missing block",
            ),
        });
    };
    load_replay_batch_entry_from_validated(entry, validated, embedding_spec)
}

fn load_replay_batch_entry_from_validated(
    entry: ReplayOrderEntry,
    validated: lexongraph_block::ValidatedBlock,
    embedding_spec: &EmbeddingSpec,
) -> Result<ReplayBatchEntryLoad, RuntimeError> {
    let block_id = validated.hash;
    let Some(input_hash) = replay_embedding_input_hash(&validated, embedding_spec)? else {
        return Err(RuntimeError::MissingReplayMetadata {
            block_id: block_id.to_string(),
        });
    };
    let Some((item, embedding)) = replay_item_from_validated_block(&validated, embedding_spec)?
    else {
        return Err(RuntimeError::MissingReplayMetadata {
            block_id: block_id.to_string(),
        });
    };
    if replay_sort_key_digest(&item) != entry.digest_hash() {
        return Err(RuntimeError::InvalidReplayJournalHead {
            block_id: validated.hash.to_string(),
            message: "journal entry does not match referenced block replay metadata".into(),
        });
    }
    Ok(ReplayBatchEntryLoad {
        audit_record: replay_journal_record_from_item(validated.hash, &item),
        item,
        input_hash: input_hash.into_bytes(),
        embedding,
    })
}

impl<EP> EmbeddingProvider for RecordingEmbeddingProvider<EP>
where
    EP: EmbeddingProvider,
{
    type Error = EP::Error;

    async fn embed(
        &self,
        input: &EmbeddingInput,
        spec: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        let key = hash_embedding_input(input).into_bytes();
        if let Some(embedding) = lock_unpoisoned(&self.embeddings_by_input_hash)
            .get(&key)
            .cloned()
        {
            return Ok(embedding);
        }

        let embedding = self.inner.embed(input, spec).await?;
        lock_unpoisoned(&self.embeddings_by_input_hash).insert(key, embedding.clone());
        Ok(embedding)
    }

    async fn embed_batch(
        &self,
        inputs: &[EmbeddingInput],
        spec: &EmbeddingSpec,
    ) -> Result<Vec<Vec<u8>>, Self::Error> {
        let mut embeddings = vec![None; inputs.len()];
        let mut missing_indices = Vec::new();
        let mut missing_inputs = Vec::new();
        {
            let cache = lock_unpoisoned(&self.embeddings_by_input_hash);
            for (index, input) in inputs.iter().enumerate() {
                let key = hash_embedding_input(input).into_bytes();
                if let Some(embedding) = cache.get(&key) {
                    embeddings[index] = Some(embedding.clone());
                } else {
                    missing_indices.push(index);
                    missing_inputs.push(input.clone());
                }
            }
        }
        if missing_inputs.is_empty() {
            return Ok(embeddings.into_iter().map(Option::unwrap).collect());
        }

        let fetched_embeddings = self.inner.embed_batch(&missing_inputs, spec).await?;
        {
            let mut cache = lock_unpoisoned(&self.embeddings_by_input_hash);
            for ((index, input), embedding) in missing_indices
                .into_iter()
                .zip(missing_inputs.iter())
                .zip(fetched_embeddings)
            {
                cache.insert(hash_embedding_input(input).into_bytes(), embedding.clone());
                embeddings[index] = Some(embedding);
            }
        }
        Ok(embeddings.into_iter().map(Option::unwrap).collect())
    }
}

fn resolved_published_profile(
    clustering: &ConfiguredClustering,
) -> Result<PublishedIndexingProfile, StreamingIndexerError> {
    let mut profile = published_indexing_profile(clustering.profile_version)?;
    if let Some(cluster_count) = clustering.local_testing_cluster_count {
        match &mut profile.planning_strategy {
            PublishedPlanningStrategy::SphericalKmeansGreedyPack(settings) => {
                settings.cluster_count = cluster_count;
            }
            PublishedPlanningStrategy::DirectionalPcaDivisive(settings) => {
                settings.cluster_count = cluster_count;
            }
        }
    }
    Ok(profile)
}

fn uses_streaming_indexer_v2(clustering: &ConfiguredClustering) -> bool {
    clustering.profile_version == PUBLISHED_PROFILE_V0_7_0
}

const V2_PLANNING_NEEDS_ROUTED_OR_TERMINAL_PREFIX: &str =
    "planning completion requires every v2 partition to be terminal or routed";
const V2_PLANNING_NEEDS_CHILDREN_PREFIX: &str =
    "planning completion requires every routed v2 partition to install child partitions";

fn is_incomplete_v2_planning_transition(error: &StreamingIndexerError) -> bool {
    matches!(
        error,
        StreamingIndexerError::InvalidLifecycleTransition(message)
            if message.starts_with(V2_PLANNING_NEEDS_ROUTED_OR_TERMINAL_PREFIX)
                || message.starts_with(V2_PLANNING_NEEDS_CHILDREN_PREFIX)
    )
}

#[derive(Debug, PartialEq, Eq)]
enum PlanningCompletionAction {
    Complete,
    ReplayRequired(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct PlanningPassReport {
    completed_pass_count: usize,
    observed_item_count: usize,
    requested_planning_cluster_count: Option<u32>,
    realized_planning_cluster_count: Option<u32>,
    planning_quality_metric: f64,
    planning_balance_metric: f64,
    planned_partition_count: usize,
    terminal_partition_count: usize,
    hierarchy_depth: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
enum PlanningConvergenceVerdict {
    #[serde(rename = "converging")]
    Converging,
    #[serde(rename = "not-converging")]
    NotConverging,
    #[serde(rename = "inconclusive")]
    Inconclusive,
}

impl PlanningConvergenceVerdict {
    fn as_str(self) -> &'static str {
        match self {
            Self::Converging => "converging",
            Self::NotConverging => "not-converging",
            Self::Inconclusive => "inconclusive",
        }
    }
}

#[derive(Clone, Debug)]
struct PlanningPassDiagnosis {
    verdict: PlanningConvergenceVerdict,
    evidence_summary: String,
}

#[derive(Debug, Default)]
struct PlanningTelemetryState {
    previous_pass_report: Option<PlanningPassReport>,
    latest_pass_diagnosis: Option<PlanningPassDiagnosis>,
    latest_blocked_on_summary: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
enum DelegatedContractFamily {
    #[serde(rename = "legacy/non-v2")]
    LegacyNonV2,
    #[serde(rename = "v2")]
    V2,
}

impl DelegatedContractFamily {
    fn as_str(self) -> &'static str {
        match self {
            Self::LegacyNonV2 => "legacy/non-v2",
            Self::V2 => "v2",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PlanningRunIdentity {
    effective_profile_version: String,
    delegated_contract_family: DelegatedContractFamily,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct PlanningPassTelemetryRecord {
    telemetry_kind: &'static str,
    effective_profile_version: String,
    delegated_contract_family: DelegatedContractFamily,
    completed_pass_count: usize,
    observed_item_count: usize,
    requested_planning_cluster_count: Option<u32>,
    realized_planning_cluster_count: Option<u32>,
    planning_quality_metric: f64,
    planning_balance_metric: f64,
    planned_partition_count: usize,
    terminal_partition_count: usize,
    hierarchy_depth: usize,
    convergence_verdict: PlanningConvergenceVerdict,
    convergence_evidence_summary: String,
    last_known_blocked_on_summary: Option<String>,
    planning_completion_state: String,
    planning_completion_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct PlanningPendingPartitionTelemetryRecord {
    partition_path: String,
    expected_item_count: usize,
    observed_replay_progress: Option<usize>,
    routing_bucket_fill_counts: Option<Vec<usize>>,
    trainer_subphase: Option<&'static str>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct PlanningIntraPassTelemetryRecord {
    telemetry_kind: &'static str,
    effective_profile_version: String,
    delegated_contract_family: DelegatedContractFamily,
    pass_number: usize,
    planning_status_state: &'static str,
    observed_item_count: usize,
    completed_unit_count: usize,
    phase_total_unit_count: Option<usize>,
    elapsed_ms: u128,
    last_progress_ms: Option<u128>,
    pending_partition_count: Option<usize>,
    pending_partition_preview_count: Option<usize>,
    pending_partitions: Option<Vec<PlanningPendingPartitionTelemetryRecord>>,
    suspected_stall_reason: Option<&'static str>,
    suspected_stall_duration_without_progress_ms: Option<u128>,
    convergence_verdict: PlanningConvergenceVerdict,
    convergence_evidence_summary: String,
    blocked_on_summary: Option<String>,
}

#[derive(Clone, Debug)]
struct PlanningTelemetryContext {
    run_identity: PlanningRunIdentity,
    sink_path: Option<PathBuf>,
    sink_initialized: Arc<AtomicBool>,
    sink_write_lock: Arc<Mutex<()>>,
    diagnosis_state: Arc<Mutex<PlanningTelemetryState>>,
}

fn v2_planning_completion_action(
    completed_pass_count: usize,
    result: Result<(), StreamingIndexerError>,
) -> Result<PlanningCompletionAction, StreamingIndexerError> {
    match result {
        Ok(()) => Ok(PlanningCompletionAction::Complete),
        Err(error) if is_incomplete_v2_planning_transition(&error) => {
            Ok(PlanningCompletionAction::ReplayRequired(format!(
                "Planning pass {} requires another full replay pass before v2 planning can complete: {}",
                completed_pass_count, error
            )))
        }
        Err(error) => Err(error),
    }
}

fn planning_run_identity(clustering: &ConfiguredClustering) -> PlanningRunIdentity {
    PlanningRunIdentity {
        effective_profile_version: clustering.profile_version.to_string(),
        delegated_contract_family: if uses_streaming_indexer_v2(clustering) {
            DelegatedContractFamily::V2
        } else {
            DelegatedContractFamily::LegacyNonV2
        },
    }
}

impl PlanningTelemetryContext {
    fn bootstrap_message(&self) -> String {
        let mut message = format!(
            "Clustering run identity: effective profile {} via delegated contract {}",
            self.run_identity.effective_profile_version,
            self.run_identity.delegated_contract_family.as_str()
        );
        if let Some(path) = &self.sink_path {
            message.push_str(&format!(
                "; planning pass telemetry file {}",
                path.display()
            ));
        }
        message
    }

    fn project_pass_summary(
        &self,
        pass_report: PlanningPassReport,
        completion: &PlanningCompletionAction,
    ) -> (String, PlanningPassTelemetryRecord) {
        let mut state = lock_unpoisoned(&self.diagnosis_state);
        let diagnosis =
            planning_pass_diagnosis(state.previous_pass_report, pass_report, completion);
        let last_known_blocked_on_summary = state.latest_blocked_on_summary.clone();
        let mut message = format!(
            "Completed planning pass {} over {} item(s) for profile {} via {}",
            pass_report.completed_pass_count,
            pass_report.observed_item_count,
            self.run_identity.effective_profile_version,
            self.run_identity.delegated_contract_family.as_str()
        );
        message.push_str(&format!(
            "; terminal partitions {}/{}; hierarchy depth {}",
            pass_report.terminal_partition_count,
            pass_report.planned_partition_count,
            pass_report.hierarchy_depth
        ));
        if let (Some(requested), Some(realized)) = (
            pass_report.requested_planning_cluster_count,
            pass_report.realized_planning_cluster_count,
        ) {
            message.push_str(&format!(
                "; realized/requested clusters {realized}/{requested}"
            ));
        }
        message.push_str(&format!(
            "; quality {:.6}; balance {:.6}",
            pass_report.planning_quality_metric, pass_report.planning_balance_metric
        ));
        message.push_str(&format!(
            "; diagnosis {} ({})",
            diagnosis.verdict.as_str(),
            diagnosis.evidence_summary
        ));
        if let Some(blocked_on_summary) = last_known_blocked_on_summary.as_deref() {
            message.push_str(&format!("; last blocked on {blocked_on_summary}"));
        }
        let record = PlanningPassTelemetryRecord {
            telemetry_kind: "pass-summary",
            effective_profile_version: self.run_identity.effective_profile_version.clone(),
            delegated_contract_family: self.run_identity.delegated_contract_family,
            completed_pass_count: pass_report.completed_pass_count,
            observed_item_count: pass_report.observed_item_count,
            requested_planning_cluster_count: pass_report.requested_planning_cluster_count,
            realized_planning_cluster_count: pass_report.realized_planning_cluster_count,
            planning_quality_metric: pass_report.planning_quality_metric,
            planning_balance_metric: pass_report.planning_balance_metric,
            planned_partition_count: pass_report.planned_partition_count,
            terminal_partition_count: pass_report.terminal_partition_count,
            hierarchy_depth: pass_report.hierarchy_depth,
            convergence_verdict: diagnosis.verdict,
            convergence_evidence_summary: diagnosis.evidence_summary.clone(),
            last_known_blocked_on_summary: last_known_blocked_on_summary.clone(),
            planning_completion_state: match completion {
                PlanningCompletionAction::Complete => "complete".to_string(),
                PlanningCompletionAction::ReplayRequired(_) => "replay-required".to_string(),
            },
            planning_completion_reason: match completion {
                PlanningCompletionAction::Complete => None,
                PlanningCompletionAction::ReplayRequired(reason) => Some(reason.clone()),
            },
        };
        state.previous_pass_report = Some(pass_report);
        state.latest_pass_diagnosis = Some(diagnosis);
        state.latest_blocked_on_summary = None;
        (message, record)
    }

    fn project_planning_status(
        &self,
        status: &StreamingIndexingStatus,
    ) -> (Option<PlanningIntraPassTelemetryRecord>, Option<String>) {
        let mut state = lock_unpoisoned(&self.diagnosis_state);
        let blocked_on_summary = planning_blocked_on_summary(status);
        if let Some(blocked_on_summary) = blocked_on_summary.as_ref()
            && (blocked_on_summary != UNKNOWN_BLOCKED_ON_SUMMARY
                || state.latest_blocked_on_summary.is_none())
        {
            state.latest_blocked_on_summary = Some(blocked_on_summary.clone());
        }
        let latest_pass_diagnosis =
            state
                .latest_pass_diagnosis
                .clone()
                .unwrap_or_else(|| PlanningPassDiagnosis {
                    verdict: PlanningConvergenceVerdict::Inconclusive,
                    evidence_summary: "no completed-pass comparison available yet".to_string(),
                });
        let progress_message = planning_status_diagnosis_message(
            &latest_pass_diagnosis,
            status,
            blocked_on_summary.as_deref(),
        );
        if self.run_identity.delegated_contract_family != DelegatedContractFamily::V2 {
            return (None, progress_message);
        }
        let StreamingIndexingPhase::PlanningPass { pass_number } = status.phase else {
            return (None, progress_message);
        };
        if matches!(status.state, StreamingIndexingStatusState::Completed) {
            return (None, progress_message);
        }
        let pending_partitions = status.v2_pending_partitions.as_ref().map(|partitions| {
            partitions
                .iter()
                .take(2)
                .map(planning_pending_partition_telemetry_record)
                .collect::<Vec<_>>()
        });
        let record = PlanningIntraPassTelemetryRecord {
            telemetry_kind: "intra-pass",
            effective_profile_version: self.run_identity.effective_profile_version.clone(),
            delegated_contract_family: self.run_identity.delegated_contract_family,
            pass_number,
            planning_status_state: planning_status_state_label(status.state),
            observed_item_count: status.item_count,
            completed_unit_count: status.completed_unit_count,
            phase_total_unit_count: status.phase_total_unit_count,
            elapsed_ms: status.elapsed.as_millis(),
            last_progress_ms: status.last_progress_at.map(|duration| duration.as_millis()),
            pending_partition_count: status
                .pending_partition_count
                .or_else(|| status.v2_pending_partitions.as_ref().map(Vec::len)),
            pending_partition_preview_count: pending_partitions.as_ref().map(Vec::len),
            pending_partitions,
            suspected_stall_reason: status
                .suspected_stall
                .as_ref()
                .map(|stall| suspected_stall_reason_label(stall.reason)),
            suspected_stall_duration_without_progress_ms: status
                .suspected_stall
                .as_ref()
                .map(|stall| stall.duration_without_progress.as_millis()),
            convergence_verdict: latest_pass_diagnosis.verdict,
            convergence_evidence_summary: latest_pass_diagnosis.evidence_summary,
            blocked_on_summary,
        };
        (Some(record), progress_message)
    }

    fn write_json_record<T: Serialize>(&self, record: &T) -> Result<(), String> {
        let Some(path) = &self.sink_path else {
            return Ok(());
        };
        if let Some(parent) = parent_directory_to_create(path) {
            fs::create_dir_all(parent).map_err(|source| {
                format!(
                    "Failed to create planning pass telemetry directory for {}: {}",
                    path.display(),
                    source
                )
            })?;
        }
        let mut rendered = serde_json::to_vec(record).map_err(|source| {
            format!(
                "Failed to render planning pass telemetry for {}: {}",
                path.display(),
                source
            )
        })?;
        rendered.push(b'\n');
        let _write_guard = lock_unpoisoned(&self.sink_write_lock);
        let mut file_options = fs::OpenOptions::new();
        file_options.create(true).write(true);
        let is_first_record = self
            .sink_initialized
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if is_first_record {
            file_options.truncate(true);
        } else {
            file_options.append(true);
        }
        let mut file = file_options.open(path).map_err(|source| {
            format!(
                "Failed to open planning pass telemetry file {}: {}",
                path.display(),
                source
            )
        })?;
        file.write_all(&rendered).map_err(|source| {
            format!(
                "Failed to write planning pass telemetry file {}: {}",
                path.display(),
                source
            )
        })
    }
}

fn planning_status_state_label(state: StreamingIndexingStatusState) -> &'static str {
    match state {
        StreamingIndexingStatusState::Started => "started",
        StreamingIndexingStatusState::InProgress => "in-progress",
        StreamingIndexingStatusState::Completed => "completed",
        StreamingIndexingStatusState::Failed => "failed",
    }
}

fn trainer_subphase_label(subphase: StreamingIndexingTrainerSubphase) -> &'static str {
    match subphase {
        StreamingIndexingTrainerSubphase::AnalyzePca => "analyze-pca",
        StreamingIndexingTrainerSubphase::PlanCuts => "plan-cuts",
        StreamingIndexingTrainerSubphase::CountCells => "count-cells",
        StreamingIndexingTrainerSubphase::RealizePartition => "realize-partition",
    }
}

fn suspected_stall_reason_label(reason: StreamingIndexingSuspectedStallReason) -> &'static str {
    match reason {
        StreamingIndexingSuspectedStallReason::UnchangedPassObservedCount => {
            "unchanged-pass-observed-count"
        }
        StreamingIndexingSuspectedStallReason::UnchangedPendingPartitionProgress => {
            "unchanged-pending-partition-progress"
        }
        StreamingIndexingSuspectedStallReason::UnchangedRoutingBucketFill => {
            "unchanged-routing-bucket-fill"
        }
        StreamingIndexingSuspectedStallReason::UnchangedTrainerSubphase => {
            "unchanged-trainer-subphase"
        }
    }
}

fn planning_partition_gap(report: PlanningPassReport) -> usize {
    report
        .planned_partition_count
        .saturating_sub(report.terminal_partition_count)
}

fn planning_pass_diagnosis(
    previous: Option<PlanningPassReport>,
    current: PlanningPassReport,
    completion: &PlanningCompletionAction,
) -> PlanningPassDiagnosis {
    if matches!(completion, PlanningCompletionAction::Complete) {
        return PlanningPassDiagnosis {
            verdict: PlanningConvergenceVerdict::Converging,
            evidence_summary: format!(
                "planning completion confirmed on pass {}",
                current.completed_pass_count
            ),
        };
    }
    let Some(previous) = previous else {
        return PlanningPassDiagnosis {
            verdict: PlanningConvergenceVerdict::Inconclusive,
            evidence_summary:
                "first completed pass; need another completed pass before trend can be compared"
                    .to_string(),
        };
    };
    let previous_gap = planning_partition_gap(previous);
    let current_gap = planning_partition_gap(current);
    let mut improvements = Vec::new();
    let mut regressions = Vec::new();
    if current.terminal_partition_count > previous.terminal_partition_count {
        improvements.push(format!(
            "terminal partitions {} -> {}",
            previous.terminal_partition_count, current.terminal_partition_count
        ));
    } else if current.terminal_partition_count < previous.terminal_partition_count {
        regressions.push(format!(
            "terminal partitions {} -> {}",
            previous.terminal_partition_count, current.terminal_partition_count
        ));
    }
    if current_gap < previous_gap {
        improvements.push(format!("non-terminal gap {previous_gap} -> {current_gap}"));
    } else if current_gap > previous_gap {
        regressions.push(format!("non-terminal gap {previous_gap} -> {current_gap}"));
    }
    if !improvements.is_empty() && regressions.is_empty() {
        return PlanningPassDiagnosis {
            verdict: PlanningConvergenceVerdict::Converging,
            evidence_summary: improvements.join("; "),
        };
    }
    if improvements.is_empty() && regressions.is_empty() {
        return PlanningPassDiagnosis {
            verdict: PlanningConvergenceVerdict::NotConverging,
            evidence_summary: format!(
                "terminal partitions stayed at {} and non-terminal gap stayed at {} across passes {} -> {}",
                current.terminal_partition_count,
                current_gap,
                previous.completed_pass_count,
                current.completed_pass_count
            ),
        };
    }
    if improvements.is_empty() {
        return PlanningPassDiagnosis {
            verdict: PlanningConvergenceVerdict::NotConverging,
            evidence_summary: regressions.join("; "),
        };
    }
    if regressions.is_empty() {
        return PlanningPassDiagnosis {
            verdict: PlanningConvergenceVerdict::Converging,
            evidence_summary: improvements.join("; "),
        };
    }
    PlanningPassDiagnosis {
        verdict: PlanningConvergenceVerdict::Inconclusive,
        evidence_summary: format!("{}; {}", improvements.join("; "), regressions.join("; ")),
    }
}

fn planning_blocked_on_summary(status: &StreamingIndexingStatus) -> Option<String> {
    if !matches!(status.phase, StreamingIndexingPhase::PlanningPass { .. }) {
        return None;
    }
    let mut parts = Vec::new();
    let pending_partition_count = status
        .pending_partition_count
        .or_else(|| status.v2_pending_partitions.as_ref().map(Vec::len));
    if let Some(count) = pending_partition_count {
        parts.push(format!("{count} pending partition(s)"));
    }
    if let Some(partitions) = status.v2_pending_partitions.as_ref()
        && !partitions.is_empty()
    {
        let preview = partitions
            .iter()
            .take(2)
            .map(format_pending_partition_message)
            .collect::<Vec<_>>()
            .join(" | ");
        parts.push(format!("pending detail {preview}"));
        if partitions.len() > 2 {
            parts.push(format!(
                "+{} more pending partition(s)",
                partitions.len() - 2
            ));
        }
    }
    if let Some(stall) = status.suspected_stall.as_ref() {
        parts.push(format!(
            "suspected stall {} for {} ms",
            suspected_stall_reason_label(stall.reason),
            stall.duration_without_progress.as_millis()
        ));
    }
    if parts.is_empty() {
        Some(UNKNOWN_BLOCKED_ON_SUMMARY.to_string())
    } else {
        Some(parts.join("; "))
    }
}

fn planning_status_diagnosis_message(
    diagnosis: &PlanningPassDiagnosis,
    status: &StreamingIndexingStatus,
    blocked_on_summary: Option<&str>,
) -> Option<String> {
    let StreamingIndexingPhase::PlanningPass { pass_number } = status.phase else {
        return None;
    };
    let blocked_on_summary = blocked_on_summary?;
    Some(format!(
        "Planning diagnosis for pass {pass_number}: verdict {} ({}){}; blocked on {blocked_on_summary}",
        diagnosis.verdict.as_str(),
        diagnosis.evidence_summary,
        status
            .error
            .as_ref()
            .map(|error| format!("; delegated status error {error}"))
            .unwrap_or_default(),
    ))
}

fn planning_pending_partition_telemetry_record(
    partition: &StreamingV2PendingPartitionStatus,
) -> PlanningPendingPartitionTelemetryRecord {
    PlanningPendingPartitionTelemetryRecord {
        partition_path: partition.partition_path.clone(),
        expected_item_count: partition.expected_item_count,
        observed_replay_progress: partition.observed_replay_progress,
        routing_bucket_fill_counts: partition.routing_bucket_fill_counts.clone(),
        trainer_subphase: partition.trainer_subphase.map(trainer_subphase_label),
    }
}

fn report_planning_pass_completion(
    progress: &ProgressReporter,
    planning_telemetry: Option<&PlanningTelemetryContext>,
    pass_report: PlanningPassReport,
    completion: &PlanningCompletionAction,
) -> Result<(), RuntimeError> {
    if let Some(telemetry) = planning_telemetry {
        let (message, record) = telemetry.project_pass_summary(pass_report, completion);
        report_progress(progress, message);
        if let Err(error) = telemetry.write_json_record(&record) {
            report_progress(progress, error);
        }
    } else {
        report_progress(
            progress,
            format!(
                "Completed planning pass {} over {} item(s)",
                pass_report.completed_pass_count, pass_report.observed_item_count
            ),
        );
    }
    if let PlanningCompletionAction::ReplayRequired(message) = completion {
        report_progress(progress, message.clone());
    }
    Ok(())
}

fn handle_v2_planning_pass_completion(
    progress: &ProgressReporter,
    planning_telemetry: Option<&PlanningTelemetryContext>,
    pass_report: PlanningPassReport,
    result: Result<(), StreamingIndexerError>,
) -> Result<bool, RuntimeError> {
    let completion = v2_planning_completion_action(pass_report.completed_pass_count, result)
        .map_err(RuntimeError::StreamingIndexer)?;
    report_planning_pass_completion(progress, planning_telemetry, pass_report, &completion)?;
    match completion {
        PlanningCompletionAction::Complete => Ok(false),
        PlanningCompletionAction::ReplayRequired(_) => Ok(true),
    }
}

fn clustering_failure_input(item: &IndexItem<ContentRef>) -> ClusteringFailureInput {
    match &item.content_ref {
        ContentRef::Document { path } => {
            let source_path = normalize_document_identity_path(&path.to_string_lossy());
            ClusteringFailureInput::Document {
                logical_id: format!("document:{source_path}"),
                source_path,
            }
        }
        ContentRef::Inline { media_type, body } => {
            let input_hash = hash_embedding_content(media_type, body);
            ClusteringFailureInput::Inline {
                logical_id: format!("inline:{media_type}:{input_hash}"),
                media_type: media_type.clone(),
            }
        }
        ContentRef::StoredReplay { identity, .. } => match identity {
            ReplayIdentity::Document { source_path } => {
                let source_path = normalize_document_identity_path(source_path);
                ClusteringFailureInput::Document {
                    logical_id: format!("document:{source_path}"),
                    source_path,
                }
            }
            ReplayIdentity::EmailChunk {
                email_artifact_ref,
                chunk_index,
            } => ClusteringFailureInput::EmailChunk {
                logical_id: format!("email-chunk:{email_artifact_ref}:{chunk_index}"),
                email_artifact_ref: email_artifact_ref.clone(),
                chunk_index: *chunk_index,
            },
        },
        ContentRef::EmailChunk {
            email_artifact_ref,
            chunk_index,
        } => ClusteringFailureInput::EmailChunk {
            logical_id: format!("email-chunk:{email_artifact_ref}:{chunk_index}"),
            email_artifact_ref: email_artifact_ref.clone(),
            chunk_index: *chunk_index,
        },
    }
}

fn effective_clustering_diagnostics(
    clustering: &ConfiguredClustering,
) -> Option<EffectiveClusteringDiagnostics> {
    let profile = resolved_published_profile(clustering).ok()?;
    Some(EffectiveClusteringDiagnostics {
        profile_version: profile.version.to_string(),
        planning_algorithm_id: profile.planning_algorithm_id.to_string(),
        planning_direction: profile
            .planning_direction
            .map(published_planning_direction_name),
        packing_strategy_id: profile.packing_strategy_id.map(str::to_string),
        hierarchy_strategy_id: profile.hierarchy_strategy_id.to_string(),
        summary_policy_id: profile.summary_policy_id.to_string(),
        cluster_count: published_profile_cluster_count(&profile),
        random_seed: published_profile_random_seed(&profile),
    })
}

fn published_profile_cluster_count(profile: &PublishedIndexingProfile) -> Option<u32> {
    match &profile.planning_strategy {
        PublishedPlanningStrategy::SphericalKmeansGreedyPack(settings) => {
            Some(settings.cluster_count)
        }
        PublishedPlanningStrategy::DirectionalPcaDivisive(settings) => Some(settings.cluster_count),
    }
}

fn published_profile_random_seed(profile: &PublishedIndexingProfile) -> Option<u64> {
    match &profile.planning_strategy {
        PublishedPlanningStrategy::SphericalKmeansGreedyPack(settings) => settings.random_seed,
        PublishedPlanningStrategy::DirectionalPcaDivisive(settings) => settings.random_seed,
    }
}

fn published_planning_direction_name(direction: BuiltInPlanningDirection) -> String {
    match direction {
        BuiltInPlanningDirection::Divisive => "divisive",
        BuiltInPlanningDirection::Agglomerative => "agglomerative",
    }
    .to_string()
}

const SUSPICIOUS_INPUT_SAMPLE_LIMIT: usize = 5;
const EXTERNALIZED_CLUSTERING_DIAGNOSTIC_INPUT_LIMIT: usize = 1024;
const VARIANCE_EPSILON: f64 = 1e-12;

#[derive(Clone, Debug, Default)]
struct EmbeddingObservation {
    fingerprint: Option<String>,
    l2_norm: Option<f64>,
    content_fingerprint: Option<String>,
    missing: bool,
    lookup_error: Option<String>,
    undecodable: bool,
    non_finite: bool,
    zero_vector: bool,
}

fn build_embedding_health_diagnostics(
    resolver: &LocalFilesystemContentResolver,
    embedding_source: &dyn ClusteringFailureEmbeddingSource,
    replay_batches: &[ReplayBatch],
    inputs: &[ClusteringFailureInput],
    embedding_spec: &EmbeddingSpec,
) -> EmbeddingHealthDiagnostics {
    let mut available_embedding_count = 0usize;
    let mut missing_embedding_count = 0usize;
    let mut embedding_lookup_error_count = 0usize;
    let mut undecodable_embedding_count = 0usize;
    let mut non_finite_embedding_count = 0usize;
    let mut zero_vector_count = 0usize;
    let mut fingerprint_counts = HashMap::<String, usize>::new();
    let mut norm_sum = 0.0f64;
    let mut min_l2_norm = None::<f64>;
    let mut max_l2_norm = None::<f64>;
    let mut observations = Vec::with_capacity(inputs.len());

    let dimension_count = usize::try_from(embedding_spec.dims).ok();
    let mut component_sums = dimension_count.map(|dims| vec![0.0f64; dims]);
    let mut component_square_sums = dimension_count.map(|dims| vec![0.0f64; dims]);
    let mut finite_embedding_count = 0usize;

    for item in replay_batches.iter().flat_map(|batch| batch.items.iter()) {
        let Some(content) = resolver.resolve(&item.content_ref).ok() else {
            missing_embedding_count += 1;
            observations.push(EmbeddingObservation {
                missing: true,
                ..EmbeddingObservation::default()
            });
            continue;
        };
        let input_hash = hash_embedding_content(&content.media_type, &content.body);
        let content_fingerprint = Some(input_hash.to_string());
        let embedding_bytes = match embedding_source.embedding_for_hash(&input_hash.into_bytes()) {
            Ok(Some(embedding_bytes)) => embedding_bytes,
            Ok(None) => {
                missing_embedding_count += 1;
                observations.push(EmbeddingObservation {
                    content_fingerprint,
                    missing: true,
                    ..EmbeddingObservation::default()
                });
                continue;
            }
            Err(error) => {
                embedding_lookup_error_count += 1;
                observations.push(EmbeddingObservation {
                    content_fingerprint,
                    lookup_error: Some(error),
                    ..EmbeddingObservation::default()
                });
                continue;
            }
        };
        available_embedding_count += 1;

        let fingerprint = hash_bytes(&embedding_bytes).to_string();
        let decoded = match decode_embedding_values(&embedding_bytes, embedding_spec) {
            Some(values) => values,
            None => {
                undecodable_embedding_count += 1;
                observations.push(EmbeddingObservation {
                    fingerprint: Some(fingerprint),
                    content_fingerprint,
                    undecodable: true,
                    ..EmbeddingObservation::default()
                });
                continue;
            }
        };

        let non_finite = decoded.iter().any(|value| !value.is_finite());
        if non_finite {
            non_finite_embedding_count += 1;
            observations.push(EmbeddingObservation {
                fingerprint: Some(fingerprint),
                content_fingerprint,
                non_finite: true,
                ..EmbeddingObservation::default()
            });
            continue;
        }

        let l2_norm = decoded
            .iter()
            .map(|value| {
                let widened = f64::from(*value);
                widened * widened
            })
            .sum::<f64>()
            .sqrt();
        let zero_vector = l2_norm <= f64::EPSILON;
        if zero_vector {
            zero_vector_count += 1;
        }

        norm_sum += l2_norm;
        min_l2_norm = Some(min_l2_norm.map_or(l2_norm, |current| current.min(l2_norm)));
        max_l2_norm = Some(max_l2_norm.map_or(l2_norm, |current| current.max(l2_norm)));
        *fingerprint_counts.entry(fingerprint.clone()).or_insert(0) += 1;

        if let (Some(sums), Some(square_sums)) =
            (component_sums.as_mut(), component_square_sums.as_mut())
            && decoded.len() == sums.len()
        {
            for ((sum, square_sum), value) in sums
                .iter_mut()
                .zip(square_sums.iter_mut())
                .zip(decoded.iter())
            {
                let widened = f64::from(*value);
                *sum += widened;
                *square_sum += widened * widened;
            }
        }
        finite_embedding_count += 1;
        observations.push(EmbeddingObservation {
            fingerprint: Some(fingerprint),
            l2_norm: Some(l2_norm),
            content_fingerprint,
            zero_vector,
            ..EmbeddingObservation::default()
        });
    }

    let repeated_embedding_count = fingerprint_counts
        .values()
        .map(|count| count.saturating_sub(1))
        .sum();
    let unique_embedding_count = fingerprint_counts.len();
    let repeated_embedding_group_count = fingerprint_counts
        .values()
        .filter(|count| **count > 1)
        .count();
    let max_repeated_embedding_occurrence = fingerprint_counts
        .values()
        .copied()
        .filter(|count| *count > 1)
        .max();
    let mean_l2_norm =
        (finite_embedding_count > 0).then(|| norm_sum / finite_embedding_count as f64);

    let (non_zero_variance_dimension_count, max_component_variance) =
        if let (Some(sums), Some(square_sums)) =
            (component_sums.as_ref(), component_square_sums.as_ref())
        {
            if finite_embedding_count == 0 {
                (None, None)
            } else {
                let mut non_zero_count = 0usize;
                let mut max_variance = 0.0f64;
                for (sum, square_sum) in sums.iter().zip(square_sums.iter()) {
                    let mean = *sum / finite_embedding_count as f64;
                    let variance = (*square_sum / finite_embedding_count as f64) - (mean * mean);
                    let variance = variance.max(0.0);
                    if variance > VARIANCE_EPSILON {
                        non_zero_count += 1;
                    }
                    max_variance = max_variance.max(variance);
                }
                (Some(non_zero_count), Some(max_variance))
            }
        } else {
            (None, None)
        };
    let collapsed_variance_population =
        non_zero_variance_dimension_count.is_some_and(|count| count <= 1);

    let mut fingerprint_sample_inputs =
        HashMap::<String, Vec<RepeatedEmbeddingSampleDiagnostics>>::with_capacity(
            fingerprint_counts.len(),
        );
    for (input, observation) in inputs.iter().zip(observations.iter()) {
        let Some(fingerprint) = observation.fingerprint.as_ref() else {
            continue;
        };
        let sample_inputs = fingerprint_sample_inputs
            .entry(fingerprint.clone())
            .or_default();
        if sample_inputs.len() < SUSPICIOUS_INPUT_SAMPLE_LIMIT {
            sample_inputs.push(RepeatedEmbeddingSampleDiagnostics {
                input: input.clone(),
                content_fingerprint: observation.content_fingerprint.clone(),
            });
        }
    }
    let mut top_repeated_embedding_groups = fingerprint_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(fingerprint, count)| RepeatedEmbeddingGroupDiagnostics {
            embedding_fingerprint: fingerprint.clone(),
            occurrence_count: *count,
            sample_inputs: fingerprint_sample_inputs
                .remove(fingerprint)
                .unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    top_repeated_embedding_groups.sort_by(|left, right| {
        right
            .occurrence_count
            .cmp(&left.occurrence_count)
            .then_with(|| left.embedding_fingerprint.cmp(&right.embedding_fingerprint))
    });
    top_repeated_embedding_groups.truncate(SUSPICIOUS_INPUT_SAMPLE_LIMIT);

    let mut suspicious_input_sample = Vec::new();
    let mut embedding_lookup_error_sample = Vec::new();
    for (input, observation) in inputs.iter().zip(observations.iter()) {
        if let Some(error) = observation.lookup_error.as_ref()
            && embedding_lookup_error_sample.len() < SUSPICIOUS_INPUT_SAMPLE_LIMIT
        {
            embedding_lookup_error_sample.push(EmbeddingLookupErrorDiagnostics {
                input: input.clone(),
                content_fingerprint: observation.content_fingerprint.clone(),
                error: error.clone(),
            });
        }
        let mut reasons = Vec::new();
        if observation.missing {
            reasons.push("missing-embedding".to_string());
        }
        if observation.lookup_error.is_some() {
            reasons.push("embedding-lookup-error".to_string());
        }
        if observation.undecodable {
            reasons.push("undecodable-embedding".to_string());
        }
        if observation.non_finite {
            reasons.push("non-finite-embedding".to_string());
        }
        if observation.zero_vector {
            reasons.push("zero-vector".to_string());
        }
        if observation
            .fingerprint
            .as_ref()
            .and_then(|fingerprint| fingerprint_counts.get(fingerprint))
            .is_some_and(|count| *count > 1)
        {
            reasons.push("repeated-embedding".to_string());
        }
        if reasons.is_empty()
            && collapsed_variance_population
            && observation.fingerprint.is_some()
            && observation.l2_norm.is_some()
        {
            reasons.push("collapsed-variance-population".to_string());
        }
        if reasons.is_empty() {
            continue;
        }
        suspicious_input_sample.push(SuspiciousClusteringFailureInput {
            input: input.clone(),
            reasons,
            embedding_fingerprint: observation.fingerprint.clone(),
            l2_norm: observation.l2_norm,
        });
        if suspicious_input_sample.len() >= SUSPICIOUS_INPUT_SAMPLE_LIMIT {
            break;
        }
    }

    EmbeddingHealthDiagnostics {
        available_embedding_count,
        missing_embedding_count,
        embedding_lookup_error_count,
        undecodable_embedding_count,
        non_finite_embedding_count,
        zero_vector_count,
        repeated_embedding_count,
        unique_embedding_count,
        repeated_embedding_group_count,
        max_repeated_embedding_occurrence,
        min_l2_norm,
        max_l2_norm,
        mean_l2_norm,
        non_zero_variance_dimension_count,
        max_component_variance,
        top_repeated_embedding_groups,
        embedding_lookup_error_sample,
        suspicious_input_sample,
    }
}

fn failing_subset_phase_diagnostics(
    phase: &StreamingIndexingPhase,
) -> FailingSubsetPhaseDiagnostics {
    match phase {
        StreamingIndexingPhase::PlanningPass { pass_number } => {
            FailingSubsetPhaseDiagnostics::PlanningPass {
                pass_number: *pass_number,
            }
        }
        StreamingIndexingPhase::HierarchyPlanning { stage } => {
            FailingSubsetPhaseDiagnostics::HierarchyPlanning {
                stage: format_planning_stage(*stage).to_string(),
            }
        }
        StreamingIndexingPhase::FinalMaterializationReplay => {
            FailingSubsetPhaseDiagnostics::FinalMaterializationReplay
        }
        StreamingIndexingPhase::BottomUpAssembly { layer_index } => {
            FailingSubsetPhaseDiagnostics::BottomUpAssembly {
                layer_index: *layer_index,
            }
        }
    }
}

fn build_failing_subset_diagnostics(
    status: &StreamingIndexingStatus,
    top_level_input_count: usize,
    embedding_health: &EmbeddingHealthDiagnostics,
) -> FailingSubsetDiagnostics {
    let exact_top_level_match = status.item_count == top_level_input_count;
    let (provenance, basis) = if exact_top_level_match {
        (
            FailingSubsetProvenance::Exact,
            "the upstream failure surface reported the same active item count as the top-level clustering attempt, so the count-based repository-visible subset matches the top-level attempt".to_string(),
        )
    } else {
        (
            FailingSubsetProvenance::NarrowestProvable,
            format!(
                "the upstream failure surface reported {} active item(s) for the failing step but did not expose repository-visible identities for a narrower subset, so the top-level clustering attempt remains the narrowest provable repository-visible subset",
                status.item_count
            ),
        )
    };
    FailingSubsetDiagnostics {
        phase: failing_subset_phase_diagnostics(&status.phase),
        provenance,
        basis,
        upstream_active_item_count: status.item_count,
        upstream_completed_unit_count: status.completed_unit_count,
        upstream_phase_total_unit_count: status.phase_total_unit_count,
        repository_visible_subset: RepositoryVisibleSubsetDiagnostics::SameAsTopLevelAttempt {
            input_count: top_level_input_count,
        },
        embedding_health: embedding_health.clone(),
    }
}

fn build_clustering_failure_diagnostics(
    resolver: &LocalFilesystemContentResolver,
    embedding_source: &dyn ClusteringFailureEmbeddingSource,
    failing_status: Option<&StreamingIndexingStatus>,
    config: &StreamingStageConfig,
    replay_batches: &[ReplayBatch],
    embedding_spec: &EmbeddingSpec,
) -> Option<ClusteringFailureDiagnostics> {
    let inputs = replay_batches
        .iter()
        .flat_map(|batch| batch.items.iter().map(clustering_failure_input))
        .collect::<Vec<_>>();
    let input_count = inputs.len();
    let clustering = effective_clustering_diagnostics(&config.clustering)?;
    let embedding_health = build_embedding_health_diagnostics(
        resolver,
        embedding_source,
        replay_batches,
        &inputs,
        embedding_spec,
    );
    let failing_subset = failing_status
        .map(|status| build_failing_subset_diagnostics(status, input_count, &embedding_health));
    Some(ClusteringFailureDiagnostics {
        stage: config.stage,
        embedding_spec: ClusteringFailureEmbeddingSpec {
            dims: embedding_spec.dims,
            encoding: embedding_spec.encoding.clone(),
        },
        block_size_target: config.block_size_target,
        clustering,
        embedding_health,
        failing_subset,
        input_count,
        inputs,
    })
}

fn build_externalized_clustering_failure_diagnostics(
    resolver: &LocalFilesystemContentResolver,
    embedding_source: &dyn ClusteringFailureEmbeddingSource,
    failing_status: Option<&StreamingIndexingStatus>,
    config: &StreamingStageConfig,
    replay_state: &ExternalizedReplayState,
    embedding_spec: &EmbeddingSpec,
) -> Option<ClusteringFailureDiagnostics> {
    if replay_state.total_items > EXTERNALIZED_CLUSTERING_DIAGNOSTIC_INPUT_LIMIT {
        return None;
    }
    let replay_batches = replay_state.collect_replay_batches().ok()?;
    build_clustering_failure_diagnostics(
        resolver,
        embedding_source,
        failing_status,
        config,
        &replay_batches,
        embedding_spec,
    )
}

fn format_clustering_failure_diagnostics(
    diagnostics: &ClusteringFailureDiagnostics,
) -> Result<String, serde_json::Error> {
    Ok(format!(
        "Clustering failure diagnostics:\n{}",
        serde_json::to_string_pretty(diagnostics)?
    ))
}

fn clustering_failure_error(
    source: StreamingIndexerError,
    diagnostics: Option<&ClusteringFailureDiagnostics>,
    progress: &ProgressReporter,
) -> RuntimeError {
    if let Some(diagnostics) = diagnostics {
        match format_clustering_failure_diagnostics(diagnostics) {
            Ok(message) => report_progress(progress, message),
            Err(error) => report_progress(
                progress,
                format!(
                    "Clustering failure diagnostics were available but could not be rendered: {error}"
                ),
            ),
        }
        RuntimeError::ClusteringFailure {
            source,
            diagnostics: Box::new(diagnostics.clone()),
        }
    } else {
        RuntimeError::StreamingIndexer(source)
    }
}

fn persist_clustering_failure_diagnostics(
    diagnostics_path: Option<&Path>,
    error: &RuntimeError,
    progress: &ProgressReporter,
) {
    let Some(diagnostics) = error.clustering_failure_diagnostics() else {
        return;
    };
    let Some(path) = diagnostics_path else {
        return;
    };
    match write_clustering_failure_diagnostics_file(path, diagnostics) {
        Ok(()) => report_progress(
            progress,
            format!("Wrote clustering failure diagnostics to {}", path.display()),
        ),
        Err(write_error) => report_progress(
            progress,
            format!(
                "Failed to write clustering failure diagnostics to {}: {write_error}",
                path.display()
            ),
        ),
    }
}

#[cfg(test)]
fn serialized_branch_size(
    embedding_spec: &EmbeddingSpec,
    entry_count: usize,
) -> Result<usize, AutoSizingBuiltInPlanningError> {
    if entry_count < 2 {
        return Err(AutoSizingBuiltInPlanningError::DeriveClusterCount(
            "branch-size estimation requires at least two entries".into(),
        ));
    }

    let embedding_len = expected_embedding_len(embedding_spec)?;
    let top_level_size = cbor_map_size(4)
        + cbor_unsigned_field_size(0, VERSION_1)
        + cbor_unsigned_field_size(1, 1)
        + cbor_key_size(2)
        + embedding_spec_cbor_size(embedding_spec)
        + cbor_key_size(3)
        + cbor_array_size(entry_count);
    let entry_size = cbor_map_size(2)
        + cbor_key_size(0)
        + cbor_bytes_size(embedding_len)
        + cbor_key_size(1)
        + cbor_bytes_size(BlockHash::LEN);

    top_level_size
        .checked_add(entry_size.checked_mul(entry_count).ok_or_else(|| {
            AutoSizingBuiltInPlanningError::DeriveClusterCount(format!(
                "branch-size estimation overflow for {entry_count} entries"
            ))
        })?)
        .ok_or_else(|| {
            AutoSizingBuiltInPlanningError::DeriveClusterCount(format!(
                "branch-size estimation overflow for {entry_count} entries"
            ))
        })
}

#[cfg(test)]
fn expected_embedding_len(
    embedding_spec: &EmbeddingSpec,
) -> Result<usize, AutoSizingBuiltInPlanningError> {
    let scalar_width = match embedding_spec.encoding.as_str() {
        "f32le" => 4_u64,
        "f16le" => 2_u64,
        other => {
            return Err(AutoSizingBuiltInPlanningError::DeriveClusterCount(format!(
                "unsupported embedding encoding {other:?} for branch-size estimation"
            )));
        }
    };
    embedding_spec
        .dims
        .checked_mul(scalar_width)
        .and_then(|length| usize::try_from(length).ok())
        .ok_or_else(|| {
            AutoSizingBuiltInPlanningError::DeriveClusterCount(format!(
                "embedding length overflow for {} dimensions with encoding {:?}",
                embedding_spec.dims, embedding_spec.encoding
            ))
        })
}

#[cfg(test)]
fn embedding_spec_cbor_size(embedding_spec: &EmbeddingSpec) -> usize {
    cbor_map_size(2)
        + cbor_unsigned_field_size(0, embedding_spec.dims)
        + cbor_key_size(1)
        + cbor_text_size(&embedding_spec.encoding)
}

#[cfg(test)]
fn cbor_unsigned_field_size(key: u64, value: u64) -> usize {
    cbor_key_size(key) + cbor_unsigned_size(value)
}

#[cfg(test)]
fn cbor_key_size(key: u64) -> usize {
    cbor_unsigned_size(key)
}

#[cfg(test)]
fn cbor_map_size(entry_count: usize) -> usize {
    cbor_major_size(entry_count)
}

#[cfg(test)]
fn cbor_array_size(entry_count: usize) -> usize {
    cbor_major_size(entry_count)
}

#[cfg(test)]
fn cbor_text_size(value: &str) -> usize {
    cbor_major_size(value.len()) + value.len()
}

#[cfg(test)]
fn cbor_bytes_size(byte_len: usize) -> usize {
    cbor_major_size(byte_len) + byte_len
}

#[cfg(test)]
fn cbor_unsigned_size(value: u64) -> usize {
    match value {
        0..=23 => 1,
        24..=0xff => 2,
        0x100..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
    }
}

#[cfg(test)]
fn cbor_major_size(value: usize) -> usize {
    match value {
        0..=23 => 1,
        24..=0xff => 2,
        0x100..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
    }
}

pub async fn run_request_file(request_path: &Path) -> Result<BatchSummary, RuntimeError> {
    run_request_file_with_overrides(request_path, None, ClusteringConfigOverrides::default()).await
}

pub async fn run_request_file_with_stage(
    request_path: &Path,
    stage_override: Option<ExecutionStage>,
) -> Result<BatchSummary, RuntimeError> {
    run_request_file_with_overrides(
        request_path,
        stage_override,
        ClusteringConfigOverrides::default(),
    )
    .await
}

pub async fn run_request_file_with_overrides(
    request_path: &Path,
    stage_override: Option<ExecutionStage>,
    clustering_overrides: ClusteringConfigOverrides,
) -> Result<BatchSummary, RuntimeError> {
    run_request_file_with_outputs(request_path, stage_override, clustering_overrides, None).await
}

pub async fn run_request_file_with_outputs(
    request_path: &Path,
    stage_override: Option<ExecutionStage>,
    clustering_overrides: ClusteringConfigOverrides,
    summary_out: Option<&Path>,
) -> Result<BatchSummary, RuntimeError> {
    let bytes = fs::read(request_path).map_err(|source| RuntimeError::ReadRequest {
        path: request_path.display().to_string(),
        source,
    })?;
    let mut request: BatchRequest =
        serde_json::from_slice(&bytes).map_err(|source| RuntimeError::ParseRequest {
            path: request_path.display().to_string(),
            source,
        })?;
    if let Some(stage) = stage_override {
        request.stage = stage;
    }
    let request_dir = request_path.parent().unwrap_or_else(|| Path::new("."));
    let diagnostics_path = clustering_failure_diagnostics_path(request_path, summary_out);
    let planning_pass_path = planning_pass_telemetry_path(request_path, summary_out);
    let replay_order_scratch_root = replay_order_scratch_root_path(request_path, summary_out);
    let planner_state_root = planner_state_root_path(request_path, summary_out);

    run_request_with_progress(
        request_dir,
        request,
        clustering_overrides,
        RunRequestArtifactPaths {
            diagnostics_path: Some(diagnostics_path.as_path()),
            planning_pass_telemetry_path: Some(planning_pass_path.as_path()),
            replay_order_scratch_root: Some(replay_order_scratch_root.as_path()),
            planner_state_root: Some(planner_state_root.as_path()),
        },
        |message| {
            eprintln!("{message}");
        },
    )
    .await
}

pub async fn validate_request_file_with_overrides(
    request_path: &Path,
    stage_override: Option<ExecutionStage>,
    clustering_overrides: ClusteringConfigOverrides,
    summary_out: Option<&Path>,
) -> Result<(), RuntimeError> {
    let bytes = fs::read(request_path).map_err(|source| RuntimeError::ReadRequest {
        path: request_path.display().to_string(),
        source,
    })?;
    let mut request: BatchRequest =
        serde_json::from_slice(&bytes).map_err(|source| RuntimeError::ParseRequest {
            path: request_path.display().to_string(),
            source,
        })?;
    if let Some(stage) = stage_override {
        request.stage = stage;
    }
    let request_dir = request_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let replay_order_scratch_root = replay_order_scratch_root_path(request_path, summary_out);
    let planner_state_root = planner_state_root_path(request_path, summary_out);
    tokio::task::spawn_blocking(move || {
        validate_request_with_overrides(
            &request_dir,
            request,
            clustering_overrides,
            Some(replay_order_scratch_root.as_path()),
            Some(planner_state_root.as_path()),
        )
    })
    .await
    .map_err(RuntimeError::BlockingMutableRefTaskJoin)?
}

pub async fn run_request(
    request_dir: &Path,
    request: BatchRequest,
) -> Result<BatchSummary, RuntimeError> {
    run_request_with_overrides(request_dir, request, ClusteringConfigOverrides::default()).await
}

pub async fn run_request_with_overrides(
    request_dir: &Path,
    request: BatchRequest,
    clustering_overrides: ClusteringConfigOverrides,
) -> Result<BatchSummary, RuntimeError> {
    run_request_with_progress(
        request_dir,
        request,
        clustering_overrides,
        RunRequestArtifactPaths::default(),
        |message| eprintln!("{message}"),
    )
    .await
}

#[derive(Clone, Copy, Debug, Default)]
struct RunRequestArtifactPaths<'a> {
    diagnostics_path: Option<&'a Path>,
    planning_pass_telemetry_path: Option<&'a Path>,
    replay_order_scratch_root: Option<&'a Path>,
    planner_state_root: Option<&'a Path>,
}

fn validate_request_with_overrides(
    request_dir: &Path,
    request: BatchRequest,
    clustering_overrides: ClusteringConfigOverrides,
    replay_order_scratch_root: Option<&Path>,
    planner_state_root: Option<&Path>,
) -> Result<(), RuntimeError> {
    request.validate()?;
    let clustering = clustering_overrides
        .to_configured_clustering(request.profile_version, &request.environment)?;
    let stage = request.stage;
    let _block_store = ConfiguredBlockStore::from_environment(request_dir, &request.environment)?;
    let mutable_ref_store = request
        .environment
        .resolve_mutable_ref_store(request_dir, &request.ref_name);

    if stage.includes_clustering() {
        let replay_order_scratch_root = replay_order_scratch_root
            .map(Path::to_path_buf)
            .unwrap_or_else(|| replay_order_scratch_root_for_request_dir(request_dir));
        prepare_replay_order_scratch_root(&replay_order_scratch_root)?;
        if uses_streaming_indexer_v2(&clustering) {
            let planner_state_root = planner_state_root
                .map(Path::to_path_buf)
                .unwrap_or_else(|| planner_state_root_for_request_dir(request_dir));
            prepare_planner_state_root(&planner_state_root)?;
            let _: StreamingIndexingRunV2<ContentRef, _, _> =
                StreamingIndexingRunV2::with_published_profile(
                    ValidateOnlyResolver,
                    ValidateOnlyEmbeddingProvider,
                    clustering.profile_version,
                    request.to_embedding_spec(),
                    request.block_size_target,
                    planner_state_root.as_path(),
                )?;
        } else {
            let profile = resolved_published_profile(&clustering)?;
            let _: StreamingIndexingRun<ContentRef, _, _, _, _> =
                StreamingIndexingRun::with_resolved_published_profile(
                    ValidateOnlyResolver,
                    ValidateOnlyEmbeddingProvider,
                    profile,
                    request.to_embedding_spec(),
                    request.block_size_target,
                )?;
        }
    }

    if stage == ExecutionStage::ClusteringAndBlockAssembly {
        let Some(mutable_ref_store) = mutable_ref_store.as_ref() else {
            return Err(RuntimeError::MissingReplayJournalHead {
                path: "<unresolved mutable ref>".into(),
            });
        };
        let refs = load_mutable_ref_store(mutable_ref_store)?;
        if refs.replay_journal_head_block_id.is_none() {
            return Err(RuntimeError::MissingReplayJournalHead {
                path: mutable_ref_store_label(mutable_ref_store),
            });
        }
    }

    Ok(())
}

async fn run_request_with_progress<F>(
    request_dir: &Path,
    request: BatchRequest,
    clustering_overrides: ClusteringConfigOverrides,
    artifact_paths: RunRequestArtifactPaths<'_>,
    progress: F,
) -> Result<BatchSummary, RuntimeError>
where
    F: Fn(String) + Send + Sync + 'static,
{
    let progress: ProgressReporter = Arc::new(progress);
    request.validate()?;
    let clustering = clustering_overrides
        .to_configured_clustering(request.profile_version, &request.environment)?;
    let stage = request.stage;
    let block_store = ConfiguredBlockStore::from_environment(request_dir, &request.environment)?;
    let mutable_ref_store = request
        .environment
        .resolve_mutable_ref_store(request_dir, &request.ref_name);
    let mutable_ref_metadata = mutable_ref_store_metadata(stage, &clustering);
    let replay_order_scratch_root = if stage.includes_clustering() {
        let path = artifact_paths
            .replay_order_scratch_root
            .map(Path::to_path_buf)
            .unwrap_or_else(|| replay_order_scratch_root_for_request_dir(request_dir));
        prepare_replay_order_scratch_root(&path)?;
        Some(path)
    } else {
        None
    };
    let planner_state_root =
        if stage.includes_clustering() && uses_streaming_indexer_v2(&clustering) {
            let path = artifact_paths
                .planner_state_root
                .map(Path::to_path_buf)
                .unwrap_or_else(|| planner_state_root_for_request_dir(request_dir));
            prepare_planner_state_root(&path)?;
            Some(path)
        } else {
            None
        };
    let planning_telemetry = stage
        .includes_clustering()
        .then(|| PlanningTelemetryContext {
            run_identity: planning_run_identity(&clustering),
            sink_path: artifact_paths
                .planning_pass_telemetry_path
                .map(Path::to_path_buf),
            sink_initialized: Arc::new(AtomicBool::new(false)),
            sink_write_lock: Arc::new(Mutex::new(())),
            diagnosis_state: Arc::new(Mutex::new(PlanningTelemetryState::default())),
        });
    let embedding_spec = request.to_embedding_spec();
    let resolver = LocalFilesystemContentResolver::new(block_store.clone());
    let max_concurrency = request.effective_max_concurrency();
    let replay_batch_size = request.effective_replay_batch_size();
    if let Some(planning_telemetry) = planning_telemetry.as_ref() {
        report_progress(&progress, planning_telemetry.bootstrap_message());
    }
    let io = RuntimeIo {
        mutable_ref_store: mutable_ref_store.as_ref(),
        mutable_ref_metadata: mutable_ref_store.as_ref().map(|_| &mutable_ref_metadata),
        planning_telemetry: planning_telemetry.as_ref(),
        progress: &progress,
    };

    if stage.includes_ingestion()
        && let Some(mutable_ref_store) = io.mutable_ref_store
    {
        prepare_mutable_ref_store_async(mutable_ref_store.clone()).await?;
    }

    if stage == ExecutionStage::IngestionAndEmbedding {
        request.environment.local_embedding()?;
        let embedding_provider =
            ConfiguredEmbeddingProvider::from_environment(&request.environment)?;
        let replay_batches = prepare_request_replay_batches(
            request_dir,
            &request,
            &block_store,
            max_concurrency,
            &progress,
        )?;
        return run_ingestion_only_stage(
            &block_store,
            resolver,
            embedding_provider,
            replay_batches,
            &embedding_spec,
            max_concurrency,
            io,
        )
        .await;
    }

    let result = if stage.includes_ingestion() {
        request.environment.local_embedding()?;
        stream_request_ingestion_to_store(
            request_dir,
            &request,
            &block_store,
            resolver.clone(),
            ConfiguredEmbeddingProvider::from_environment(&request.environment)?,
            &embedding_spec,
            max_concurrency,
            io,
        )
        .await?;
        let Some(mutable_ref_store) = mutable_ref_store.clone() else {
            return Err(RuntimeError::MissingReplayJournalHead {
                path: "<unresolved mutable ref>".into(),
            });
        };
        let (replay_state, embedding_provider) = externalize_replay_batches_from_store_async(
            block_store.clone(),
            embedding_spec.clone(),
            replay_batch_size,
            max_concurrency,
            mutable_ref_store,
            replay_order_scratch_root
                .clone()
                .ok_or(RuntimeError::MissingReplayOrderScratchRoot)?,
            Arc::clone(&progress),
        )
        .await?;
        run_streaming_stage_externalized(
            resolver,
            embedding_provider,
            StreamingStageConfig {
                stage,
                clustering,
                block_size_target: request.block_size_target,
                submission_progress_kind: SubmissionProgressKind::Replay,
                planner_state_root: planner_state_root.clone(),
            },
            replay_state,
            &block_store,
            &embedding_spec,
            io,
        )
        .await
    } else {
        let Some(mutable_ref_store) = mutable_ref_store.clone() else {
            return Err(RuntimeError::MissingReplayJournalHead {
                path: "<unresolved mutable ref>".into(),
            });
        };
        let (replay_state, embedding_provider) = externalize_replay_batches_from_store_async(
            block_store.clone(),
            embedding_spec.clone(),
            replay_batch_size,
            max_concurrency,
            mutable_ref_store,
            replay_order_scratch_root
                .clone()
                .ok_or(RuntimeError::MissingReplayOrderScratchRoot)?,
            Arc::clone(&progress),
        )
        .await?;
        run_streaming_stage_externalized(
            resolver,
            embedding_provider,
            StreamingStageConfig {
                stage,
                clustering,
                block_size_target: request.block_size_target,
                submission_progress_kind: SubmissionProgressKind::Replay,
                planner_state_root: planner_state_root.clone(),
            },
            replay_state,
            &block_store,
            &embedding_spec,
            io,
        )
        .await
    };

    if let Err(error) = &result {
        persist_clustering_failure_diagnostics(artifact_paths.diagnostics_path, error, &progress);
    }
    result
}

async fn run_ingestion_only_stage(
    block_store: &ConfiguredBlockStore,
    resolver: LocalFilesystemContentResolver,
    embedding_provider: ConfiguredEmbeddingProvider,
    replay_batches: Vec<ReplayBatch>,
    embedding_spec: &EmbeddingSpec,
    max_concurrency: usize,
    io: RuntimeIo<'_>,
) -> Result<BatchSummary, RuntimeError> {
    let mut staged = StagedBlocks::default();
    let total_batches = replay_batches.len();
    let total_items: usize = replay_batches.iter().map(|batch| batch.items.len()).sum();
    let mut completed_items = 0usize;
    for (batch_index, batch) in replay_batches.into_iter().enumerate() {
        let batch_number = batch_index + 1;
        let batch_item_count = batch.items.len();
        report_progress(
            io.progress,
            format!(
                "Embedding batch {batch_number} of {total_batches} started for {batch_item_count} delegated item(s); completed {completed_items} of {total_items} delegated item(s)"
            ),
        );
        let constructed = build_leaf_blocks_concurrently(
            resolver.clone(),
            embedding_provider.clone(),
            &batch.items,
            embedding_spec,
            max_concurrency,
        );
        let constructed = await_with_periodic_progress(
            constructed,
            io.progress,
            PROGRESS_HEARTBEAT_INTERVAL,
            |elapsed| {
                format!(
                    "Embedding batch {batch_number} of {total_batches} still running after {} ms for {batch_item_count} delegated item(s); completed {completed_items} of {total_items} delegated item(s)",
                    elapsed.as_millis()
                )
            },
        )
        .await?;
        persist_staged_blocks(&constructed.blocks, block_store)?;
        if let Some(mutable_ref_store) = io.mutable_ref_store {
            let records = batch
                .items
                .iter()
                .zip(constructed.block_ids.iter().copied())
                .map(|(item, block_id)| replay_journal_record_from_item(block_id, item))
                .collect::<Vec<_>>();
            let replay_journal_head_block_id = append_replay_journal_records_async(
                block_store.clone(),
                mutable_ref_store.clone(),
                records,
            )
            .await?;
            update_mutable_ref_store_async(
                mutable_ref_store.clone(),
                MutableRefStoreUpdate {
                    replay_journal_head_block_id,
                    metadata: io.mutable_ref_metadata.cloned(),
                    ..MutableRefStoreUpdate::default()
                },
            )
            .await?;
        }
        completed_items += batch_item_count;
        if let Some(message) = batch.completion_message {
            report_progress(
                io.progress,
                format!("{message} into {} leaf block(s)", constructed.blocks.len()),
            );
        }
        staged.extend_constructed(&constructed);
    }
    report_progress(
        io.progress,
        format!(
            "Skipped clustering and block assembly; returning placeholder root_id {}",
            placeholder_root_id()
        ),
    );
    Ok(staged.into_summary(placeholder_root_id()))
}

fn for_each_request_replay_item(
    request_dir: &Path,
    request: &BatchRequest,
    block_store: &ConfiguredBlockStore,
    progress: &ProgressReporter,
    mut visit: impl FnMut(IndexItem<ContentRef>) -> Result<(), RuntimeError>,
) -> Result<usize, RuntimeError> {
    let mut total_items = 0usize;

    let document_items = request.to_document_index_items(request_dir);
    if !document_items.is_empty() {
        report_progress(
            progress,
            format!(
                "Preparing {} document item(s) for delegated indexing",
                document_items.len()
            ),
        );
        for item in document_items {
            visit(item)?;
            total_items += 1;
        }
        report_progress(
            progress,
            format!("Prepared {total_items} delegated item(s) so far"),
        );
    }

    for item in &request.items {
        if let BatchItemConfig::Mailbox { path, metadata } = item {
            let resolved = resolve_path(request_dir, path);
            report_progress(
                progress,
                format!("Processing mailbox {}", resolved.display()),
            );
            let expansion = match expand_mailbox_item_with_stats(&resolved, metadata, block_store) {
                Ok(expansion) => expansion,
                Err(MailboxExpansionError::EmptyMailbox { .. }) => {
                    report_progress(
                        progress,
                        format!(
                            "Skipping empty mailbox {}; prepared 0 delegated item(s)",
                            resolved.display()
                        ),
                    );
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            report_progress(
                progress,
                format!(
                    "Processed mailbox {}: {} message(s), {} delegated item(s)",
                    resolved.display(),
                    expansion.message_count,
                    expansion.items.len()
                ),
            );
            for mailbox_item in expansion.items {
                visit(mailbox_item)?;
                total_items += 1;
            }
            report_progress(
                progress,
                format!(
                    "Prepared {total_items} delegated item(s) after mailbox {}",
                    resolved.display()
                ),
            );
        }
    }

    Ok(total_items)
}

#[allow(clippy::too_many_arguments)]
async fn ingest_replay_batch_to_store(
    block_store: &ConfiguredBlockStore,
    resolver: LocalFilesystemContentResolver,
    embedding_provider: ConfiguredEmbeddingProvider,
    embedding_spec: &EmbeddingSpec,
    max_concurrency: usize,
    io: RuntimeIo<'_>,
    batch_number: usize,
    batch_items: Vec<IndexItem<ContentRef>>,
    completed_items: usize,
) -> Result<ConstructedBlocks, RuntimeError> {
    let batch_item_count = batch_items.len();
    report_progress(
        io.progress,
        format!(
            "Embedding batch {batch_number} started for {batch_item_count} delegated item(s); completed {completed_items} delegated item(s)"
        ),
    );
    let constructed = build_leaf_blocks_concurrently(
        resolver,
        embedding_provider,
        &batch_items,
        embedding_spec,
        max_concurrency,
    );
    let constructed = await_with_periodic_progress(
        constructed,
        io.progress,
        PROGRESS_HEARTBEAT_INTERVAL,
        |elapsed| {
            format!(
                "Embedding batch {batch_number} still running after {} ms for {batch_item_count} delegated item(s); completed {completed_items} delegated item(s)",
                elapsed.as_millis()
            )
        },
    )
    .await?;
    persist_staged_blocks(&constructed.blocks, block_store)?;
    if let Some(mutable_ref_store) = io.mutable_ref_store {
        let records = batch_items
            .iter()
            .zip(constructed.block_ids.iter().copied())
            .map(|(item, block_id)| replay_journal_record_from_item(block_id, item))
            .collect::<Vec<_>>();
        let replay_journal_head_block_id = append_replay_journal_records_async(
            block_store.clone(),
            mutable_ref_store.clone(),
            records,
        )
        .await?;
        update_mutable_ref_store_async(
            mutable_ref_store.clone(),
            MutableRefStoreUpdate {
                replay_journal_head_block_id,
                metadata: io.mutable_ref_metadata.cloned(),
                ..MutableRefStoreUpdate::default()
            },
        )
        .await?;
    }
    report_progress(
        io.progress,
        format!(
            "Embedded batch {batch_number}; completed {} delegated item(s) into {} leaf block(s)",
            completed_items + batch_item_count,
            constructed.blocks.len()
        ),
    );
    Ok(constructed)
}

#[allow(clippy::too_many_arguments)]
async fn stream_request_ingestion_to_store(
    request_dir: &Path,
    request: &BatchRequest,
    block_store: &ConfiguredBlockStore,
    resolver: LocalFilesystemContentResolver,
    embedding_provider: ConfiguredEmbeddingProvider,
    embedding_spec: &EmbeddingSpec,
    max_concurrency: usize,
    io: RuntimeIo<'_>,
) -> Result<(), RuntimeError> {
    let chunk_size = max_concurrency.max(1);
    let mut buffered_items = Vec::with_capacity(chunk_size);
    let mut completed_items = 0usize;
    let mut batch_number = 0usize;
    let mut total_items = 0usize;
    let mut staged = StagedBlocks::default();

    let document_items = request.to_document_index_items(request_dir);
    if !document_items.is_empty() {
        report_progress(
            io.progress,
            format!(
                "Preparing {} document item(s) for streaming delegated indexing",
                document_items.len()
            ),
        );
        for item in document_items {
            buffered_items.push(item);
            total_items += 1;
            if buffered_items.len() < chunk_size {
                continue;
            }
            batch_number += 1;
            let batch_items = std::mem::take(&mut buffered_items);
            let batch_item_count = batch_items.len();
            let constructed = ingest_replay_batch_to_store(
                block_store,
                resolver.clone(),
                embedding_provider.clone(),
                embedding_spec,
                max_concurrency,
                io,
                batch_number,
                batch_items,
                completed_items,
            )
            .await?;
            completed_items += batch_item_count;
            staged.extend_constructed(&constructed);
        }
    }

    for item in &request.items {
        if let BatchItemConfig::Mailbox { path, metadata } = item {
            let resolved = resolve_path(request_dir, path);
            report_progress(
                io.progress,
                format!("Processing mailbox {}", resolved.display()),
            );
            let expansion = match expand_mailbox_item_with_stats(&resolved, metadata, block_store) {
                Ok(expansion) => expansion,
                Err(MailboxExpansionError::EmptyMailbox { .. }) => {
                    report_progress(
                        io.progress,
                        format!(
                            "Skipping empty mailbox {}; prepared 0 delegated item(s)",
                            resolved.display()
                        ),
                    );
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            report_progress(
                io.progress,
                format!(
                    "Processed mailbox {}: {} message(s), {} delegated item(s)",
                    resolved.display(),
                    expansion.message_count,
                    expansion.items.len()
                ),
            );
            report_progress(
                io.progress,
                format!(
                    "Prepared {} delegated item(s) from mailbox {}",
                    expansion.items.len(),
                    resolved.display()
                ),
            );
            for mailbox_item in expansion.items {
                buffered_items.push(mailbox_item);
                total_items += 1;
                if buffered_items.len() < chunk_size {
                    continue;
                }
                batch_number += 1;
                let batch_items = std::mem::take(&mut buffered_items);
                let batch_item_count = batch_items.len();
                let constructed = ingest_replay_batch_to_store(
                    block_store,
                    resolver.clone(),
                    embedding_provider.clone(),
                    embedding_spec,
                    max_concurrency,
                    io,
                    batch_number,
                    batch_items,
                    completed_items,
                )
                .await?;
                completed_items += batch_item_count;
                staged.extend_constructed(&constructed);
            }
        }
    }

    if !buffered_items.is_empty() {
        batch_number += 1;
        let batch_items = std::mem::take(&mut buffered_items);
        let batch_item_count = batch_items.len();
        let constructed = ingest_replay_batch_to_store(
            block_store,
            resolver.clone(),
            embedding_provider.clone(),
            embedding_spec,
            max_concurrency,
            io,
            batch_number,
            batch_items,
            completed_items,
        )
        .await?;
        completed_items += batch_item_count;
        staged.extend_constructed(&constructed);
    }
    debug_assert_eq!(completed_items, total_items);
    report_progress(
        io.progress,
        format!(
            "Completed streaming ingestion for {total_items} delegated item(s) into {} leaf block(s)",
            staged.blocks.len()
        ),
    );
    Ok(())
}

fn prepare_request_replay_batches(
    request_dir: &Path,
    request: &BatchRequest,
    block_store: &ConfiguredBlockStore,
    max_concurrency: usize,
    progress: &ProgressReporter,
) -> Result<Vec<ReplayBatch>, RuntimeError> {
    let mut items = Vec::new();
    for_each_request_replay_item(request_dir, request, block_store, progress, |item| {
        items.push(item);
        Ok(())
    })?;

    sort_replay_items(&mut items);
    let mut replay_batches = chunk_replay_items(items, max_concurrency);
    annotate_submission_progress_batches(&mut replay_batches, SubmissionProgressKind::Embedding);
    Ok(replay_batches)
}

fn chunk_replay_items(
    items: Vec<IndexItem<ContentRef>>,
    max_concurrency: usize,
) -> Vec<ReplayBatch> {
    let mut batches = Vec::new();
    let chunk_size = max_concurrency.max(1);
    let mut iter = items.into_iter().peekable();
    while iter.peek().is_some() {
        let chunk = iter.by_ref().take(chunk_size).collect();
        batches.push(ReplayBatch {
            items: chunk,
            audit_records: Vec::new(),
            completion_message: None,
        });
    }
    batches
}

#[allow(dead_code)]
fn chunk_replay_journal_records(
    records: Vec<ReplayJournalRecord>,
    max_concurrency: usize,
) -> Vec<ReplayBatch> {
    let mut batches = Vec::new();
    let chunk_size = max_concurrency.max(1);
    let mut iter = records.into_iter().peekable();
    while iter.peek().is_some() {
        let audit_records = iter.by_ref().take(chunk_size).collect::<Vec<_>>();
        let items = audit_records
            .iter()
            .map(|record| {
                replay_journal_record_to_item(record)
                    .expect("replay journal record batching only applies to replay inputs")
            })
            .collect::<Vec<_>>();
        batches.push(ReplayBatch {
            items,
            audit_records,
            completion_message: None,
        });
    }
    batches
}

fn annotate_submission_progress_batches(
    batches: &mut [ReplayBatch],
    progress_kind: SubmissionProgressKind,
) {
    let total_batches = batches.len();
    let total_items: usize = batches.iter().map(|batch| batch.items.len()).sum();
    let mut completed_items = 0usize;
    for (batch_index, batch) in batches.iter_mut().enumerate() {
        completed_items += batch.items.len();
        batch.completion_message = Some(progress_kind.completion_message(
            batch_index + 1,
            total_batches,
            completed_items,
            total_items,
        ));
    }
}

fn sort_replay_items(items: &mut [IndexItem<ContentRef>]) {
    items.sort_by_key(replay_sort_key);
}

#[allow(dead_code)]
fn sort_replay_journal_records(records: &mut [ReplayJournalRecord]) {
    records.sort_by_key(|record| {
        replay_sort_key(
            &replay_journal_record_to_item(record)
                .expect("replay journal record sorting only applies to replay inputs"),
        )
    });
}

fn replay_sort_key(item: &IndexItem<ContentRef>) -> (String, Vec<(String, String)>) {
    let content_key = match &item.content_ref {
        ContentRef::Document { path } => format!(
            "document:{}",
            normalize_document_identity_path(&path.to_string_lossy())
        ),
        ContentRef::Inline { media_type, body } => {
            format!("inline:{media_type}:{:?}", body)
        }
        ContentRef::StoredReplay { identity, .. } => match identity {
            ReplayIdentity::Document { source_path } => {
                format!("document:{}", normalize_document_identity_path(source_path))
            }
            ReplayIdentity::EmailChunk {
                email_artifact_ref,
                chunk_index,
            } => format!("email:{email_artifact_ref}:{chunk_index:020}"),
        },
        ContentRef::EmailChunk {
            email_artifact_ref,
            chunk_index,
        } => format!("email:{email_artifact_ref}:{chunk_index:020}"),
    };
    let metadata_key = metadata_to_text_map(&item.metadata).into_iter().collect();
    (content_key, metadata_key)
}

fn append_comparable_sort_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) {
    for byte in bytes {
        if *byte == 0 {
            buffer.extend_from_slice(&[0, 1]);
        } else {
            buffer.push(*byte);
        }
    }
}

fn append_comparable_sort_string(buffer: &mut Vec<u8>, value: &str) {
    append_comparable_sort_bytes(buffer, value.as_bytes());
    buffer.extend_from_slice(&[0, 0]);
}

fn append_usize_decimal(buffer: &mut Vec<u8>, value: usize) {
    let mut digits = [0u8; 20];
    let mut remaining = value;
    let mut index = digits.len();
    loop {
        index -= 1;
        digits[index] = b'0' + (remaining % 10) as u8;
        remaining /= 10;
        if remaining == 0 {
            break;
        }
    }
    buffer.extend_from_slice(&digits[index..]);
}

fn append_zero_padded_usize_decimal(buffer: &mut Vec<u8>, value: usize, width: usize) {
    let mut digits = [0u8; 20];
    let mut remaining = value;
    let mut index = digits.len();
    loop {
        index -= 1;
        digits[index] = b'0' + (remaining % 10) as u8;
        remaining /= 10;
        if remaining == 0 {
            break;
        }
    }
    let digit_len = digits.len() - index;
    for _ in digit_len..width {
        buffer.push(b'0');
    }
    buffer.extend_from_slice(&digits[index..]);
}

fn append_inline_debug_body(buffer: &mut Vec<u8>, body: &[u8]) {
    buffer.push(b'[');
    for (index, byte) in body.iter().enumerate() {
        if index > 0 {
            buffer.extend_from_slice(b", ");
        }
        append_usize_decimal(buffer, usize::from(*byte));
    }
    buffer.push(b']');
}

fn append_normalized_document_identity_path_bytes(buffer: &mut Vec<u8>, path: &str) {
    for byte in path.as_bytes() {
        let normalized = if *byte == b'\\' { b'/' } else { *byte };
        append_comparable_sort_bytes(buffer, &[normalized]);
    }
}

fn append_replay_content_sort_key_bytes(buffer: &mut Vec<u8>, content_ref: &ContentRef) {
    match content_ref {
        ContentRef::Document { path } => {
            append_comparable_sort_bytes(buffer, b"document:");
            append_normalized_document_identity_path_bytes(buffer, &path.to_string_lossy());
        }
        ContentRef::Inline { media_type, body } => {
            append_comparable_sort_bytes(buffer, b"inline:");
            append_comparable_sort_bytes(buffer, media_type.as_bytes());
            append_comparable_sort_bytes(buffer, b":");
            append_inline_debug_body(buffer, body);
        }
        ContentRef::StoredReplay { identity, .. } => match identity {
            ReplayIdentity::Document { source_path } => {
                append_comparable_sort_bytes(buffer, b"document:");
                append_normalized_document_identity_path_bytes(buffer, source_path);
            }
            ReplayIdentity::EmailChunk {
                email_artifact_ref,
                chunk_index,
            } => {
                append_comparable_sort_bytes(buffer, b"email:");
                append_comparable_sort_bytes(buffer, email_artifact_ref.as_bytes());
                append_comparable_sort_bytes(buffer, b":");
                append_zero_padded_usize_decimal(buffer, *chunk_index, 20);
            }
        },
        ContentRef::EmailChunk {
            email_artifact_ref,
            chunk_index,
        } => {
            append_comparable_sort_bytes(buffer, b"email:");
            append_comparable_sort_bytes(buffer, email_artifact_ref.as_bytes());
            append_comparable_sort_bytes(buffer, b":");
            append_zero_padded_usize_decimal(buffer, *chunk_index, 20);
        }
    }
    buffer.extend_from_slice(&[0, 0]);
}

fn append_replay_journal_content_sort_key_bytes(
    buffer: &mut Vec<u8>,
    content_ref: &ReplayJournalContentRef,
) {
    match content_ref {
        ReplayJournalContentRef::Document { path } => {
            append_comparable_sort_bytes(buffer, b"document:");
            append_normalized_document_identity_path_bytes(buffer, path);
        }
        ReplayJournalContentRef::Inline { media_type, body } => {
            append_comparable_sort_bytes(buffer, b"inline:");
            append_comparable_sort_bytes(buffer, media_type.as_bytes());
            append_comparable_sort_bytes(buffer, b":");
            append_inline_debug_body(buffer, body);
        }
        ReplayJournalContentRef::EmailChunk {
            email_artifact_ref,
            chunk_index,
        } => {
            append_comparable_sort_bytes(buffer, b"email:");
            append_comparable_sort_bytes(buffer, email_artifact_ref.as_bytes());
            append_comparable_sort_bytes(buffer, b":");
            append_zero_padded_usize_decimal(buffer, *chunk_index, 20);
        }
    }
    buffer.extend_from_slice(&[0, 0]);
}

fn append_metadata_sort_key(buffer: &mut Vec<u8>, metadata_key: &[(String, String)]) {
    for (key, value) in metadata_key {
        append_comparable_sort_string(buffer, key);
        append_comparable_sort_string(buffer, value);
    }
}

fn append_canonical_replay_journal_metadata_sort_key(
    buffer: &mut Vec<u8>,
    metadata: &[(String, String)],
) {
    let metadata_key = metadata
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<BTreeMap<_, _>>();
    for (key, value) in metadata_key {
        append_comparable_sort_string(buffer, key);
        append_comparable_sort_string(buffer, value);
    }
}

#[cfg(test)]
fn encode_metadata_sort_key(metadata_key: &[(String, String)]) -> Vec<u8> {
    let mut encoded = Vec::new();
    append_metadata_sort_key(&mut encoded, metadata_key);
    encoded
}

fn replay_sort_key_digest(item: &IndexItem<ContentRef>) -> BlockHash {
    let mut encoded = Vec::new();
    append_replay_content_sort_key_bytes(&mut encoded, &item.content_ref);
    let metadata_key = metadata_to_text_map(&item.metadata)
        .into_iter()
        .collect::<Vec<_>>();
    append_metadata_sort_key(&mut encoded, &metadata_key);
    hash_bytes(&encoded)
}

fn replay_journal_record_sort_key_digest(record: &ReplayJournalRecord) -> Option<BlockHash> {
    let ReplayJournalRecord::ReplayInput {
        metadata,
        content_ref,
        ..
    } = record
    else {
        return None;
    };
    let mut encoded = Vec::new();
    append_replay_journal_content_sort_key_bytes(&mut encoded, content_ref);
    append_canonical_replay_journal_metadata_sort_key(&mut encoded, metadata);
    Some(hash_bytes(&encoded))
}

#[cfg(test)]
fn replay_sort_key_sql(item: &IndexItem<ContentRef>) -> Result<(String, Vec<u8>), RuntimeError> {
    let (content_key, metadata_key) = replay_sort_key(item);
    Ok((content_key, encode_metadata_sort_key(&metadata_key)))
}

#[cfg(test)]
fn mutable_ref_store_path(block_store_root: &Path, ref_name: &str) -> PathBuf {
    let mut relative = PathBuf::from(MUTABLE_REF_ROOT_DIR);
    for segment in ref_name.split('/') {
        relative.push(segment);
    }
    match block_store_root.parent() {
        Some(parent) => parent.join(relative),
        None => block_store_root.join(relative),
    }
}

#[cfg(test)]
fn local_mutable_ref_store_location(
    block_store_root: &Path,
    ref_name: &str,
) -> MutableRefStoreLocation {
    MutableRefStoreLocation::LocalFile {
        path: mutable_ref_store_path(block_store_root, ref_name),
    }
}

fn execution_stage_label(stage: ExecutionStage) -> &'static str {
    match stage {
        ExecutionStage::FullPipeline => "full-pipeline",
        ExecutionStage::IngestionAndEmbedding => "ingestion-and-embedding",
        ExecutionStage::ClusteringAndBlockAssembly => "clustering-and-block-assembly",
    }
}

fn mutable_ref_store_metadata(
    stage: ExecutionStage,
    clustering: &ConfiguredClustering,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "profile_version".into(),
            clustering.profile_version.to_string(),
        ),
        ("stage".into(), execution_stage_label(stage).into()),
    ])
}

fn mutable_ref_store_label(location: &MutableRefStoreLocation) -> String {
    match location {
        MutableRefStoreLocation::LocalFile { path } => path.display().to_string(),
        MutableRefStoreLocation::AzureBlob { display_path, .. } => display_path.clone(),
        MutableRefStoreLocation::AzureTable { display_path, .. } => display_path.clone(),
    }
}

#[derive(Clone, Debug)]
struct MutableRefTableEndpoint {
    account: String,
    table_name: String,
    sas_token: String,
}

impl MutableRefTableEndpoint {
    fn parse(table_sas_url: &str) -> Result<Self, io::Error> {
        let mut url = Url::parse(table_sas_url)
            .map_err(|error| mutable_ref_store_io_error(error.to_string()))?;
        url.set_fragment(None);
        if url.query().is_none_or(str::is_empty) {
            return Err(mutable_ref_store_io_error(
                "Azure Table SAS URL must include SAS query parameters".into(),
            ));
        }
        if !url
            .query_pairs()
            .any(|(key, value)| key == "sig" && !value.is_empty())
        {
            return Err(mutable_ref_store_io_error(
                "Azure Table SAS URL must include a non-empty SAS signature parameter".into(),
            ));
        }

        let host = url.host_str().ok_or_else(|| {
            mutable_ref_store_io_error("Azure Table SAS URL must include a host".into())
        })?;
        let account = host
            .split('.')
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                mutable_ref_store_io_error(
                    "Azure Table SAS URL must include an account host".into(),
                )
            })?
            .to_string();

        let path_segments = url
            .path_segments()
            .ok_or_else(|| {
                mutable_ref_store_io_error("Azure Table SAS URL must be hierarchical".into())
            })?
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        if path_segments.len() != 1 {
            return Err(mutable_ref_store_io_error(format!(
                "Azure Table SAS URL must address a table root, got path {}",
                url.path()
            )));
        }

        let table_name = path_segments[0].to_string();
        if table_name.contains('(') || table_name.contains(')') {
            return Err(mutable_ref_store_io_error(format!(
                "Azure Table SAS URL must address a table root, got path {}",
                url.path()
            )));
        }

        Ok(Self {
            account,
            table_name,
            sas_token: url.query().unwrap_or_default().to_string(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MutableRefTableEntity {
    #[serde(rename = "PartitionKey")]
    partition_key: String,
    #[serde(rename = "RowKey")]
    row_key: String,
    #[serde(rename = "SchemaVersion")]
    schema_version: i32,
    #[serde(rename = "RefPath")]
    ref_path: String,
    #[serde(rename = "StateJson")]
    state_json: String,
}

fn mutable_ref_table_client(table_sas_url: &str) -> Result<TableClient, io::Error> {
    let endpoint = MutableRefTableEndpoint::parse(table_sas_url)?;
    let credentials = StorageCredentials::sas_token(&endpoint.sas_token)
        .map_err(|error| mutable_ref_store_io_error(error.to_string()))?;
    let table_service = TableServiceClientBuilder::new(endpoint.account, credentials).build();
    Ok(table_service.table_client(endpoint.table_name))
}

fn read_mutable_ref_table_entity(
    table_sas_url: &str,
    partition_key: &str,
    row_key: &str,
) -> Result<Option<MutableRefTableEntity>, io::Error> {
    let table_client = mutable_ref_table_client(table_sas_url)?;
    let filter = Filter::new(mutable_ref_table_lookup_filter(partition_key, row_key));
    let response = block_on_future_factory(move || async move {
        let mut stream = table_client
            .query()
            .filter(filter)
            .top(Top::new(2))
            .into_stream::<MutableRefTableEntity>();
        match stream.next().await {
            Some(result) => result,
            None => Err(azure_core::Error::message(
                azure_core::error::ErrorKind::Other,
                "Azure Table query returned no response pages",
            )),
        }
    })
    .map_err(|error| mutable_ref_store_io_error(error.to_string()))?;

    match response.entities.len() {
        0 => Ok(None),
        1 => Ok(response.entities.into_iter().next()),
        count => Err(mutable_ref_store_io_error(format!(
            "lookup for PartitionKey={partition_key} RowKey={row_key} returned {count} entities"
        ))),
    }
}

fn mutable_ref_table_lookup_filter(partition_key: &str, row_key: &str) -> String {
    format!(
        "PartitionKey eq '{}' and RowKey eq '{}'",
        escape_odata_string_literal(partition_key),
        escape_odata_string_literal(row_key)
    )
}

fn escape_odata_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn write_mutable_ref_table_entity(
    table_sas_url: &str,
    entity: &MutableRefTableEntity,
) -> Result<(), io::Error> {
    let table_client = mutable_ref_table_client(table_sas_url)?;
    let entity = entity.clone();
    block_on_future_factory(move || async move {
        table_client
            .partition_key_client(&entity.partition_key)
            .entity_client(&entity.row_key)?
            .insert_or_replace(entity)?
            .await
    })
    .map(|_| ())
    .map_err(|error| mutable_ref_store_io_error(error.to_string()))
}

fn mutable_ref_store_io_error(message: String) -> io::Error {
    io::Error::other(message)
}

fn mutable_ref_store_http_client() -> Result<Client, io::Error> {
    Client::builder()
        .timeout(MUTABLE_REF_STORE_HTTP_TIMEOUT)
        .build()
        .map_err(|error| mutable_ref_store_io_error(error.to_string()))
}

fn mutable_ref_store_request_with_retry<F>(
    mut send: F,
) -> Result<reqwest::blocking::Response, io::Error>
where
    F: FnMut(&Client) -> Result<reqwest::blocking::Response, reqwest::Error>,
{
    let client = mutable_ref_store_http_client()?;
    let mut last_error = None;
    for attempt in 0..MUTABLE_REF_STORE_HTTP_RETRY_ATTEMPTS {
        match send(&client) {
            Ok(response)
                if attempt + 1 < MUTABLE_REF_STORE_HTTP_RETRY_ATTEMPTS
                    && (response.status().is_server_error()
                        || response.status() == StatusCode::REQUEST_TIMEOUT
                        || response.status() == StatusCode::TOO_MANY_REQUESTS) =>
            {
                last_error = Some(mutable_ref_store_io_error(format!(
                    "retryable HTTP status {} while accessing mutable ref store",
                    response.status()
                )));
                std::thread::sleep(MUTABLE_REF_STORE_HTTP_RETRY_DELAY);
            }
            Ok(response) => return Ok(response),
            Err(error) if attempt + 1 < MUTABLE_REF_STORE_HTTP_RETRY_ATTEMPTS => {
                last_error = Some(mutable_ref_store_io_error(error.to_string()));
                std::thread::sleep(MUTABLE_REF_STORE_HTTP_RETRY_DELAY);
            }
            Err(error) => return Err(mutable_ref_store_io_error(error.to_string())),
        }
    }
    Err(last_error
        .unwrap_or_else(|| mutable_ref_store_io_error("mutable ref request failed".into())))
}

fn read_mutable_ref_store_bytes(
    location: &MutableRefStoreLocation,
) -> Result<Option<Vec<u8>>, RuntimeError> {
    match location {
        MutableRefStoreLocation::LocalFile { path } => {
            if !path.exists() {
                return Ok(None);
            }
            fs::read(path)
                .map(Some)
                .map_err(|source| RuntimeError::ReadMutableRefStore {
                    path: path.display().to_string(),
                    source,
                })
        }
        MutableRefStoreLocation::AzureBlob { url, display_path } => {
            let response = mutable_ref_store_request_with_retry(|client| {
                client
                    .get(url)
                    .header("x-ms-version", AZURE_BLOB_API_VERSION)
                    .send()
            })
            .map_err(|source| RuntimeError::ReadMutableRefStore {
                path: display_path.clone(),
                source,
            })?;
            match response.status() {
                StatusCode::NOT_FOUND => Ok(None),
                status if status.is_success() => response
                    .bytes()
                    .map(|bytes| Some(bytes.to_vec()))
                    .map_err(|error| RuntimeError::ReadMutableRefStore {
                        path: display_path.clone(),
                        source: mutable_ref_store_io_error(error.to_string()),
                    }),
                status => Err(RuntimeError::ReadMutableRefStore {
                    path: display_path.clone(),
                    source: mutable_ref_store_io_error(format!(
                        "unexpected HTTP status {} while reading mutable ref store",
                        status
                    )),
                }),
            }
        }
        MutableRefStoreLocation::AzureTable {
            table_sas_url,
            display_path,
            partition_key,
            row_key,
        } => {
            let Some(entity) = read_mutable_ref_table_entity(table_sas_url, partition_key, row_key)
                .map_err(|source| RuntimeError::ReadMutableRefStore {
                    path: display_path.clone(),
                    source,
                })?
            else {
                return Ok(None);
            };
            if entity.schema_version != MUTABLE_REF_TABLE_SCHEMA_VERSION {
                return Err(RuntimeError::ReadMutableRefStore {
                    path: display_path.clone(),
                    source: mutable_ref_store_io_error(format!(
                        "unsupported mutable ref table schema version {}",
                        entity.schema_version
                    )),
                });
            }
            Ok(Some(entity.state_json.into_bytes()))
        }
    }
}

fn write_mutable_ref_store_bytes(
    location: &MutableRefStoreLocation,
    encoded: &[u8],
) -> Result<(), RuntimeError> {
    match location {
        MutableRefStoreLocation::LocalFile { path } => {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|source| {
                    RuntimeError::PrepareMutableRefStore {
                        path: parent.display().to_string(),
                        source,
                    }
                })?;
            }
            let temp_path = path.with_extension("tmp");
            let backup_path = path.with_extension("bak");
            fs::write(&temp_path, encoded).map_err(|source| {
                RuntimeError::WriteMutableRefStore {
                    path: temp_path.display().to_string(),
                    source,
                }
            })?;
            let had_existing_file = path.exists();
            if had_existing_file {
                if backup_path.exists() {
                    fs::remove_file(&backup_path).map_err(|source| {
                        RuntimeError::WriteMutableRefStore {
                            path: backup_path.display().to_string(),
                            source,
                        }
                    })?;
                }
                fs::rename(path, &backup_path).map_err(|source| {
                    RuntimeError::WriteMutableRefStore {
                        path: path.display().to_string(),
                        source,
                    }
                })?;
            }
            if let Err(source) = fs::rename(&temp_path, path) {
                if had_existing_file {
                    let _ = fs::rename(&backup_path, path);
                }
                return Err(RuntimeError::WriteMutableRefStore {
                    path: path.display().to_string(),
                    source,
                });
            }
            if had_existing_file && backup_path.exists() {
                fs::remove_file(&backup_path).map_err(|source| {
                    RuntimeError::WriteMutableRefStore {
                        path: backup_path.display().to_string(),
                        source,
                    }
                })?;
            }
            Ok(())
        }
        MutableRefStoreLocation::AzureBlob { url, display_path } => {
            let response = mutable_ref_store_request_with_retry(|client| {
                client
                    .put(url)
                    .header("content-type", "application/json")
                    .header("x-ms-blob-type", "BlockBlob")
                    .header("x-ms-version", AZURE_BLOB_API_VERSION)
                    .body(encoded.to_vec())
                    .send()
            })
            .map_err(|source| RuntimeError::WriteMutableRefStore {
                path: display_path.clone(),
                source,
            })?;
            if response.status().is_success() {
                Ok(())
            } else {
                Err(RuntimeError::WriteMutableRefStore {
                    path: display_path.clone(),
                    source: mutable_ref_store_io_error(format!(
                        "unexpected HTTP status {} while writing mutable ref store",
                        response.status()
                    )),
                })
            }
        }
        MutableRefStoreLocation::AzureTable {
            table_sas_url,
            display_path,
            partition_key,
            row_key,
        } => {
            let state_json = std::str::from_utf8(encoded).map_err(|error| {
                RuntimeError::WriteMutableRefStore {
                    path: display_path.clone(),
                    source: mutable_ref_store_io_error(error.to_string()),
                }
            })?;
            let entity = MutableRefTableEntity {
                partition_key: partition_key.clone(),
                row_key: row_key.clone(),
                schema_version: MUTABLE_REF_TABLE_SCHEMA_VERSION,
                ref_path: display_path.clone(),
                state_json: state_json.to_string(),
            };
            write_mutable_ref_table_entity(table_sas_url, &entity).map_err(|source| {
                RuntimeError::WriteMutableRefStore {
                    path: display_path.clone(),
                    source,
                }
            })
        }
    }
}

fn prepare_mutable_ref_store(location: &MutableRefStoreLocation) -> Result<(), RuntimeError> {
    match location {
        MutableRefStoreLocation::LocalFile { path } => {
            let Some(parent) = path.parent() else {
                return Ok(());
            };
            fs::create_dir_all(parent).map_err(|source| RuntimeError::PrepareMutableRefStore {
                path: parent.display().to_string(),
                source,
            })
        }
        MutableRefStoreLocation::AzureBlob { .. } => Ok(()),
        MutableRefStoreLocation::AzureTable {
            table_sas_url,
            display_path,
            ..
        } => mutable_ref_table_client(table_sas_url)
            .map(|_| ())
            .map_err(|source| RuntimeError::PrepareMutableRefStore {
                path: display_path.clone(),
                source,
            }),
    }
}

async fn prepare_mutable_ref_store_async(
    location: MutableRefStoreLocation,
) -> Result<(), RuntimeError> {
    tokio::task::spawn_blocking(move || prepare_mutable_ref_store(&location))
        .await
        .map_err(RuntimeError::BlockingMutableRefTaskJoin)?
}

fn load_mutable_ref_store(
    location: &MutableRefStoreLocation,
) -> Result<MutableRefStoreState, RuntimeError> {
    let label = mutable_ref_store_label(location);
    let Some(bytes) = read_mutable_ref_store_bytes(location)? else {
        return Ok(MutableRefStoreState::default());
    };
    serde_json::from_slice(bytes.as_slice()).map_err(|error| RuntimeError::DecodeMutableRefStore {
        path: label,
        message: error.to_string(),
    })
}

fn write_mutable_ref_store(
    location: &MutableRefStoreLocation,
    state: &MutableRefStoreState,
) -> Result<(), RuntimeError> {
    let label = mutable_ref_store_label(location);
    let encoded =
        serde_json::to_vec_pretty(state).map_err(|error| RuntimeError::EncodeMutableRefStore {
            path: label,
            message: error.to_string(),
        })?;
    write_mutable_ref_store_bytes(location, &encoded)
}

fn apply_mutable_ref_store_update(refs: &mut MutableRefStoreState, update: MutableRefStoreUpdate) {
    if let Some(current_root_block_id) = update.current_root_block_id {
        refs.current_root_block_id = Some(current_root_block_id);
    }
    if let Some(replay_journal_head_block_id) = update.replay_journal_head_block_id {
        refs.replay_journal_head_block_id = Some(replay_journal_head_block_id);
    }
    if let Some(metadata) = update.metadata {
        refs.metadata = Some(metadata);
    }
}

fn update_mutable_ref_store(
    mutable_ref_store: &MutableRefStoreLocation,
    update: MutableRefStoreUpdate,
) -> Result<(), RuntimeError> {
    let mut refs = load_mutable_ref_store(mutable_ref_store)?;
    apply_mutable_ref_store_update(&mut refs, update);
    write_mutable_ref_store(mutable_ref_store, &refs)
}

async fn update_mutable_ref_store_async(
    mutable_ref_store: MutableRefStoreLocation,
    update: MutableRefStoreUpdate,
) -> Result<(), RuntimeError> {
    tokio::task::spawn_blocking(move || update_mutable_ref_store(&mutable_ref_store, update))
        .await
        .map_err(RuntimeError::BlockingMutableRefTaskJoin)?
}

fn append_replay_journal_records(
    store: &dyn BlockStore,
    mutable_ref_store: &MutableRefStoreLocation,
    records: &[ReplayJournalRecord],
) -> Result<Option<String>, RuntimeError> {
    if records.is_empty() {
        return Ok(load_mutable_ref_store(mutable_ref_store)?.replay_journal_head_block_id);
    }

    let mut current_head = load_mutable_ref_store(mutable_ref_store)?.replay_journal_head_block_id;
    let mut pending_entries = Vec::new();
    let mut pending_entry_sizes = Vec::new();
    let mut pending_entry_size_sum = 0usize;

    for record in records {
        let encoded_record = encode_replay_journal_record(record)?;
        let encoded_record_size = encoded_record.len();
        pending_entries.push(record.clone());
        pending_entry_sizes.push(encoded_record_size);
        pending_entry_size_sum += encoded_record_size;
        let block_size = replay_journal_block_body_size(
            pending_entry_sizes.as_slice(),
            pending_entry_size_sum,
            current_head.as_deref(),
        )?;
        if block_size > REPLAY_JOURNAL_BLOCK_MAX_BYTES {
            let overflow = pending_entries
                .pop()
                .expect("pending_entries was just pushed");
            let overflow_size = pending_entry_sizes
                .pop()
                .expect("pending_entry_sizes was just pushed");
            if pending_entries.is_empty() {
                return Err(RuntimeError::WriteReplayJournal {
                    block_id: replay_journal_record_label(&overflow).to_string(),
                    source: io::Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "replay journal block exceeded {}-byte maximum payload",
                            REPLAY_JOURNAL_BLOCK_MAX_BYTES
                        ),
                    ),
                });
            }
            let published = store_replay_journal_block(
                store,
                current_head.as_deref(),
                std::mem::take(&mut pending_entries),
            )?;
            current_head = Some(published.to_string());
            pending_entry_sizes.clear();
            pending_entry_size_sum = 0;
            pending_entries.push(overflow);
            pending_entry_sizes.push(overflow_size);
            pending_entry_size_sum += overflow_size;
        }
    }

    if !pending_entries.is_empty() {
        let published = store_replay_journal_block(
            store,
            current_head.as_deref(),
            std::mem::take(&mut pending_entries),
        )?;
        current_head = Some(published.to_string());
    }

    Ok(current_head)
}

async fn append_replay_journal_records_async(
    store: ConfiguredBlockStore,
    mutable_ref_store: MutableRefStoreLocation,
    records: Vec<ReplayJournalRecord>,
) -> Result<Option<String>, RuntimeError> {
    tokio::task::spawn_blocking(move || {
        append_replay_journal_records(&store, &mutable_ref_store, &records)
    })
    .await
    .map_err(RuntimeError::BlockingMutableRefTaskJoin)?
}

fn replay_journal_block_body_size(
    encoded_entry_sizes: &[usize],
    encoded_entry_size_sum: usize,
    previous_block_id: Option<&str>,
) -> Result<usize, RuntimeError> {
    let base_len = encode_replay_journal_block_body(&[], previous_block_id)?.len();
    Ok(base_len - cbor_array_header_size(0)
        + cbor_array_header_size(encoded_entry_sizes.len())
        + encoded_entry_size_sum)
}

fn cbor_array_header_size(len: usize) -> usize {
    match len {
        0..=23 => 1,
        24..=0xff => 2,
        0x100..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
    }
}

fn encode_replay_journal_record(record: &ReplayJournalRecord) -> Result<Vec<u8>, RuntimeError> {
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(record, &mut encoded).map_err(|source| {
        RuntimeError::WriteReplayJournal {
            block_id: replay_journal_record_label(record).to_string(),
            source: io::Error::new(ErrorKind::InvalidData, source.to_string()),
        }
    })?;
    Ok(encoded)
}

fn encode_replay_journal_block_body(
    entries: &[ReplayJournalRecord],
    previous_block_id: Option<&str>,
) -> Result<Vec<u8>, RuntimeError> {
    let body = ReplayJournalBlockBody {
        schema_version: REPLAY_JOURNAL_SCHEMA_VERSION,
        previous_block_id: previous_block_id.map(str::to_string),
        entries: entries.to_vec(),
    };
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(&body, &mut encoded).map_err(|source| {
        RuntimeError::WriteReplayJournal {
            block_id: previous_block_id.unwrap_or("<new>").to_string(),
            source: io::Error::new(ErrorKind::InvalidData, source.to_string()),
        }
    })?;
    Ok(encoded)
}

fn replay_journal_custom_block(body: Vec<u8>) -> Result<VersionedBlock, RuntimeError> {
    let block = v2::build_custom_block(
        REPLAY_JOURNAL_BLOCK_TYPE,
        Value::Map(vec![
            (
                Value::Text("media_type".into()),
                Value::Text(REPLAY_JOURNAL_MEDIA_TYPE.to_string()),
            ),
            (Value::Text("body".into()), Value::Bytes(body)),
        ]),
    )
    .map_err(|source| RuntimeError::WriteReplayJournal {
        block_id: "<new>".into(),
        source: io::Error::new(ErrorKind::InvalidData, source.to_string()),
    })?;
    Ok(VersionedBlock::V2(block))
}

fn store_replay_journal_block(
    store: &dyn BlockStore,
    previous_block_id: Option<&str>,
    entries: Vec<ReplayJournalRecord>,
) -> Result<BlockHash, RuntimeError> {
    let body = encode_replay_journal_block_body(&entries, previous_block_id)?;
    if body.len() > REPLAY_JOURNAL_BLOCK_MAX_BYTES {
        let block_id = entries
            .first()
            .map(|entry| replay_journal_record_label(entry).to_string())
            .unwrap_or_else(|| "<empty>".into());
        return Err(RuntimeError::WriteReplayJournal {
            block_id,
            source: io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "replay journal block exceeded {}-byte maximum payload",
                    REPLAY_JOURNAL_BLOCK_MAX_BYTES
                ),
            ),
        });
    }
    let versioned = replay_journal_custom_block(body)?;
    block_on_block_store_future(store.put_versioned(&versioned)).map_err(RuntimeError::BlockStore)
}

#[allow(dead_code)]
fn load_replay_journal_records(
    store: &ConfiguredBlockStore,
    mutable_ref_store: &MutableRefStoreLocation,
) -> Result<Vec<ReplayJournalRecord>, RuntimeError> {
    let refs = load_mutable_ref_store(mutable_ref_store)?;
    let Some(mut current_block_id) = refs.replay_journal_head_block_id else {
        return Err(RuntimeError::MissingReplayJournalHead {
            path: mutable_ref_store_label(mutable_ref_store),
        });
    };

    let mut visited = HashSet::new();
    let mut blocks = Vec::new();
    loop {
        if !visited.insert(current_block_id.clone()) {
            return Err(RuntimeError::InvalidReplayJournalHead {
                block_id: current_block_id,
                message: "replay journal chain contains a cycle".into(),
            });
        }
        let block_hash = parse_block_hash(&current_block_id).map_err(|error| {
            RuntimeError::InvalidReplayJournalHead {
                block_id: current_block_id.clone(),
                message: error.to_string(),
            }
        })?;
        let Some(decoded) = block_on_block_store_future(store.get_decoded(&block_hash))? else {
            return Err(RuntimeError::ReadReplayJournal {
                block_id: current_block_id,
                source: io::Error::new(ErrorKind::NotFound, "referenced journal block is missing"),
            });
        };
        let decoded = replay_journal_block_body_from_decoded(decoded, &block_hash.to_string())?;
        current_block_id = match decoded.previous_block_id.clone() {
            Some(previous) => {
                blocks.push(decoded);
                previous
            }
            None => {
                blocks.push(decoded);
                break;
            }
        };
    }

    blocks.reverse();
    let mut records = Vec::new();
    for block in blocks {
        records.extend(block.entries);
    }
    Ok(records)
}

fn replay_journal_block_body_from_decoded(
    decoded: DecodedBlock,
    block_id: &str,
) -> Result<ReplayJournalBlockBody, RuntimeError> {
    let custom = match decoded {
        DecodedBlock::V2(validated) => match v2::into_typed_block(validated).map_err(|error| {
            RuntimeError::InvalidReplayJournalHead {
                block_id: block_id.to_string(),
                message: error.to_string(),
            }
        })? {
            v2::TypedBlock::Custom(custom) => custom,
            other => {
                return Err(RuntimeError::InvalidReplayJournalHead {
                    block_id: block_id.to_string(),
                    message: format!("unexpected journal block type {}", typed_block_name(&other)),
                });
            }
        },
        other => {
            return Err(RuntimeError::InvalidReplayJournalHead {
                block_id: block_id.to_string(),
                message: format!("unexpected journal block version {:?}", other),
            });
        }
    };
    if custom.type_name != REPLAY_JOURNAL_BLOCK_TYPE {
        return Err(RuntimeError::InvalidReplayJournalHead {
            block_id: block_id.to_string(),
            message: format!("unexpected journal block type name {}", custom.type_name),
        });
    }
    let (media_type, body) = custom_block_payload(&custom.content).map_err(|message| {
        RuntimeError::InvalidReplayJournalHead {
            block_id: block_id.to_string(),
            message,
        }
    })?;
    if media_type != REPLAY_JOURNAL_MEDIA_TYPE {
        return Err(RuntimeError::InvalidReplayJournalHead {
            block_id: block_id.to_string(),
            message: format!("unexpected journal media type {media_type}"),
        });
    }
    let decoded: ReplayJournalBlockBody =
        ciborium::de::from_reader(Cursor::new(body)).map_err(|error| {
            RuntimeError::InvalidReplayJournalHead {
                block_id: block_id.to_string(),
                message: error.to_string(),
            }
        })?;
    if decoded.schema_version != REPLAY_JOURNAL_SCHEMA_VERSION {
        return Err(RuntimeError::InvalidReplayJournalHead {
            block_id: block_id.to_string(),
            message: format!(
                "journal block schema_version must be {}",
                REPLAY_JOURNAL_SCHEMA_VERSION
            ),
        });
    }
    Ok(decoded)
}

fn typed_block_name(block: &v2::TypedBlock) -> &str {
    match block {
        v2::TypedBlock::Branch(branch) => &branch.type_name,
        v2::TypedBlock::Leaf(leaf) => &leaf.type_name,
        v2::TypedBlock::Custom(custom) => &custom.type_name,
    }
}

fn replay_journal_record_label(record: &ReplayJournalRecord) -> &str {
    match record {
        ReplayJournalRecord::ReplayInput { block_id, .. } => block_id,
        ReplayJournalRecord::IndexingOutcome { root_block_id, .. } => root_block_id,
    }
}

fn replay_journal_record_from_item(
    block_id: BlockHash,
    item: &IndexItem<ContentRef>,
) -> ReplayJournalRecord {
    let metadata = metadata_to_text_map(&item.metadata).into_iter().collect();
    let content_ref = match &item.content_ref {
        ContentRef::Document { path } => ReplayJournalContentRef::Document {
            path: normalize_replay_journal_path(path),
        },
        ContentRef::Inline { media_type, body } => ReplayJournalContentRef::Inline {
            media_type: media_type.clone(),
            body: body.clone(),
        },
        ContentRef::StoredReplay { identity, .. } => match identity {
            ReplayIdentity::Document { source_path } => ReplayJournalContentRef::Document {
                path: normalize_document_identity_path(source_path),
            },
            ReplayIdentity::EmailChunk {
                email_artifact_ref,
                chunk_index,
            } => ReplayJournalContentRef::EmailChunk {
                email_artifact_ref: email_artifact_ref.clone(),
                chunk_index: *chunk_index,
            },
        },
        ContentRef::EmailChunk {
            email_artifact_ref,
            chunk_index,
        } => ReplayJournalContentRef::EmailChunk {
            email_artifact_ref: email_artifact_ref.clone(),
            chunk_index: *chunk_index,
        },
    };
    ReplayJournalRecord::ReplayInput {
        step_kind: ReplayJournalStepKind::Embedding,
        block_id: block_id.to_string(),
        metadata,
        content_ref,
    }
}

fn normalize_replay_journal_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn replay_journal_record_to_item(record: &ReplayJournalRecord) -> Option<IndexItem<ContentRef>> {
    let ReplayJournalRecord::ReplayInput {
        metadata,
        content_ref,
        ..
    } = record
    else {
        return None;
    };
    let metadata = text_pairs_to_metadata(metadata);
    let content_ref = match content_ref {
        ReplayJournalContentRef::Document { path } => ContentRef::Document { path: path.into() },
        ReplayJournalContentRef::Inline { media_type, body } => ContentRef::Inline {
            media_type: media_type.clone(),
            body: body.clone(),
        },
        ReplayJournalContentRef::EmailChunk {
            email_artifact_ref,
            chunk_index,
        } => ContentRef::EmailChunk {
            email_artifact_ref: email_artifact_ref.clone(),
            chunk_index: *chunk_index,
        },
    };
    Some(IndexItem {
        metadata,
        content_ref,
    })
}

fn text_pairs_to_metadata(pairs: &[(String, String)]) -> Vec<(ciborium::Value, ciborium::Value)> {
    pairs
        .iter()
        .map(|(key, value)| {
            (
                ciborium::Value::Text(key.clone()),
                ciborium::Value::Text(value.clone()),
            )
        })
        .collect()
}

fn replay_journal_records_from_block_ids(
    block_ids: &[BlockHash],
    store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
) -> Result<Vec<ReplayJournalRecord>, RuntimeError> {
    let mut records = Vec::new();
    for block_id in block_ids {
        let Some(validated) = block_on_block_store_future(store.get(block_id))? else {
            return Err(RuntimeError::MissingIteratedBlock {
                block_id: block_id.to_string(),
            });
        };
        let Some((item, _)) = replay_item_from_validated_block(&validated, embedding_spec)? else {
            continue;
        };
        records.push(replay_journal_record_from_item(validated.hash, &item));
    }
    Ok(records)
}

#[allow(dead_code)]
fn replay_input_count_from_batches(batches: &[ReplayBatch]) -> usize {
    batches.iter().map(|batch| batch.items.len()).sum()
}

fn replay_journal_indexing_outcome_record(
    input_block_count: usize,
    generated_block_ids: &[BlockHash],
    root_id: &BlockHash,
) -> ReplayJournalRecord {
    let mut generated_block_ids = generated_block_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    generated_block_ids.sort();
    generated_block_ids.dedup();

    ReplayJournalRecord::IndexingOutcome {
        step_kind: ReplayJournalStepKind::Indexing,
        input_block_ids: Vec::new(),
        input_block_count,
        generated_block_ids,
        root_block_id: root_id.to_string(),
    }
}

async fn build_leaf_blocks_concurrently(
    resolver: LocalFilesystemContentResolver,
    embedding_provider: ConfiguredEmbeddingProvider,
    items: &[IndexItem<ContentRef>],
    embedding_spec: &EmbeddingSpec,
    max_concurrency: usize,
) -> Result<ConstructedBlocks, RuntimeError> {
    if items.is_empty() {
        return Ok(ConstructedBlocks {
            block_ids: Vec::new(),
            blocks: Vec::new(),
        });
    }

    let concurrency = max_concurrency.max(1).min(items.len());
    let mut join_set = JoinSet::new();
    let mut next_index = 0;
    while next_index < concurrency {
        spawn_leaf_block_task(
            &mut join_set,
            next_index,
            resolver.clone(),
            embedding_provider.clone(),
            items[next_index].clone(),
            embedding_spec.clone(),
        );
        next_index += 1;
    }

    let mut completed = (0..items.len()).map(|_| None).collect::<Vec<_>>();
    while let Some(result) = join_set.join_next().await {
        let (batch_index, constructed) = result??;
        completed[batch_index] = Some(constructed);
        if next_index < items.len() {
            spawn_leaf_block_task(
                &mut join_set,
                next_index,
                resolver.clone(),
                embedding_provider.clone(),
                items[next_index].clone(),
                embedding_spec.clone(),
            );
            next_index += 1;
        }
    }

    let mut block_ids = Vec::with_capacity(items.len());
    let mut blocks = Vec::with_capacity(items.len());
    for constructed in completed.into_iter().flatten() {
        block_ids.extend(constructed.block_ids);
        blocks.extend(constructed.blocks);
    }

    Ok(ConstructedBlocks { block_ids, blocks })
}

fn spawn_leaf_block_task(
    join_set: &mut JoinSet<Result<(usize, ConstructedBlocks), RuntimeError>>,
    item_index: usize,
    resolver: LocalFilesystemContentResolver,
    embedding_provider: ConfiguredEmbeddingProvider,
    item: IndexItem<ContentRef>,
    embedding_spec: EmbeddingSpec,
) {
    join_set.spawn(async move {
        let constructed =
            construct_leaf_block_batch(resolver, embedding_provider, vec![item], embedding_spec)
                .await?;
        Ok::<(usize, ConstructedBlocks), RuntimeError>((item_index, constructed))
    });
}

async fn construct_leaf_block_batch(
    resolver: LocalFilesystemContentResolver,
    embedding_provider: ConfiguredEmbeddingProvider,
    items: Vec<IndexItem<ContentRef>>,
    embedding_spec: EmbeddingSpec,
) -> Result<ConstructedBlocks, RuntimeError> {
    let mut contents = Vec::with_capacity(items.len());
    let mut inputs = Vec::with_capacity(items.len());
    for item in &items {
        let content = resolver.resolve(&item.content_ref)?;
        inputs.push(lexongraph_embeddings_trait::EmbeddingInput {
            media_type: content.media_type.clone(),
            body: content.body.clone(),
        });
        contents.push(content);
    }

    let embeddings = lexongraph_embeddings_trait::EmbeddingProvider::embed_batch(
        &embedding_provider,
        &inputs,
        &embedding_spec,
    )
    .await
    .map_err(RuntimeError::Provider)?;

    let mut constructed = ConstructedBlocks::default();
    for ((item, content), embedding) in items.iter().zip(contents).zip(embeddings) {
        let block = build_leaf_block(
            VERSION_1,
            embedding_spec.clone(),
            vec![LeafEntry {
                embedding,
                metadata: item.metadata.clone(),
                content,
            }],
            None,
        )
        .map_err(|source| RuntimeError::ConstructLeafBlock {
            block_id: "<leaf>".into(),
            source,
        })?;
        let block = Block::Leaf(block);
        let serialized = lexongraph_block::serialize_block(&block).map_err(|source| {
            RuntimeError::SerializeIteratedBlock {
                block_id: "<leaf>".into(),
                source,
            }
        })?;
        constructed.block_ids.push(serialized.hash);
        constructed.blocks.push(serialized);
    }
    Ok(constructed)
}

#[allow(dead_code)]
async fn run_streaming_stage<EP>(
    resolver: LocalFilesystemContentResolver,
    embedding_provider: EP,
    config: StreamingStageConfig,
    replay_batches: Vec<ReplayBatch>,
    block_store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
    io: RuntimeIo<'_>,
) -> Result<BatchSummary, RuntimeError>
where
    EP: EmbeddingProvider + ClusteringFailureEmbeddingSource + Clone,
{
    let latest_failed_status = Arc::new(Mutex::new(None));
    let observer = Some(make_status_observer(
        Arc::clone(io.progress),
        Arc::clone(&latest_failed_status),
        io.planning_telemetry.cloned(),
    ));
    let total_batches = replay_batches.len();
    let total_items: usize = replay_batches.iter().map(|batch| batch.items.len()).sum();
    let clustering_failure_diagnostics = OnceLock::new();
    let diagnostics_resolver = resolver.clone();
    let diagnostics_embedding_provider = embedding_provider.clone();

    let resolved_profile = resolved_published_profile(&config.clustering)?;
    let mut indexer = StreamingIndexingRun::with_resolved_published_profile(
        resolver,
        embedding_provider,
        resolved_profile,
        embedding_spec.clone(),
        config.block_size_target,
    )?;
    if let Some(observer) = observer {
        indexer = indexer.with_observer(observer);
    }

    let mut completed_items = 0usize;
    for (batch_index, batch) in replay_batches.iter().enumerate() {
        if batch.items.is_empty() {
            continue;
        }
        let batch_number = batch_index + 1;
        let batch_item_count = batch.items.len();
        report_progress(
            io.progress,
            config.submission_progress_kind.started_message(
                batch_number,
                total_batches,
                batch_item_count,
                completed_items,
                total_items,
            ),
        );
        await_with_periodic_progress(
            indexer.ingest_batch(&batch.items),
            io.progress,
            PROGRESS_HEARTBEAT_INTERVAL,
            |elapsed| {
                config.submission_progress_kind.heartbeat_message(
                    batch_number,
                    total_batches,
                    batch_item_count,
                    completed_items,
                    total_items,
                    elapsed.as_millis(),
                )
            },
        )
        .await?;
        completed_items += batch_item_count;
        if let Some(message) = &batch.completion_message {
            report_progress(io.progress, message.clone());
        }
    }
    report_progress(
        io.progress,
        config
            .submission_progress_kind
            .handoff_message(total_batches, total_items),
    );
    let pass_report = indexer.finish_pass().map_err(|error| {
        clustering_failure_error(
            error,
            clustering_failure_diagnostics
                .get_or_init(|| {
                    build_clustering_failure_diagnostics(
                        &diagnostics_resolver,
                        &diagnostics_embedding_provider,
                        lock_unpoisoned(&latest_failed_status).as_ref(),
                        &config,
                        &replay_batches,
                        embedding_spec,
                    )
                })
                .as_ref(),
            io.progress,
        )
    })?;
    indexer.mark_planning_complete().map_err(|error| {
        clustering_failure_error(
            error,
            clustering_failure_diagnostics
                .get_or_init(|| {
                    build_clustering_failure_diagnostics(
                        &diagnostics_resolver,
                        &diagnostics_embedding_provider,
                        lock_unpoisoned(&latest_failed_status).as_ref(),
                        &config,
                        &replay_batches,
                        embedding_spec,
                    )
                })
                .as_ref(),
            io.progress,
        )
    })?;
    report_planning_pass_completion(
        io.progress,
        io.planning_telemetry,
        PlanningPassReport {
            completed_pass_count: pass_report.completed_pass_count,
            observed_item_count: pass_report.observed_item_count,
            requested_planning_cluster_count: pass_report.requested_planning_cluster_count,
            realized_planning_cluster_count: pass_report.realized_planning_cluster_count,
            planning_quality_metric: pass_report.planning_quality_metric,
            planning_balance_metric: pass_report.planning_balance_metric,
            planned_partition_count: pass_report.planned_partition_count,
            terminal_partition_count: pass_report.terminal_partition_count,
            hierarchy_depth: pass_report.hierarchy_depth,
        },
        &PlanningCompletionAction::Complete,
    )?;
    report_progress(
        io.progress,
        "Streaming planning complete; starting final materialization".into(),
    );
    let result = indexer
        .finalize(
            replay_batches.iter().map(|batch| batch.items.as_slice()),
            block_store,
        )
        .await
        .map_err(|error| {
            clustering_failure_error(
                error,
                clustering_failure_diagnostics
                    .get_or_init(|| {
                        build_clustering_failure_diagnostics(
                            &diagnostics_resolver,
                            &diagnostics_embedding_provider,
                            lock_unpoisoned(&latest_failed_status).as_ref(),
                            &config,
                            &replay_batches,
                            embedding_spec,
                        )
                    })
                    .as_ref(),
                io.progress,
            )
        })?;

    if result.block_ids.is_empty() {
        return Err(RuntimeError::EmptyDelegatedOutput);
    }

    if let Some(mutable_ref_store) = io.mutable_ref_store {
        let mut records = if config.stage.includes_ingestion() {
            replay_journal_records_from_block_ids(&result.block_ids, block_store, embedding_spec)?
        } else {
            Vec::new()
        };
        let input_block_count = if config.stage.includes_ingestion() {
            records
                .iter()
                .filter(|record| matches!(record, ReplayJournalRecord::ReplayInput { .. }))
                .count()
        } else {
            replay_input_count_from_batches(&replay_batches)
        };
        records.push(replay_journal_indexing_outcome_record(
            input_block_count,
            &result.block_ids,
            &result.root_id,
        ));
        let replay_journal_head_block_id = append_replay_journal_records_async(
            block_store.clone(),
            mutable_ref_store.clone(),
            records,
        )
        .await?;
        update_mutable_ref_store_async(
            mutable_ref_store.clone(),
            MutableRefStoreUpdate {
                current_root_block_id: Some(result.root_id.to_string()),
                replay_journal_head_block_id,
                metadata: io.mutable_ref_metadata.cloned(),
            },
        )
        .await?;
    }

    let mut block_ids = result
        .block_ids
        .into_iter()
        .map(|block_id| block_id.to_string())
        .collect::<Vec<_>>();
    block_ids.sort();
    block_ids.dedup();
    Ok(BatchSummary {
        root_id: result.root_id.to_string(),
        block_count: block_ids.len(),
        block_ids,
    })
}

async fn run_streaming_stage_externalized<EP>(
    resolver: LocalFilesystemContentResolver,
    embedding_provider: EP,
    config: StreamingStageConfig,
    replay_state: ExternalizedReplayState,
    block_store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
    io: RuntimeIo<'_>,
) -> Result<BatchSummary, RuntimeError>
where
    EP: EmbeddingProvider + ClusteringFailureEmbeddingSource + Clone,
{
    if uses_streaming_indexer_v2(&config.clustering) {
        return run_streaming_stage_externalized_v2(
            resolver,
            embedding_provider,
            config,
            replay_state,
            block_store,
            embedding_spec,
            io,
        )
        .await;
    }

    run_streaming_stage_externalized_legacy(
        resolver,
        embedding_provider,
        config,
        replay_state,
        block_store,
        embedding_spec,
        io,
    )
    .await
}

async fn run_streaming_stage_externalized_legacy<EP>(
    resolver: LocalFilesystemContentResolver,
    embedding_provider: EP,
    config: StreamingStageConfig,
    replay_state: ExternalizedReplayState,
    block_store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
    io: RuntimeIo<'_>,
) -> Result<BatchSummary, RuntimeError>
where
    EP: EmbeddingProvider + ClusteringFailureEmbeddingSource + Clone,
{
    let latest_failed_status = Arc::new(Mutex::new(None));
    let observer = Some(make_status_observer(
        Arc::clone(io.progress),
        Arc::clone(&latest_failed_status),
        io.planning_telemetry.cloned(),
    ));
    let total_batches = replay_state.total_batches();
    let total_items = replay_state.total_items;
    let clustering_failure_diagnostics = OnceLock::new();
    let diagnostics_resolver = resolver.clone();
    let diagnostics_embedding_provider = embedding_provider.clone();

    let resolved_profile = resolved_published_profile(&config.clustering)?;
    let mut indexer = StreamingIndexingRun::with_resolved_published_profile(
        resolver,
        embedding_provider,
        resolved_profile,
        embedding_spec.clone(),
        config.block_size_target,
    )?;
    if let Some(observer) = observer {
        indexer = indexer.with_observer(observer);
    }

    let mut completed_items = 0usize;
    let mut iterator = replay_state.batch_iterator()?;
    let mut batch_number = 0usize;
    while let Some(batch) = iterator.next_batch()? {
        if batch.items.is_empty() {
            continue;
        }
        batch_number += 1;
        let batch_item_count = batch.items.len();
        report_progress(
            io.progress,
            config.submission_progress_kind.started_message(
                batch_number,
                total_batches,
                batch_item_count,
                completed_items,
                total_items,
            ),
        );
        await_with_periodic_progress(
            indexer.ingest_batch(&batch.items),
            io.progress,
            PROGRESS_HEARTBEAT_INTERVAL,
            |elapsed| {
                config.submission_progress_kind.heartbeat_message(
                    batch_number,
                    total_batches,
                    batch_item_count,
                    completed_items,
                    total_items,
                    elapsed.as_millis(),
                )
            },
        )
        .await?;
        completed_items += batch_item_count;
        report_progress(
            io.progress,
            config.submission_progress_kind.completion_message(
                batch_number,
                total_batches,
                completed_items,
                total_items,
            ),
        );
    }

    report_progress(
        io.progress,
        config
            .submission_progress_kind
            .handoff_message(total_batches, total_items),
    );
    let pass_report = indexer.finish_pass().map_err(|error| {
        clustering_failure_error(
            error,
            clustering_failure_diagnostics
                .get_or_init(|| {
                    build_externalized_clustering_failure_diagnostics(
                        &diagnostics_resolver,
                        &diagnostics_embedding_provider,
                        lock_unpoisoned(&latest_failed_status).as_ref(),
                        &config,
                        &replay_state,
                        embedding_spec,
                    )
                })
                .as_ref(),
            io.progress,
        )
    })?;
    indexer.mark_planning_complete().map_err(|error| {
        clustering_failure_error(
            error,
            clustering_failure_diagnostics
                .get_or_init(|| {
                    build_externalized_clustering_failure_diagnostics(
                        &diagnostics_resolver,
                        &diagnostics_embedding_provider,
                        lock_unpoisoned(&latest_failed_status).as_ref(),
                        &config,
                        &replay_state,
                        embedding_spec,
                    )
                })
                .as_ref(),
            io.progress,
        )
    })?;
    report_planning_pass_completion(
        io.progress,
        io.planning_telemetry,
        PlanningPassReport {
            completed_pass_count: pass_report.completed_pass_count,
            observed_item_count: pass_report.observed_item_count,
            requested_planning_cluster_count: pass_report.requested_planning_cluster_count,
            realized_planning_cluster_count: pass_report.realized_planning_cluster_count,
            planning_quality_metric: pass_report.planning_quality_metric,
            planning_balance_metric: pass_report.planning_balance_metric,
            planned_partition_count: pass_report.planned_partition_count,
            terminal_partition_count: pass_report.terminal_partition_count,
            hierarchy_depth: pass_report.hierarchy_depth,
        },
        &PlanningCompletionAction::Complete,
    )?;
    report_progress(
        io.progress,
        "Streaming planning complete; starting final materialization".into(),
    );
    let result = indexer
        .finalize(replay_state.finalize_source()?, block_store)
        .await
        .map_err(|error| {
            clustering_failure_error(
                error,
                clustering_failure_diagnostics
                    .get_or_init(|| {
                        build_externalized_clustering_failure_diagnostics(
                            &diagnostics_resolver,
                            &diagnostics_embedding_provider,
                            lock_unpoisoned(&latest_failed_status).as_ref(),
                            &config,
                            &replay_state,
                            embedding_spec,
                        )
                    })
                    .as_ref(),
                io.progress,
            )
        })?;

    if result.block_ids.is_empty() {
        return Err(RuntimeError::EmptyDelegatedOutput);
    }

    if let Some(mutable_ref_store) = io.mutable_ref_store {
        let records = vec![replay_journal_indexing_outcome_record(
            replay_state.total_items,
            &result.block_ids,
            &result.root_id,
        )];
        let replay_journal_head_block_id = append_replay_journal_records_async(
            block_store.clone(),
            mutable_ref_store.clone(),
            records,
        )
        .await?;
        update_mutable_ref_store_async(
            mutable_ref_store.clone(),
            MutableRefStoreUpdate {
                current_root_block_id: Some(result.root_id.to_string()),
                replay_journal_head_block_id,
                metadata: io.mutable_ref_metadata.cloned(),
            },
        )
        .await?;
    }

    let mut block_ids = result
        .block_ids
        .into_iter()
        .map(|block_id| block_id.to_string())
        .collect::<Vec<_>>();
    block_ids.sort();
    block_ids.dedup();
    Ok(BatchSummary {
        root_id: result.root_id.to_string(),
        block_count: block_ids.len(),
        block_ids,
    })
}

async fn run_streaming_stage_externalized_v2<EP>(
    resolver: LocalFilesystemContentResolver,
    embedding_provider: EP,
    config: StreamingStageConfig,
    replay_state: ExternalizedReplayState,
    block_store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
    io: RuntimeIo<'_>,
) -> Result<BatchSummary, RuntimeError>
where
    EP: EmbeddingProvider + ClusteringFailureEmbeddingSource + Clone,
{
    let latest_failed_status = Arc::new(Mutex::new(None));
    let observer = Some(make_status_observer(
        Arc::clone(io.progress),
        Arc::clone(&latest_failed_status),
        io.planning_telemetry.cloned(),
    ));
    let total_batches = replay_state.total_batches();
    let total_items = replay_state.total_items;
    let clustering_failure_diagnostics = OnceLock::new();
    let diagnostics_resolver = resolver.clone();
    let diagnostics_embedding_provider = embedding_provider.clone();

    let mut indexer = StreamingIndexingRunV2::with_published_profile(
        resolver,
        embedding_provider,
        config.clustering.profile_version,
        embedding_spec.clone(),
        config.block_size_target,
        config
            .planner_state_root
            .as_deref()
            .ok_or(RuntimeError::MissingPlannerStateRoot)?,
    )?;
    if let Some(observer) = observer {
        indexer = indexer.with_observer(observer);
    }

    loop {
        let mut completed_items = 0usize;
        let mut iterator = Some(replay_state.batch_iterator()?);
        let mut batch_number = 0usize;
        let mut current_batch = iterator
            .as_mut()
            .expect("iterator must be available before replay starts")
            .load_next_batch()?;
        let mut prefetched_batches = VecDeque::new();
        let mut pending_prefetch = None;
        while let Some(batch) = current_batch {
            if batch.batch.items.is_empty() {
                current_batch = take_next_externalized_replay_batch(
                    &mut iterator,
                    &mut prefetched_batches,
                    &mut pending_prefetch,
                )
                .await?;
                continue;
            }
            iterator
                .as_ref()
                .expect("iterator must be available to publish current batch embeddings")
                .publish_batch_embeddings(&batch.embeddings_by_input_hash);
            batch_number += 1;
            let batch_item_count = batch.batch.items.len();
            report_progress(
                io.progress,
                config.submission_progress_kind.started_message(
                    batch_number,
                    total_batches,
                    batch_item_count,
                    completed_items,
                    total_items,
                ),
            );
            let requested_prefetch_count = EXTERNALIZED_REPLAY_PREFETCH_FUTURE_BATCHES
                .saturating_sub(prefetched_batches.len());
            if pending_prefetch.is_none() && requested_prefetch_count > 0 {
                pending_prefetch = Some(spawn_externalized_replay_batch_prefetches(
                    iterator
                        .take()
                        .expect("iterator must be available when spawning prefetch"),
                    requested_prefetch_count,
                ));
            }
            let ingest_result = await_with_periodic_progress(
                indexer.ingest_batch(&batch.batch.items),
                io.progress,
                PROGRESS_HEARTBEAT_INTERVAL,
                |elapsed| {
                    config.submission_progress_kind.heartbeat_message(
                        batch_number,
                        total_batches,
                        batch_item_count,
                        completed_items,
                        total_items,
                        elapsed.as_millis(),
                    )
                },
            )
            .await;
            ingest_result?;
            current_batch = take_next_externalized_replay_batch(
                &mut iterator,
                &mut prefetched_batches,
                &mut pending_prefetch,
            )
            .await?;
            completed_items += batch_item_count;
            report_progress(
                io.progress,
                config.submission_progress_kind.completion_message(
                    batch_number,
                    total_batches,
                    completed_items,
                    total_items,
                ),
            );
        }
        await_externalized_replay_batch_prefetch(
            &mut pending_prefetch,
            &mut iterator,
            &mut prefetched_batches,
        )
        .await?;
        iterator
            .as_ref()
            .expect("iterator must be available when clearing active batch embeddings")
            .clear_current_batch_embeddings();
        report_progress(
            io.progress,
            config
                .submission_progress_kind
                .handoff_message(total_batches, total_items),
        );
        let pass_report = indexer.finish_pass().map_err(|error| {
            clustering_failure_error(
                error,
                clustering_failure_diagnostics
                    .get_or_init(|| {
                        build_externalized_clustering_failure_diagnostics(
                            &diagnostics_resolver,
                            &diagnostics_embedding_provider,
                            lock_unpoisoned(&latest_failed_status).as_ref(),
                            &config,
                            &replay_state,
                            embedding_spec,
                        )
                    })
                    .as_ref(),
                io.progress,
            )
        })?;
        let should_replay = handle_v2_planning_pass_completion(
            io.progress,
            io.planning_telemetry,
            PlanningPassReport {
                completed_pass_count: pass_report.completed_pass_count,
                observed_item_count: pass_report.observed_item_count,
                requested_planning_cluster_count: pass_report.requested_planning_cluster_count,
                realized_planning_cluster_count: pass_report.realized_planning_cluster_count,
                planning_quality_metric: pass_report.planning_quality_metric,
                planning_balance_metric: pass_report.planning_balance_metric,
                planned_partition_count: pass_report.planned_partition_count,
                terminal_partition_count: pass_report.terminal_partition_count,
                hierarchy_depth: pass_report.hierarchy_depth,
            },
            indexer.mark_planning_complete(),
        )
        .map_err(|error| match error {
            RuntimeError::StreamingIndexer(source) => clustering_failure_error(
                source,
                clustering_failure_diagnostics
                    .get_or_init(|| {
                        build_externalized_clustering_failure_diagnostics(
                            &diagnostics_resolver,
                            &diagnostics_embedding_provider,
                            lock_unpoisoned(&latest_failed_status).as_ref(),
                            &config,
                            &replay_state,
                            embedding_spec,
                        )
                    })
                    .as_ref(),
                io.progress,
            ),
            other => other,
        })?;
        if !should_replay {
            break;
        }
    }
    report_progress(
        io.progress,
        "Streaming planning complete; starting final materialization".into(),
    );
    let result = indexer
        .finalize(replay_state.finalize_source()?, block_store)
        .await
        .map_err(|error| {
            clustering_failure_error(
                error,
                clustering_failure_diagnostics
                    .get_or_init(|| {
                        build_externalized_clustering_failure_diagnostics(
                            &diagnostics_resolver,
                            &diagnostics_embedding_provider,
                            lock_unpoisoned(&latest_failed_status).as_ref(),
                            &config,
                            &replay_state,
                            embedding_spec,
                        )
                    })
                    .as_ref(),
                io.progress,
            )
        })?;

    if result.block_ids.is_empty() {
        return Err(RuntimeError::EmptyDelegatedOutput);
    }

    if let Some(mutable_ref_store) = io.mutable_ref_store {
        let records = vec![replay_journal_indexing_outcome_record(
            replay_state.total_items,
            &result.block_ids,
            &result.root_id,
        )];
        let replay_journal_head_block_id = append_replay_journal_records_async(
            block_store.clone(),
            mutable_ref_store.clone(),
            records,
        )
        .await?;
        update_mutable_ref_store_async(
            mutable_ref_store.clone(),
            MutableRefStoreUpdate {
                current_root_block_id: Some(result.root_id.to_string()),
                replay_journal_head_block_id,
                metadata: io.mutable_ref_metadata.cloned(),
            },
        )
        .await?;
    }

    let mut block_ids = result
        .block_ids
        .into_iter()
        .map(|block_id| block_id.to_string())
        .collect::<Vec<_>>();
    block_ids.sort();
    block_ids.dedup();
    Ok(BatchSummary {
        root_id: result.root_id.to_string(),
        block_count: block_ids.len(),
        block_ids,
    })
}

async fn await_with_periodic_progress<Fut, T, M>(
    operation: Fut,
    progress: &ProgressReporter,
    heartbeat_interval: Duration,
    heartbeat_message: M,
) -> T
where
    Fut: Future<Output = T>,
    M: Fn(Duration) -> String,
{
    let start = std::time::Instant::now();
    let mut heartbeat = interval_at(TokioInstant::now() + heartbeat_interval, heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tokio::pin!(operation);
    loop {
        tokio::select! {
            biased;
            result = &mut operation => return result,
            _ = heartbeat.tick() => {
                report_progress(progress, heartbeat_message(start.elapsed()));
            }
        }
    }
}

#[allow(dead_code)]
fn load_replay_batches_from_store(
    store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
    replay_batch_size: usize,
    materialization_max_concurrency: usize,
    io: RuntimeIo<'_>,
) -> Result<(Vec<ReplayBatch>, StoredLeafEmbeddingProvider), RuntimeError> {
    let Some(mutable_ref_store) = io.mutable_ref_store else {
        return Err(RuntimeError::MissingReplayJournalHead {
            path: "<unresolved mutable ref>".into(),
        });
    };
    load_replay_batches_from_journal(
        store,
        embedding_spec,
        replay_batch_size,
        materialization_max_concurrency,
        mutable_ref_store,
        io.progress,
    )
}

fn for_each_replay_journal_record_newest_first(
    store: &ConfiguredBlockStore,
    mutable_ref_store: &MutableRefStoreLocation,
    mut visit: impl FnMut(&ReplayJournalRecord) -> Result<(), RuntimeError>,
) -> Result<(), RuntimeError> {
    let refs = load_mutable_ref_store(mutable_ref_store)?;
    let Some(mut current_block_id) = refs.replay_journal_head_block_id else {
        return Err(RuntimeError::MissingReplayJournalHead {
            path: mutable_ref_store_label(mutable_ref_store),
        });
    };

    let mut visited = HashSet::new();
    loop {
        if !visited.insert(current_block_id.clone()) {
            return Err(RuntimeError::InvalidReplayJournalHead {
                block_id: current_block_id,
                message: "replay journal chain contains a cycle".into(),
            });
        }
        let block_hash = parse_block_hash(&current_block_id).map_err(|error| {
            RuntimeError::InvalidReplayJournalHead {
                block_id: current_block_id.clone(),
                message: error.to_string(),
            }
        })?;
        let Some(decoded) = block_on_block_store_future(store.get_decoded(&block_hash))? else {
            return Err(RuntimeError::ReadReplayJournal {
                block_id: current_block_id,
                source: io::Error::new(ErrorKind::NotFound, "referenced journal block is missing"),
            });
        };
        let decoded = replay_journal_block_body_from_decoded(decoded, &block_hash.to_string())?;
        for entry in decoded.entries.iter().rev() {
            visit(entry)?;
        }
        match decoded.previous_block_id {
            Some(previous) => current_block_id = previous,
            None => break,
        }
    }
    Ok(())
}

fn collect_ordered_replay_block_ids_from_journal(
    store: &ConfiguredBlockStore,
    mutable_ref_store: &MutableRefStoreLocation,
    replay_order_scratch_root: &Path,
    progress: &ProgressReporter,
) -> Result<ReplayOrderStorage, RuntimeError> {
    collect_ordered_replay_block_ids_from_journal_with_limit(
        store,
        mutable_ref_store,
        replay_order_scratch_root,
        progress,
        REPLAY_ORDER_FLUSH_ENTRY_LIMIT,
    )
}

fn collect_ordered_replay_block_ids_from_journal_with_limit(
    store: &ConfiguredBlockStore,
    mutable_ref_store: &MutableRefStoreLocation,
    replay_order_scratch_root: &Path,
    progress: &ProgressReporter,
    flush_entry_limit: usize,
) -> Result<ReplayOrderStorage, RuntimeError> {
    prepare_replay_order_scratch_root(replay_order_scratch_root)?;
    let scratch_dir = tempfile::Builder::new()
        .prefix("replay-order-")
        .tempdir_in(replay_order_scratch_root)
        .map_err(|source| RuntimeError::PrepareReplayOrderScratchRoot {
            path: replay_order_scratch_root.display().to_string(),
            source,
        })?;
    let mut ordered_entries = Vec::with_capacity(flush_entry_limit.max(1));
    let mut run_paths = Vec::new();
    let mut scanned_inputs = 0usize;
    for_each_replay_journal_record_newest_first(store, mutable_ref_store, |record| {
        let ReplayJournalRecord::ReplayInput { block_id, .. } = record else {
            return Ok(());
        };
        let block_id =
            parse_block_hash(block_id).map_err(|error| RuntimeError::InvalidReplayJournalHead {
                block_id: block_id.clone(),
                message: error.to_string(),
            })?;
        let digest = replay_journal_record_sort_key_digest(record)
            .expect("replay input records should compute a replay ordering digest");
        ordered_entries.push(ReplayOrderEntry::new(block_id, digest));
        if ordered_entries.len() >= flush_entry_limit.max(1) {
            let run_index = run_paths.len();
            run_paths.push(flush_sorted_replay_order_run(
                scratch_dir.path(),
                run_index,
                &mut ordered_entries,
            )?);
        }
        scanned_inputs += 1;
        if scanned_inputs.is_multiple_of(10_000) {
            report_progress(
                progress,
                format!(
                    "Scanned {scanned_inputs} replay journal input(s) while preparing replay ordering scratch state"
                ),
            );
        }
        Ok(())
    })?;
    if !ordered_entries.is_empty() {
        let run_index = run_paths.len();
        run_paths.push(flush_sorted_replay_order_run(
            scratch_dir.path(),
            run_index,
            &mut ordered_entries,
        )?);
    }
    if run_paths.is_empty() {
        return Err(RuntimeError::NoClusterableBlocks);
    }
    report_progress(
        progress,
        format!(
            "Materialized {} replay-order scratch run(s) from {scanned_inputs} replay journal input(s)",
            run_paths.len()
        ),
    );
    let merged_entries_path = scratch_dir.path().join("ordered-replay.bin");
    let total_items = merge_sorted_replay_order_runs(&run_paths, &merged_entries_path)?;
    if total_items == 0 {
        return Err(RuntimeError::NoClusterableBlocks);
    }
    report_progress(
        progress,
        format!(
            "Loaded {} replay block id(s) from the replay journal without scanning the full block store",
            total_items
        ),
    );
    Ok(ReplayOrderStorage::new(
        scratch_dir,
        merged_entries_path,
        total_items,
    ))
}

fn flush_sorted_replay_order_run(
    scratch_dir: &Path,
    run_index: usize,
    entries: &mut Vec<ReplayOrderEntry>,
) -> Result<PathBuf, RuntimeError> {
    entries.sort_unstable();
    let path = scratch_dir.join(format!("run-{run_index:06}.bin"));
    let file = File::create(&path).map_err(|source| RuntimeError::WriteReplayOrderScratch {
        path: path.display().to_string(),
        source,
    })?;
    let mut writer = BufWriter::new(file);
    let mut previous_entry: Option<ReplayOrderEntry> = None;
    for entry in entries.iter().copied() {
        match previous_entry {
            Some(previous) if previous.block_id == entry.block_id => {
                if previous.digest != entry.digest {
                    return Err(RuntimeError::InvalidReplayJournalHead {
                        block_id: BlockHash::from_bytes(entry.block_id).to_string(),
                        message: "conflicting replay journal metadata references the same block id"
                            .into(),
                    });
                }
            }
            Some(previous) => {
                previous.write_to(&mut writer).map_err(|source| {
                    RuntimeError::WriteReplayOrderScratch {
                        path: path.display().to_string(),
                        source,
                    }
                })?;
                previous_entry = Some(entry);
            }
            None => previous_entry = Some(entry),
        }
    }
    if let Some(previous) = previous_entry {
        previous
            .write_to(&mut writer)
            .map_err(|source| RuntimeError::WriteReplayOrderScratch {
                path: path.display().to_string(),
                source,
            })?;
    }
    writer
        .flush()
        .map_err(|source| RuntimeError::WriteReplayOrderScratch {
            path: path.display().to_string(),
            source,
        })?;
    entries.clear();
    Ok(path)
}

fn merge_sorted_replay_order_runs(
    run_paths: &[PathBuf],
    merged_entries_path: &Path,
) -> Result<usize, RuntimeError> {
    if run_paths.len() <= REPLAY_ORDER_MERGE_FAN_IN {
        return merge_sorted_replay_order_run_group(run_paths, merged_entries_path);
    }

    let scratch_dir =
        merged_entries_path
            .parent()
            .ok_or_else(|| RuntimeError::WriteReplayOrderScratch {
                path: merged_entries_path.display().to_string(),
                source: io::Error::new(
                    ErrorKind::InvalidInput,
                    "merged replay-order path must have a parent directory",
                ),
            })?;
    let mut pass_index = 0usize;
    let mut current_paths = run_paths.to_vec();
    while current_paths.len() > REPLAY_ORDER_MERGE_FAN_IN {
        let mut next_paths = Vec::new();
        for (group_index, group) in current_paths.chunks(REPLAY_ORDER_MERGE_FAN_IN).enumerate() {
            let intermediate_path = scratch_dir.join(format!(
                "merge-pass-{pass_index:02}-run-{group_index:06}.bin"
            ));
            merge_sorted_replay_order_run_group(group, &intermediate_path)?;
            next_paths.push(intermediate_path);
        }
        current_paths = next_paths;
        pass_index += 1;
    }
    merge_sorted_replay_order_run_group(&current_paths, merged_entries_path)
}

fn merge_sorted_replay_order_run_group(
    run_paths: &[PathBuf],
    merged_entries_path: &Path,
) -> Result<usize, RuntimeError> {
    let output = File::create(merged_entries_path).map_err(|source| {
        RuntimeError::WriteReplayOrderScratch {
            path: merged_entries_path.display().to_string(),
            source,
        }
    })?;
    let mut writer = BufWriter::new(output);
    let mut readers = Vec::with_capacity(run_paths.len());
    let mut heap = BinaryHeap::new();

    for (run_index, path) in run_paths.iter().enumerate() {
        let file = File::open(path).map_err(|source| RuntimeError::ReadReplayOrderScratch {
            path: path.display().to_string(),
            source,
        })?;
        let mut reader = BufReader::new(file);
        if let Some(entry) = ReplayOrderEntry::read_from(&mut reader).map_err(|source| {
            RuntimeError::ReadReplayOrderScratch {
                path: path.display().to_string(),
                source,
            }
        })? {
            heap.push(ReplayOrderCursor { entry, run_index });
        }
        readers.push((path.clone(), reader));
    }

    let mut unique_entries = 0usize;
    let mut previous_entry: Option<ReplayOrderEntry> = None;
    while let Some(cursor) = heap.pop() {
        let entry = cursor.entry;
        if let Some(previous) = previous_entry {
            if previous.block_id == entry.block_id {
                if previous.digest != entry.digest {
                    return Err(RuntimeError::InvalidReplayJournalHead {
                        block_id: BlockHash::from_bytes(entry.block_id).to_string(),
                        message: "conflicting replay journal metadata references the same block id"
                            .into(),
                    });
                }
            } else {
                previous.write_to(&mut writer).map_err(|source| {
                    RuntimeError::WriteReplayOrderScratch {
                        path: merged_entries_path.display().to_string(),
                        source,
                    }
                })?;
                unique_entries += 1;
                previous_entry = Some(entry);
            }
        } else {
            previous_entry = Some(entry);
        }

        let (path, reader) = &mut readers[cursor.run_index];
        if let Some(next_entry) = ReplayOrderEntry::read_from(reader).map_err(|source| {
            RuntimeError::ReadReplayOrderScratch {
                path: path.display().to_string(),
                source,
            }
        })? {
            heap.push(ReplayOrderCursor {
                entry: next_entry,
                run_index: cursor.run_index,
            });
        }
    }

    if let Some(previous) = previous_entry {
        previous
            .write_to(&mut writer)
            .map_err(|source| RuntimeError::WriteReplayOrderScratch {
                path: merged_entries_path.display().to_string(),
                source,
            })?;
        unique_entries += 1;
    }
    writer
        .flush()
        .map_err(|source| RuntimeError::WriteReplayOrderScratch {
            path: merged_entries_path.display().to_string(),
            source,
        })?;
    Ok(unique_entries)
}

fn externalize_replay_batches_from_journal(
    store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
    replay_batch_size: usize,
    materialization_max_concurrency: usize,
    mutable_ref_store: &MutableRefStoreLocation,
    replay_order_scratch_root: &Path,
    progress: &ProgressReporter,
) -> Result<
    (
        ExternalizedReplayState,
        ExternalizedStoredLeafEmbeddingProvider,
    ),
    RuntimeError,
> {
    let replay_order = collect_ordered_replay_block_ids_from_journal(
        store,
        mutable_ref_store,
        replay_order_scratch_root,
        progress,
    )?;
    let total_items = replay_order.total_items();
    let current_batch_embeddings = Arc::new(Mutex::new(HashMap::new()));
    let fallback_embeddings = Arc::new(Mutex::new(None));
    Ok((
        ExternalizedReplayState {
            replay_order: replay_order.clone(),
            total_items,
            batch_size: replay_batch_size.max(1),
            materialization_max_concurrency: materialization_max_concurrency.max(1),
            block_store: store.clone(),
            embedding_spec: embedding_spec.clone(),
            current_batch_embeddings: Arc::clone(&current_batch_embeddings),
        },
        ExternalizedStoredLeafEmbeddingProvider {
            block_store: store.clone(),
            embedding_spec: embedding_spec.clone(),
            replay_order,
            current_batch_embeddings,
            fallback_embeddings,
        },
    ))
}

async fn externalize_replay_batches_from_store_async(
    store: ConfiguredBlockStore,
    embedding_spec: EmbeddingSpec,
    replay_batch_size: usize,
    materialization_max_concurrency: usize,
    mutable_ref_store: MutableRefStoreLocation,
    replay_order_scratch_root: PathBuf,
    progress: ProgressReporter,
) -> Result<
    (
        ExternalizedReplayState,
        ExternalizedStoredLeafEmbeddingProvider,
    ),
    RuntimeError,
> {
    tokio::task::spawn_blocking(move || {
        externalize_replay_batches_from_journal(
            &store,
            &embedding_spec,
            replay_batch_size,
            materialization_max_concurrency,
            &mutable_ref_store,
            &replay_order_scratch_root,
            &progress,
        )
    })
    .await
    .map_err(RuntimeError::BlockingMutableRefTaskJoin)?
}

#[allow(dead_code)]
async fn load_replay_batches_from_store_async(
    store: ConfiguredBlockStore,
    embedding_spec: EmbeddingSpec,
    replay_batch_size: usize,
    materialization_max_concurrency: usize,
    mutable_ref_store: MutableRefStoreLocation,
    progress: ProgressReporter,
) -> Result<(Vec<ReplayBatch>, StoredLeafEmbeddingProvider), RuntimeError> {
    tokio::task::spawn_blocking(move || {
        let io = RuntimeIo {
            mutable_ref_store: Some(&mutable_ref_store),
            mutable_ref_metadata: None,
            planning_telemetry: None,
            progress: &progress,
        };
        load_replay_batches_from_store(
            &store,
            &embedding_spec,
            replay_batch_size,
            materialization_max_concurrency,
            io,
        )
    })
    .await
    .map_err(RuntimeError::BlockingMutableRefTaskJoin)?
}

#[allow(dead_code)]
fn load_replay_batches_from_journal(
    store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
    replay_batch_size: usize,
    materialization_max_concurrency: usize,
    mutable_ref_store: &MutableRefStoreLocation,
    progress: &ProgressReporter,
) -> Result<(Vec<ReplayBatch>, StoredLeafEmbeddingProvider), RuntimeError> {
    let replay_order_scratch_root = std::env::temp_dir().join("lexonarchivebuilder-replay-order");
    let replay_order = collect_ordered_replay_block_ids_from_journal(
        store,
        mutable_ref_store,
        replay_order_scratch_root.as_path(),
        progress,
    )?;
    let mut embeddings_by_input_hash = HashMap::new();
    let mut replay_batches = Vec::new();
    let batch_size = replay_batch_size.max(1);
    let mut reader = replay_order.open_reader()?;
    loop {
        let entries = reader.read_next_entries(batch_size)?;
        if entries.is_empty() {
            break;
        }
        let batch = replay_batch_from_entries(
            &entries,
            store,
            embedding_spec,
            materialization_max_concurrency,
        )?;
        for (input_hash, embedding) in &batch.embeddings_by_input_hash {
            embeddings_by_input_hash.insert(*input_hash, embedding.clone());
        }
        replay_batches.push(batch.batch);
    }
    let replay_item_count: usize = replay_batches.iter().map(|batch| batch.items.len()).sum();
    if replay_item_count == 0 {
        return Err(RuntimeError::NoClusterableBlocks);
    }
    annotate_submission_progress_batches(&mut replay_batches, SubmissionProgressKind::Replay);
    Ok((
        replay_batches,
        StoredLeafEmbeddingProvider {
            embeddings_by_input_hash: Arc::new(embeddings_by_input_hash),
        },
    ))
}

fn replay_embedding_input_hash(
    validated: &lexongraph_block::ValidatedBlock,
    embedding_spec: &EmbeddingSpec,
) -> Result<Option<BlockHash>, RuntimeError> {
    let Block::Leaf(block) = &validated.block else {
        return Ok(None);
    };
    if block.level != 0
        || block.embedding_spec != *embedding_spec
        || block.embedding_spec.dims == 0
        || block.entries.len() != 1
        || block.entries[0].embedding.is_empty()
    {
        return Ok(None);
    }
    let content = &block.entries[0].content;
    Ok(Some(hash_embedding_input(&EmbeddingInput {
        media_type: content.media_type.clone(),
        body: content.body.clone(),
    })))
}

fn replay_item_from_validated_block(
    validated: &lexongraph_block::ValidatedBlock,
    embedding_spec: &EmbeddingSpec,
) -> Result<Option<ReplayedLeaf>, RuntimeError> {
    let Block::Leaf(block) = &validated.block else {
        return Ok(None);
    };
    if block.level != 0
        || block.embedding_spec != *embedding_spec
        || block.embedding_spec.dims == 0
        || block.entries.len() != 1
        || block.entries[0].embedding.is_empty()
    {
        return Ok(None);
    }

    let entry = &block.entries[0];
    let fields = metadata_to_text_map(&entry.metadata);
    let Some(source_kind) = fields.get("source_kind").map(String::as_str) else {
        return Err(RuntimeError::MissingReplayMetadata {
            block_id: validated.hash.to_string(),
        });
    };
    let content_ref = match source_kind {
        "document" => {
            let Some(source_path) = fields.get("source_path") else {
                return Err(RuntimeError::MissingReplayMetadata {
                    block_id: validated.hash.to_string(),
                });
            };
            ContentRef::StoredReplay {
                media_type: entry.content.media_type.clone(),
                body: entry.content.body.clone(),
                identity: ReplayIdentity::Document {
                    source_path: source_path.clone(),
                },
            }
        }
        "email" => {
            let Some(email_artifact_ref) = fields.get("email_artifact_ref") else {
                return Err(RuntimeError::MissingReplayMetadata {
                    block_id: validated.hash.to_string(),
                });
            };
            let Some(chunk_index) = fields
                .get("chunk_index")
                .and_then(|value| value.parse().ok())
            else {
                return Err(RuntimeError::MissingReplayMetadata {
                    block_id: validated.hash.to_string(),
                });
            };
            ContentRef::StoredReplay {
                media_type: entry.content.media_type.clone(),
                body: entry.content.body.clone(),
                identity: ReplayIdentity::EmailChunk {
                    email_artifact_ref: email_artifact_ref.clone(),
                    chunk_index,
                },
            }
        }
        _ => return Ok(None),
    };

    Ok(Some((
        IndexItem {
            metadata: entry.metadata.clone(),
            content_ref,
        },
        entry.embedding.clone(),
    )))
}

fn make_status_observer(
    progress: ProgressReporter,
    latest_failed_status: Arc<Mutex<Option<StreamingIndexingStatus>>>,
    planning_telemetry: Option<PlanningTelemetryContext>,
) -> StreamingIndexingStatusObserver {
    Arc::new(move |status| {
        if status.state == StreamingIndexingStatusState::Failed {
            let mut captured = lock_unpoisoned(&latest_failed_status);
            match captured.as_ref() {
                Some(existing) if !prefer_failed_status(&status, existing) => {}
                _ => *captured = Some(status.clone()),
            }
        }
        let diagnosis_message = if let Some(telemetry) = planning_telemetry.as_ref() {
            let (record, diagnosis_message) = telemetry.project_planning_status(&status);
            if let Some(record) = record
                && let Err(error) = telemetry.write_json_record(&record)
            {
                report_progress(&progress, error);
            }
            diagnosis_message
        } else {
            None
        };
        let base_message = format_indexing_status(status);
        report_progress(&progress, base_message);
        if let Some(diagnosis_message) = diagnosis_message {
            report_progress(&progress, diagnosis_message);
        }
    })
}

fn failed_status_specificity(status: &StreamingIndexingStatus) -> usize {
    match status.phase {
        StreamingIndexingPhase::PlanningPass { .. } => 0,
        StreamingIndexingPhase::FinalMaterializationReplay => 1,
        StreamingIndexingPhase::HierarchyPlanning { .. } => 2,
        StreamingIndexingPhase::BottomUpAssembly { .. } => 2,
    }
}

fn prefer_failed_status(
    candidate: &StreamingIndexingStatus,
    existing: &StreamingIndexingStatus,
) -> bool {
    let candidate_specificity = failed_status_specificity(candidate);
    let existing_specificity = failed_status_specificity(existing);
    candidate_specificity > existing_specificity
        || (candidate_specificity == existing_specificity
            && candidate.item_count <= existing.item_count)
}

fn format_planning_stage(stage: PlanningStage) -> &'static str {
    match stage {
        PlanningStage::Single => "single-stage planning",
        PlanningStage::Coarse => "coarse planning",
        PlanningStage::Fine => "fine planning",
        PlanningStage::Custom => "custom planning",
    }
}

fn format_completed_of_total(
    completed: usize,
    total: Option<usize>,
    unit_label: &str,
) -> Option<String> {
    total.map(|total| format!("; completed {completed} of {total} {unit_label}"))
}

fn format_pending_partition_message(partition: &StreamingV2PendingPartitionStatus) -> String {
    let mut message = format!(
        "{} expects {} item(s)",
        partition.partition_path, partition.expected_item_count
    );
    if let Some(observed) = partition.observed_replay_progress {
        message.push_str(&format!(", observed {observed}"));
    }
    if let Some(subphase) = partition.trainer_subphase {
        message.push_str(&format!(", subphase {}", trainer_subphase_label(subphase)));
    }
    if let Some(bucket_fill_counts) = partition.routing_bucket_fill_counts.as_ref() {
        let joined = bucket_fill_counts
            .iter()
            .map(|count| count.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        message.push_str(&format!(", bucket fill [{joined}]"));
    }
    message
}

fn format_planning_status_detail_suffix(status: &StreamingIndexingStatus) -> String {
    let mut suffix = String::new();
    let pending_partition_count = status
        .pending_partition_count
        .or_else(|| status.v2_pending_partitions.as_ref().map(Vec::len));
    if let Some(count) = pending_partition_count {
        suffix.push_str(&format!("; pending partition(s) {count}"));
    }
    if let Some(partitions) = status.v2_pending_partitions.as_ref()
        && !partitions.is_empty()
    {
        let preview = partitions
            .iter()
            .take(2)
            .map(format_pending_partition_message)
            .collect::<Vec<_>>()
            .join(" | ");
        suffix.push_str(&format!("; pending detail {preview}"));
        if partitions.len() > 2 {
            suffix.push_str(&format!(" | +{} more", partitions.len() - 2));
        }
    }
    if let Some(stall) = status.suspected_stall.as_ref() {
        suffix.push_str(&format!(
            "; suspected stall {} for {} ms",
            suspected_stall_reason_label(stall.reason),
            stall.duration_without_progress.as_millis()
        ));
    }
    suffix
}

fn format_indexing_status(status: StreamingIndexingStatus) -> String {
    let elapsed_ms = status.elapsed.as_millis();
    match (&status.phase, status.state) {
        (
            StreamingIndexingPhase::PlanningPass { pass_number },
            StreamingIndexingStatusState::Started,
        ) => format!(
            "Planning pass {pass_number} started for {} item(s){}",
            status.item_count,
            format_planning_status_detail_suffix(&status)
        ),
        (
            StreamingIndexingPhase::PlanningPass { pass_number },
            StreamingIndexingStatusState::InProgress,
        ) => {
            let progress_suffix = format_completed_of_total(
                status.completed_unit_count,
                status.phase_total_unit_count,
                "pass item(s)",
            )
            .unwrap_or_default();
            format!(
                "Planning pass {pass_number} still running after {elapsed_ms} ms for {} item(s){}{}",
                status.item_count,
                progress_suffix,
                format_planning_status_detail_suffix(&status)
            )
        }
        (
            StreamingIndexingPhase::PlanningPass { pass_number },
            StreamingIndexingStatusState::Completed,
        ) => {
            let progress_suffix = format_completed_of_total(
                status.completed_unit_count,
                status.phase_total_unit_count,
                "pass item(s)",
            )
            .unwrap_or_default();
            format!(
                "Planning pass {pass_number} completed in {elapsed_ms} ms for {} item(s){}{}",
                status.item_count,
                progress_suffix,
                format_planning_status_detail_suffix(&status)
            )
        }
        (
            StreamingIndexingPhase::PlanningPass { pass_number },
            StreamingIndexingStatusState::Failed,
        ) => format!(
            "Planning pass {pass_number} failed after {elapsed_ms} ms{}: {}",
            format_planning_status_detail_suffix(&status),
            status.error.unwrap_or_else(|| "unknown error".into())
        ),
        (
            StreamingIndexingPhase::HierarchyPlanning { stage },
            StreamingIndexingStatusState::Started,
        ) => {
            format!(
                "{} started for {} item(s)",
                format_planning_stage(*stage),
                status.item_count,
            )
        }
        (
            StreamingIndexingPhase::HierarchyPlanning { stage },
            StreamingIndexingStatusState::InProgress,
        ) => {
            format!(
                "{} still running after {elapsed_ms} ms; processed {} stage-local item(s)",
                format_planning_stage(*stage),
                status.completed_unit_count,
            )
        }
        (
            StreamingIndexingPhase::HierarchyPlanning { stage },
            StreamingIndexingStatusState::Completed,
        ) => {
            format!(
                "{} completed in {elapsed_ms} ms after processing {} stage-local item(s)",
                format_planning_stage(*stage),
                status.completed_unit_count,
            )
        }
        (
            StreamingIndexingPhase::HierarchyPlanning { stage },
            StreamingIndexingStatusState::Failed,
        ) => {
            format!(
                "{} failed after {elapsed_ms} ms; processed {} stage-local item(s): {}",
                format_planning_stage(*stage),
                status.completed_unit_count,
                status.error.unwrap_or_else(|| "unknown error".into())
            )
        }
        (
            StreamingIndexingPhase::FinalMaterializationReplay,
            StreamingIndexingStatusState::Started,
        ) => {
            format!(
                "Final materialization replay started for {} item(s)",
                status.item_count
            )
        }
        (
            StreamingIndexingPhase::FinalMaterializationReplay,
            StreamingIndexingStatusState::InProgress,
        ) => {
            let progress_suffix = format_completed_of_total(
                status.completed_unit_count,
                status.phase_total_unit_count,
                "replay item(s)",
            )
            .unwrap_or_default();
            format!(
                "Final materialization replay still running after {elapsed_ms} ms for {} item(s){}",
                status.item_count, progress_suffix
            )
        }
        (
            StreamingIndexingPhase::FinalMaterializationReplay,
            StreamingIndexingStatusState::Completed,
        ) => {
            let progress_suffix = format_completed_of_total(
                status.completed_unit_count,
                status.phase_total_unit_count,
                "replay item(s)",
            )
            .unwrap_or_default();
            format!(
                "Final materialization replay completed in {elapsed_ms} ms for {} item(s){}",
                status.item_count, progress_suffix
            )
        }
        (
            StreamingIndexingPhase::FinalMaterializationReplay,
            StreamingIndexingStatusState::Failed,
        ) => format!(
            "Final materialization replay failed after {elapsed_ms} ms: {}",
            status.error.unwrap_or_else(|| "unknown error".into())
        ),
        (
            StreamingIndexingPhase::BottomUpAssembly { layer_index },
            StreamingIndexingStatusState::Started,
        ) => match status.phase_total_unit_count {
            Some(group_total) => format!(
                "Bottom-up assembly for layer {layer_index} started for {} input block(s) across {group_total} group(s)",
                status.item_count
            ),
            None => format!(
                "Bottom-up assembly for layer {layer_index} started for {} input block(s) across an unknown group total",
                status.item_count
            ),
        },
        (
            StreamingIndexingPhase::BottomUpAssembly { layer_index },
            StreamingIndexingStatusState::InProgress,
        ) => match status.phase_total_unit_count {
            Some(group_total) => format!(
                "Bottom-up assembly for layer {layer_index} still running after {elapsed_ms} ms; completed {} of {group_total} group(s) from {} input block(s)",
                status.completed_unit_count, status.item_count
            ),
            None => format!(
                "Bottom-up assembly for layer {layer_index} still running after {elapsed_ms} ms; completed {} group(s) so far from {} input block(s)",
                status.completed_unit_count, status.item_count
            ),
        },
        (
            StreamingIndexingPhase::BottomUpAssembly { layer_index },
            StreamingIndexingStatusState::Completed,
        ) => match status.phase_total_unit_count {
            Some(group_total) => format!(
                "Bottom-up assembly for layer {layer_index} completed in {elapsed_ms} ms: built {} of {group_total} group(s) from {} input block(s)",
                status.completed_unit_count, status.item_count
            ),
            None => format!(
                "Bottom-up assembly for layer {layer_index} completed in {elapsed_ms} ms: built {} group(s) from {} input block(s)",
                status.completed_unit_count, status.item_count
            ),
        },
        (
            StreamingIndexingPhase::BottomUpAssembly { layer_index },
            StreamingIndexingStatusState::Failed,
        ) => match status.phase_total_unit_count {
            Some(group_total) => format!(
                "Bottom-up assembly for layer {layer_index} failed after {elapsed_ms} ms; completed {} of {group_total} group(s) from {} input block(s): {}",
                status.completed_unit_count,
                status.item_count,
                status.error.unwrap_or_else(|| "unknown error".into())
            ),
            None => format!(
                "Bottom-up assembly for layer {layer_index} failed after {elapsed_ms} ms; completed {} group(s) from {} input block(s): {}",
                status.completed_unit_count,
                status.item_count,
                status.error.unwrap_or_else(|| "unknown error".into())
            ),
        },
    }
}

fn report_progress(progress: &ProgressReporter, message: String) {
    progress.as_ref()(message);
}

fn hash_embedding_content(media_type: &str, body: &[u8]) -> BlockHash {
    use sha2::{Digest, Sha256};

    let mut digest = Sha256::new();
    digest.update(media_type.as_bytes());
    digest.update([0]);
    digest.update(body);
    BlockHash::from_bytes(digest.finalize().into())
}

fn hash_bytes(bytes: &[u8]) -> BlockHash {
    use sha2::{Digest, Sha256};

    BlockHash::from_bytes(Sha256::digest(bytes).into())
}

fn hash_embedding_input(input: &EmbeddingInput) -> BlockHash {
    hash_embedding_content(&input.media_type, &input.body)
}

fn placeholder_root_id() -> String {
    INGESTION_ONLY_ROOT_ID_PLACEHOLDER.to_string()
}

fn persist_staged_blocks(
    blocks: &[SerializedBlock],
    store: &dyn lexongraph_block_store::BlockStore,
) -> Result<(), RuntimeError> {
    for block in blocks {
        let validated = deserialize_block(&block.bytes, &block.hash).map_err(|source| {
            RuntimeError::DeserializeStagedBlock {
                block_id: block.hash.to_string(),
                source,
            }
        })?;
        let persisted = block_on_block_store_future(store.put(&validated.block))?;
        if persisted != block.hash {
            return Err(RuntimeError::StagedBlockHashMismatch {
                expected: block.hash.to_string(),
                actual: persisted.to_string(),
            });
        }
    }
    Ok(())
}

pub fn write_summary_file(path: &Path, summary: &BatchSummary) -> Result<(), RuntimeError> {
    if let Some(parent) = parent_directory_to_create(path) {
        fs::create_dir_all(parent).map_err(|source| RuntimeError::WriteSummary {
            path: path.display().to_string(),
            source,
        })?;
    }
    let rendered = serde_json::to_vec_pretty(summary)?;
    fs::write(path, rendered).map_err(|source| RuntimeError::WriteSummary {
        path: path.display().to_string(),
        source,
    })
}

fn adjacent_output_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new(""))
}

fn output_anchor_path<'a>(request_path: &'a Path, summary_out: Option<&'a Path>) -> &'a Path {
    summary_out.unwrap_or(request_path)
}

fn parent_directory_to_create(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
}

pub fn clustering_failure_diagnostics_path(
    request_path: &Path,
    summary_out: Option<&Path>,
) -> PathBuf {
    let anchor_path = output_anchor_path(request_path, summary_out);
    let base_name = anchor_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(|stem| format!("{stem}.clustering-failure-diagnostics.json"))
        .unwrap_or_else(|| "clustering-failure-diagnostics.json".to_string());
    adjacent_output_directory(anchor_path).join(base_name)
}

pub fn planning_pass_telemetry_path(request_path: &Path, summary_out: Option<&Path>) -> PathBuf {
    let anchor_path = output_anchor_path(request_path, summary_out);
    let base_name = anchor_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(|stem| format!("{stem}.planning-pass-telemetry.jsonl"))
        .unwrap_or_else(|| "planning-pass-telemetry.jsonl".to_string());
    adjacent_output_directory(anchor_path).join(base_name)
}

pub fn replay_order_scratch_root_path(request_path: &Path, summary_out: Option<&Path>) -> PathBuf {
    let anchor_path = output_anchor_path(request_path, summary_out);
    let base_name = anchor_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(|stem| format!("{stem}.replay-order"))
        .unwrap_or_else(|| "replay-order".to_string());
    adjacent_output_directory(anchor_path).join(base_name)
}

pub fn planner_state_root_path(request_path: &Path, summary_out: Option<&Path>) -> PathBuf {
    let anchor_path = output_anchor_path(request_path, summary_out);
    let base_name = anchor_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(|stem| format!("{stem}.planner-state"))
        .unwrap_or_else(|| "planner-state".to_string());
    adjacent_output_directory(anchor_path).join(base_name)
}

fn replay_order_scratch_root_for_request_dir(request_dir: &Path) -> PathBuf {
    request_dir.join("replay-order")
}

fn planner_state_root_for_request_dir(request_dir: &Path) -> PathBuf {
    request_dir.join("planner-state")
}

fn prepare_writable_directory(path: &Path, probe_prefix: &str) -> io::Result<()> {
    fs::create_dir_all(path)?;
    let mut probe = tempfile::Builder::new()
        .prefix(probe_prefix)
        .tempfile_in(path)?;
    probe.write_all(&[0]).and_then(|_| probe.flush())
}

fn prepare_planner_state_root(path: &Path) -> Result<(), RuntimeError> {
    prepare_writable_directory(path, ".planner-state-write-probe-").map_err(|source| {
        RuntimeError::PreparePlannerStateRoot {
            path: path.display().to_string(),
            source,
        }
    })
}

fn prepare_replay_order_scratch_root(path: &Path) -> Result<(), RuntimeError> {
    prepare_writable_directory(path, ".replay-order-write-probe-").map_err(|source| {
        RuntimeError::PrepareReplayOrderScratchRoot {
            path: path.display().to_string(),
            source,
        }
    })
}

pub fn write_clustering_failure_diagnostics_file(
    path: &Path,
    diagnostics: &ClusteringFailureDiagnostics,
) -> Result<(), RuntimeError> {
    if let Some(parent) = parent_directory_to_create(path) {
        fs::create_dir_all(parent).map_err(|source| RuntimeError::WriteClusteringDiagnostics {
            path: path.display().to_string(),
            source,
        })?;
    }
    let rendered = serde_json::to_vec_pretty(diagnostics)
        .map_err(|source| RuntimeError::RenderClusteringDiagnostics { source })?;
    fs::write(path, rendered).map_err(|source| RuntimeError::WriteClusteringDiagnostics {
        path: path.display().to_string(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::thread;
    use std::time::{Duration, Instant};

    use ciborium::value::Value;
    use lexongraph_block::Content;
    use lexongraph_streaming_indexer::{
        PUBLISHED_PROFILE_V0_1_0, PUBLISHED_PROFILE_V0_7_0, PublishedProfileVersion,
    };
    use serde_json::json;
    use tempfile::tempdir;

    use crate::config::{
        BatchItemConfig, ClusteringConfigOverrides, EmbeddingSpecConfig, EnvironmentConfig,
        ExecutionStage, LocalEmbeddingConfig,
    };

    use super::*;

    fn put_block(store: &ConfiguredBlockStore, block: &Block) -> BlockHash {
        crate::block_store::block_on_block_store_future(store.put(block)).unwrap()
    }

    fn get_block(
        store: &ConfiguredBlockStore,
        block_id: &BlockHash,
    ) -> Option<lexongraph_block::ValidatedBlock> {
        crate::block_store::block_on_block_store_future(store.get(block_id)).unwrap()
    }

    fn local_block_path(root: &Path, block_id: &str) -> PathBuf {
        root.join(&block_id[..2])
            .join(&block_id[2..4])
            .join(format!("{block_id}.cbor"))
    }

    fn replay_order_storage_for_entries(entries: &[ReplayOrderEntry]) -> ReplayOrderStorage {
        let scratch_dir = tempdir().unwrap();
        let entries_path = scratch_dir.path().join("ordered-replay.bin");
        let file = File::create(&entries_path).unwrap();
        let mut writer = BufWriter::new(file);
        for entry in entries.iter().copied() {
            entry.write_to(&mut writer).unwrap();
        }
        writer.flush().unwrap();
        ReplayOrderStorage::new(scratch_dir, entries_path, entries.len())
    }

    fn replay_order_storage_for_block_ids(block_ids: &[BlockHash]) -> ReplayOrderStorage {
        let entries = block_ids
            .iter()
            .copied()
            .map(|block_id| ReplayOrderEntry::new(block_id, BlockHash::from_bytes([0u8; 32])))
            .collect::<Vec<_>>();
        replay_order_storage_for_entries(&entries)
    }

    fn test_planning_pass_report(
        completed_pass_count: usize,
        observed_item_count: usize,
    ) -> PlanningPassReport {
        PlanningPassReport {
            completed_pass_count,
            observed_item_count,
            requested_planning_cluster_count: Some(64),
            realized_planning_cluster_count: Some(48),
            planning_quality_metric: 0.25,
            planning_balance_metric: 0.125,
            planned_partition_count: 12,
            terminal_partition_count: 9,
            hierarchy_depth: 4,
        }
    }

    #[test]
    fn mutable_ref_table_lookup_filter_escapes_single_quotes() {
        assert_eq!(
            mutable_ref_table_lookup_filter("part'ition", "row'key"),
            "PartitionKey eq 'part''ition' and RowKey eq 'row''key'"
        );
    }

    #[test]
    fn incomplete_v2_planning_transition_tolerates_upstream_detail_suffixes() {
        assert!(is_incomplete_v2_planning_transition(
            &StreamingIndexerError::InvalidLifecycleTransition(format!(
                "{V2_PLANNING_NEEDS_ROUTED_OR_TERMINAL_PREFIX}: root partition is still pending"
            ))
        ));
        assert!(is_incomplete_v2_planning_transition(
            &StreamingIndexerError::InvalidLifecycleTransition(format!(
                "{V2_PLANNING_NEEDS_CHILDREN_PREFIX} for routed partition root"
            ))
        ));
        assert!(!is_incomplete_v2_planning_transition(
            &StreamingIndexerError::InvalidLifecycleTransition(
                "planning completion requires a baseline".into()
            )
        ));
    }

    #[test]
    fn v2_planning_completion_action_replays_known_incomplete_transition() {
        assert_eq!(
            v2_planning_completion_action(
                1,
                Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                    "{V2_PLANNING_NEEDS_ROUTED_OR_TERMINAL_PREFIX}: root partition is still pending"
                )))
            )
            .unwrap(),
            PlanningCompletionAction::ReplayRequired(format!(
                "Planning pass 1 requires another full replay pass before v2 planning can complete: invalid lifecycle transition: {V2_PLANNING_NEEDS_ROUTED_OR_TERMINAL_PREFIX}: root partition is still pending"
            ))
        );
    }

    #[test]
    fn v2_planning_completion_action_propagates_other_transition_errors() {
        let error =
            StreamingIndexerError::InvalidLifecycleTransition("no baseline established".into());
        assert_eq!(
            v2_planning_completion_action(1, Err(error.clone())).unwrap_err(),
            error
        );
    }

    #[test]
    fn handle_v2_planning_pass_completion_replays_then_completes() {
        let progress_messages = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_progress = Arc::clone(&progress_messages);
        let progress: ProgressReporter = Arc::new(move |message| {
            captured_progress.lock().unwrap().push(message);
        });

        assert!(
            handle_v2_planning_pass_completion(
                &progress,
                None,
                test_planning_pass_report(1, 3),
                Err(StreamingIndexerError::InvalidLifecycleTransition(format!(
                    "{V2_PLANNING_NEEDS_ROUTED_OR_TERMINAL_PREFIX}: root partition is still pending"
                )))
            )
            .unwrap()
        );
        assert!(
            !handle_v2_planning_pass_completion(
                &progress,
                None,
                test_planning_pass_report(2, 3),
                Ok(())
            )
            .unwrap()
        );

        let progress_messages = progress_messages.lock().unwrap();
        assert_eq!(
            progress_messages[0],
            "Completed planning pass 1 over 3 item(s)"
        );
        assert!(progress_messages[1].contains(
            "Planning pass 1 requires another full replay pass before v2 planning can complete"
        ));
        assert!(
            progress_messages[1].contains("requires every v2 partition to be terminal or routed")
        );
        assert_eq!(
            progress_messages[2],
            "Completed planning pass 2 over 3 item(s)"
        );
    }

    #[test]
    fn planning_pass_diagnosis_marks_unchanged_replay_as_not_converging() {
        let previous = test_planning_pass_report(1, 3);
        let current = test_planning_pass_report(2, 3);
        let diagnosis = planning_pass_diagnosis(
            Some(previous),
            current,
            &PlanningCompletionAction::ReplayRequired("still pending".into()),
        );
        assert_eq!(diagnosis.verdict, PlanningConvergenceVerdict::NotConverging);
        assert!(
            diagnosis
                .evidence_summary
                .contains("terminal partitions stayed at 9")
        );
        assert!(
            diagnosis
                .evidence_summary
                .contains("non-terminal gap stayed at 3")
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn test_streaming_status(
        phase: StreamingIndexingPhase,
        state: StreamingIndexingStatusState,
        item_count: usize,
        phase_total_unit_count: Option<usize>,
        completed_unit_count: usize,
        remaining_unit_count: Option<usize>,
        elapsed: Duration,
        error: Option<&str>,
    ) -> StreamingIndexingStatus {
        StreamingIndexingStatus {
            phase,
            state,
            item_count,
            phase_total_unit_count,
            completed_unit_count,
            remaining_unit_count,
            progress_unit_kind: None,
            discovered_unit_count: None,
            current_unit_elapsed: None,
            current_partition_path: None,
            current_partition_size: None,
            current_recursion_depth: None,
            started_subproblem_count: None,
            completed_subproblem_count: None,
            visited_partition_count: None,
            finalized_partition_count: None,
            terminal_partition_count: None,
            completed_planner_invocation_count: None,
            fallback_count: None,
            pending_partition_count: None,
            v2_pending_partitions: None,
            v2_completed_pass_summary: None,
            suspected_stall: None,
            elapsed,
            last_progress_at: None,
            error: error.map(str::to_owned),
        }
    }

    fn test_pending_partition_status(
        partition_path: &str,
        expected_item_count: usize,
        observed_replay_progress: Option<usize>,
        routing_bucket_fill_counts: Option<Vec<usize>>,
        trainer_subphase: Option<StreamingIndexingTrainerSubphase>,
    ) -> StreamingV2PendingPartitionStatus {
        StreamingV2PendingPartitionStatus {
            partition_path: partition_path.into(),
            expected_item_count,
            observed_replay_progress,
            routing_bucket_fill_counts,
            trainer_subphase,
            ready_axis_plan_count: None,
            total_axis_plan_count: None,
            populated_cell_count: None,
            realized_cell_count: None,
            planner_state_fingerprint_hex: String::new(),
        }
    }

    #[tokio::test]
    async fn repeated_runs_are_idempotent_for_unchanged_content() {
        let temp = tempdir().unwrap();
        let mailbox_path = temp.path().join("2026-01.mbox");
        let document_path = temp.path().join("readme.txt");
        fs::write(
            &mailbox_path,
            b"From user@example.com Sat Jan 01 00:00:00 2026\nSubject: Hello\n\nBody\n",
        )
        .unwrap();
        fs::write(&document_path, b"document body\n").unwrap();

        let build_request = |base_url: String| BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url,
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: None,
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![
                BatchItemConfig::Mailbox {
                    path: mailbox_path
                        .strip_prefix(temp.path())
                        .unwrap()
                        .to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Document {
                    path: document_path
                        .strip_prefix(temp.path())
                        .unwrap()
                        .to_path_buf(),
                    metadata: BTreeMap::new(),
                },
            ],
        };

        let first_server = spawn_embedding_server(2);
        let first = run_request(temp.path(), build_request(first_server.base_url.clone()))
            .await
            .unwrap();
        let stored_block_count_after_first = count_files_recursively(&temp.path().join("blocks"));
        first_server.join();

        let second_server = spawn_embedding_server(2);
        let second = run_request(temp.path(), build_request(second_server.base_url.clone()))
            .await
            .unwrap();
        let stored_block_count_after_second = count_files_recursively(&temp.path().join("blocks"));
        second_server.join();

        let clustering = run_request(
            temp.path(),
            BatchRequest {
                environment: EnvironmentConfig::Local {
                    block_store_root: Path::new("blocks").to_path_buf(),
                    embedding: LocalEmbeddingConfig {
                        base_url: String::new(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 0,
                        retry_delay_ms: 1,
                    },
                },
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                block_size_target: 65_536,
                stage: ExecutionStage::ClusteringAndBlockAssembly,
                profile_version: PUBLISHED_PROFILE_V0_1_0,
                max_concurrency: None,
                replay_batch_size: None,
                ref_name: TEST_REF_NAME.into(),
                items: vec![],
            },
        )
        .await
        .unwrap();

        assert_eq!(first.root_id, second.root_id);
        assert_eq!(second.root_id, clustering.root_id);
        assert_eq!(first.block_ids, second.block_ids);
        assert!(stored_block_count_after_second > stored_block_count_after_first);
        assert!(stored_block_count_after_second > first.block_count);
    }

    #[tokio::test]
    async fn empty_local_embedding_base_url_is_rejected_as_config_error() {
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: None,
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![BatchItemConfig::Document {
                path: Path::new("doc.txt").to_path_buf(),
                metadata: BTreeMap::new(),
            }],
        };

        let error = run_request(Path::new("C:\\request-root"), request)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            RuntimeError::Config(ConfigError::MissingLocalEmbeddingBaseUrl)
        ));
    }

    #[tokio::test]
    async fn run_request_reports_progress_for_mailbox_processing_and_indexing() {
        let temp = tempdir().unwrap();
        let mailbox_path = temp.path().join("2026-04.mbox");
        let document_path = temp.path().join("notes.txt");
        fs::write(
            &mailbox_path,
            b"From user@example.com Sat Jan 01 00:00:00 2026\nSubject: Progress\n\nBody\n",
        )
        .unwrap();
        fs::write(&document_path, b"document body\n").unwrap();

        let server = spawn_embedding_server(2);
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: None,
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![
                BatchItemConfig::Mailbox {
                    path: mailbox_path
                        .strip_prefix(temp.path())
                        .unwrap()
                        .to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Document {
                    path: document_path
                        .strip_prefix(temp.path())
                        .unwrap()
                        .to_path_buf(),
                    metadata: BTreeMap::new(),
                },
            ],
        };

        let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_capture = Arc::clone(&progress);
        let summary = run_request_with_progress(
            temp.path(),
            request,
            ClusteringConfigOverrides::default(),
            RunRequestArtifactPaths::default(),
            move |message| {
                progress_capture.lock().unwrap().push(message);
            },
        )
        .await
        .unwrap();
        let progress = progress.lock().unwrap();

        assert!(!summary.block_ids.is_empty());
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Preparing 1 document item(s)"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Processing mailbox"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Processed mailbox"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Prepared 1 delegated item(s) from mailbox"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Embedding batch 1 started"))
        );
        assert!(progress.iter().any(|line| {
            line.contains("Embedded batch") && line.contains("completed 2 delegated item(s)")
        }));
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Planning pass 1 started"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Bottom-up assembly for layer 0 completed"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Streaming planning complete"))
        );
        assert!(progress.iter().any(|line| {
            line.contains("replay batch(es); waiting for planning pass completion")
        }));
        server.join();
    }

    #[tokio::test]
    async fn run_request_skips_empty_mailboxes_and_continues_indexing() {
        let temp = tempdir().unwrap();
        let empty_mailbox_path = temp.path().join("2026-05.mail");
        let mailbox_path = temp.path().join("2026-06.mbox");
        fs::write(&empty_mailbox_path, b"").unwrap();
        fs::write(
            &mailbox_path,
            b"From user@example.com Sat Jan 01 00:00:00 2026\nSubject: Progress\n\nBody\n",
        )
        .unwrap();

        let server = spawn_embedding_server(1);
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: None,
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![
                BatchItemConfig::Mailbox {
                    path: empty_mailbox_path
                        .strip_prefix(temp.path())
                        .unwrap()
                        .to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Mailbox {
                    path: mailbox_path
                        .strip_prefix(temp.path())
                        .unwrap()
                        .to_path_buf(),
                    metadata: BTreeMap::new(),
                },
            ],
        };

        let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_capture = Arc::clone(&progress);
        let summary = run_request_with_progress(
            temp.path(),
            request,
            ClusteringConfigOverrides::default(),
            RunRequestArtifactPaths::default(),
            move |message| {
                progress_capture.lock().unwrap().push(message);
            },
        )
        .await
        .unwrap();
        let progress = progress.lock().unwrap();

        assert!(!summary.block_ids.is_empty());
        assert!(progress.iter().any(|line| {
            line.contains("Skipping empty mailbox") && line.contains("2026-05.mail")
        }));
        assert!(
            progress.iter().any(|line| {
                line.contains("Processed mailbox") && line.contains("2026-06.mbox")
            })
        );
        assert!(progress.iter().any(|line| {
            line.contains("Embedded batch") && line.contains("completed 1 delegated item(s)")
        }));
        server.join();
    }

    #[tokio::test]
    async fn clustering_only_stage_reports_replay_submission_progress_and_handoff() {
        let temp = tempdir().unwrap();
        let document_names = ["alpha", "beta", "gamma", "delta", "epsilon"];
        let items = document_names
            .iter()
            .map(|name| {
                let path = temp.path().join(format!("{name}.txt"));
                fs::write(&path, format!("{name}\n")).unwrap();
                BatchItemConfig::Document {
                    path: path.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                }
            })
            .collect::<Vec<_>>();

        let server = spawn_embedding_server(document_names.len());
        let seed_request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: Some(2),
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items,
        };
        run_request(temp.path(), seed_request).await.unwrap();

        let cluster_only_request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::ClusteringAndBlockAssembly,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: Some(2),
            replay_batch_size: Some(4),
            ref_name: TEST_REF_NAME.into(),
            items: vec![],
        };

        let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_capture = Arc::clone(&progress);
        let _summary = run_request_with_progress(
            temp.path(),
            cluster_only_request,
            ClusteringConfigOverrides::default(),
            RunRequestArtifactPaths::default(),
            move |message| {
                progress_capture.lock().unwrap().push(message);
            },
        )
        .await
        .unwrap();

        let progress = progress.lock().unwrap();
        assert!(progress.iter().any(|line| {
            line.contains("Submitting replay batch 1 of 2")
                && line.contains("completed 0 of 5 delegated item(s)")
        }));
        assert!(progress.iter().any(|line| {
            line.contains("Submitted replay batch 1 of 2")
                && line.contains("completed 4 of 5 delegated item(s)")
        }));
        assert!(progress.iter().any(|line| {
            line.contains("Submitted replay batch 2 of 2")
                && line.contains("completed 5 of 5 delegated item(s)")
        }));
        assert!(progress.iter().any(|line| {
            line.contains("Submitted all 2 replay batch(es); waiting for planning pass completion")
                && line.contains("5 delegated item(s)")
        }));
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Planning pass 1 started for 5 item(s)"))
        );
        server.join();
    }

    fn stored_leaf_clustering_request() -> BatchRequest {
        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        BatchRequest {
            environment: local_test_environment(String::new()),
            embedding_spec: EmbeddingSpecConfig {
                dims: embedding_spec.dims,
                encoding: embedding_spec.encoding.clone(),
            },
            block_size_target: serialized_branch_size(&embedding_spec, 2).unwrap(),
            stage: ExecutionStage::ClusteringAndBlockAssembly,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: None,
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![],
        }
    }

    fn stored_leaf_clustering_request_json() -> serde_json::Value {
        let request = stored_leaf_clustering_request();
        json!({
            "environment": {
                "kind": "local",
                "block_store_root": "blocks",
                "embedding": {
                    "base_url": "",
                    "model": "all-MiniLM-L6-v2",
                    "request_timeout_secs": 5,
                    "max_retries": 0,
                    "retry_delay_ms": 1
                }
            },
            "embedding_spec": {
                "dims": request.embedding_spec.dims,
                "encoding": request.embedding_spec.encoding
            },
            "block_size_target": request.block_size_target,
            "profile_version": request.profile_version.to_string(),
            "ref_name": request.ref_name,
            "stage": "clustering-and-block-assembly",
            "items": []
        })
    }

    fn local_test_environment(base_url: String) -> EnvironmentConfig {
        EnvironmentConfig::Local {
            block_store_root: Path::new("blocks").to_path_buf(),
            embedding: LocalEmbeddingConfig {
                base_url,
                model: "all-MiniLM-L6-v2".into(),
                api_key_env: None,
                request_timeout_secs: 5,
                max_retries: 0,
                retry_delay_ms: 1,
            },
        }
    }

    const UNUSED_LOCAL_EMBEDDING_BASE_URL: &str = "http://127.0.0.1:1";

    fn seed_non_finite_leaf_blocks(root: &Path, names: &[&str]) {
        let store =
            ConfiguredBlockStore::from_environment(root, &local_test_environment(String::new()))
                .unwrap();
        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        let mut records = Vec::new();

        for name in names {
            let path = root.join(format!("{name}.txt"));
            let body = format!("{name}\n").into_bytes();
            fs::write(&path, &body).unwrap();
            let block = build_leaf_block(
                VERSION_1,
                embedding_spec.clone(),
                vec![LeafEntry {
                    embedding: [f32::NAN, 0.0]
                        .into_iter()
                        .flat_map(|value| value.to_le_bytes())
                        .collect(),
                    metadata: vec![
                        (
                            Value::Text("source_kind".into()),
                            Value::Text("document".into()),
                        ),
                        (
                            Value::Text("source_path".into()),
                            Value::Text(path.to_string_lossy().replace('\\', "/")),
                        ),
                    ],
                    content: Content {
                        media_type: "text/plain".into(),
                        body,
                    },
                }],
                None,
            )
            .unwrap();
            let block_id = put_block(&store, &Block::Leaf(block));
            let validated = get_block(&store, &block_id).unwrap();
            let (item, _) = replay_item_from_validated_block(&validated, &embedding_spec)
                .unwrap()
                .unwrap();
            records.push(replay_journal_record_from_item(block_id, &item));
        }
        let mutable_ref_store =
            local_mutable_ref_store_location(&root.join("blocks"), TEST_REF_NAME);
        let replay_journal_head_block_id =
            append_replay_journal_records(&store, &mutable_ref_store, &records).unwrap();
        update_mutable_ref_store(
            &mutable_ref_store,
            MutableRefStoreUpdate {
                replay_journal_head_block_id,
                ..MutableRefStoreUpdate::default()
            },
        )
        .unwrap();
    }

    #[tokio::test]
    async fn clustering_failure_carries_diagnostics_and_reports_them_on_progress_stream() {
        let temp = tempdir().unwrap();
        seed_non_finite_leaf_blocks(temp.path(), &["alpha", "beta", "gamma"]);
        let request = stored_leaf_clustering_request();
        let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_capture = Arc::clone(&progress);

        let error = run_request_with_progress(
            temp.path(),
            request,
            ClusteringConfigOverrides::default(),
            RunRequestArtifactPaths::default(),
            move |message| {
                progress_capture.lock().unwrap().push(message);
            },
        )
        .await
        .unwrap_err();

        let diagnostics = error
            .clustering_failure_diagnostics()
            .expect("expected clustering diagnostics on directional-pca failure");
        assert_eq!(
            diagnostics.stage,
            ExecutionStage::ClusteringAndBlockAssembly
        );
        assert_eq!(diagnostics.input_count, 3);
        assert_eq!(diagnostics.inputs.len(), 3);
        assert_eq!(diagnostics.embedding_health.available_embedding_count, 3);
        assert_eq!(diagnostics.embedding_health.non_finite_embedding_count, 3);
        let failing_subset = diagnostics
            .failing_subset
            .as_ref()
            .expect("expected failing subset diagnostics");
        assert_eq!(
            failing_subset.phase,
            FailingSubsetPhaseDiagnostics::HierarchyPlanning {
                stage: "fine planning".into(),
            }
        );
        assert_eq!(failing_subset.provenance, FailingSubsetProvenance::Exact);
        assert_eq!(failing_subset.upstream_active_item_count, 3);
        assert_eq!(
            failing_subset.repository_visible_subset,
            RepositoryVisibleSubsetDiagnostics::SameAsTopLevelAttempt { input_count: 3 }
        );
        assert_eq!(
            diagnostics.embedding_health.suspicious_input_sample.len(),
            3
        );
        assert!(
            diagnostics
                .embedding_health
                .suspicious_input_sample
                .iter()
                .all(|sample| sample
                    .reasons
                    .iter()
                    .any(|reason| reason == "non-finite-embedding"))
        );
        assert!(diagnostics.inputs.iter().any(|input| matches!(
            input,
            ClusteringFailureInput::Document { source_path, .. } if source_path.ends_with("alpha.txt")
        )));
        assert_eq!(diagnostics.clustering.profile_version, "0.1.0");
        assert_eq!(
            diagnostics.clustering.planning_algorithm_id,
            "spherical-kmeans"
        );
        assert_eq!(
            diagnostics.clustering.packing_strategy_id,
            Some("cluster-order-balanced-range-packer-v1".into())
        );

        let progress = progress.lock().unwrap();
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Clustering failure diagnostics:"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("\"profile_version\": \"0.1.0\""))
        );
        assert!(progress.iter().any(|line| line.contains("alpha.txt")));
    }

    #[tokio::test]
    async fn request_file_failure_writes_clustering_diagnostics_beside_summary_output() {
        let temp = tempdir().unwrap();
        seed_non_finite_leaf_blocks(temp.path(), &["alpha", "beta", "gamma"]);
        let request_path = temp.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&stored_leaf_clustering_request_json()).unwrap(),
        )
        .unwrap();
        let summary_out = temp.path().join("output").join("summary.json");

        let error = run_request_file_with_outputs(
            &request_path,
            None,
            ClusteringConfigOverrides::default(),
            Some(summary_out.as_path()),
        )
        .await
        .unwrap_err();

        assert!(error.clustering_failure_diagnostics().is_some());
        let diagnostics_path = temp
            .path()
            .join("output")
            .join("summary.clustering-failure-diagnostics.json");
        let written = fs::read_to_string(&diagnostics_path).unwrap();
        assert!(written.contains("\"profile_version\": \"0.1.0\""));
        assert!(written.contains("\"planning_algorithm_id\": \"spherical-kmeans\""));
        assert!(written.contains("\"embedding_health\""));
        assert!(written.contains("\"failing_subset\""));
        assert!(written.contains("\"provenance\": \"exact\""));
        assert!(written.contains("\"non-finite-embedding\""));
        assert!(written.contains("alpha.txt"));
    }

    #[tokio::test]
    async fn request_file_v2_run_writes_planning_pass_telemetry_beside_summary_output() {
        let temp = tempdir().unwrap();
        let alpha_path = temp.path().join("alpha.txt");
        let beta_path = temp.path().join("beta.txt");
        fs::write(&alpha_path, b"alpha body\n").unwrap();
        fs::write(&beta_path, b"beta body\n").unwrap();
        let request_path = temp.path().join("request.json");
        let server = spawn_embedding_server(2);
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&json!({
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": {
                        "base_url": server.base_url.clone(),
                        "model": "all-MiniLM-L6-v2",
                        "request_timeout_secs": 5,
                        "max_retries": 0,
                        "retry_delay_ms": 1
                    }
                },
                "embedding_spec": {
                    "dims": 2,
                    "encoding": "f32le"
                },
                "block_size_target": 65536,
                "profile_version": "0.7.0",
                "ref_name": TEST_REF_NAME,
                "items": [
                    { "kind": "document", "path": "alpha.txt", "metadata": {} },
                    { "kind": "document", "path": "beta.txt", "metadata": {} }
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        let summary_out = temp.path().join("output").join("summary.json");

        let summary = run_request_file_with_outputs(
            &request_path,
            None,
            ClusteringConfigOverrides::default(),
            Some(summary_out.as_path()),
        )
        .await
        .unwrap();

        assert!(!summary.block_ids.is_empty());
        let telemetry_path = temp
            .path()
            .join("output")
            .join("summary.planning-pass-telemetry.jsonl");
        let planner_state_root = temp.path().join("output").join("summary.planner-state");
        let written = fs::read_to_string(&telemetry_path).unwrap();
        assert!(written.contains("\"effective_profile_version\":\"0.7.0\""));
        assert!(written.contains("\"delegated_contract_family\":\"v2\""));
        assert!(written.contains("\"telemetry_kind\":\"intra-pass\""));
        assert!(written.contains("\"telemetry_kind\":\"pass-summary\""));
        assert!(written.contains("\"planning_completion_state\":\"complete\""));
        assert!(written.contains("\"completed_pass_count\":1"));
        assert!(planner_state_root.is_dir());
        server.join();
    }

    #[tokio::test]
    async fn request_file_v2_run_fails_when_derived_planner_state_root_is_unusable() {
        let temp = tempdir().unwrap();
        let alpha_path = temp.path().join("alpha.txt");
        let beta_path = temp.path().join("beta.txt");
        fs::write(&alpha_path, b"alpha body\n").unwrap();
        fs::write(&beta_path, b"beta body\n").unwrap();
        let request_path = temp.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&json!({
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": {
                        "base_url": "http://unused.local",
                        "model": "all-MiniLM-L6-v2",
                        "request_timeout_secs": 5,
                        "max_retries": 0,
                        "retry_delay_ms": 1
                    }
                },
                "embedding_spec": {
                    "dims": 2,
                    "encoding": "f32le"
                },
                "block_size_target": 65536,
                "profile_version": "0.7.0",
                "ref_name": TEST_REF_NAME,
                "items": [
                    { "kind": "document", "path": "alpha.txt", "metadata": {} },
                    { "kind": "document", "path": "beta.txt", "metadata": {} }
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        let output_dir = temp.path().join("output");
        fs::create_dir_all(&output_dir).unwrap();
        fs::write(output_dir.join("summary.planner-state"), b"not a directory").unwrap();
        let summary_out = output_dir.join("summary.json");

        let error = run_request_file_with_outputs(
            &request_path,
            None,
            ClusteringConfigOverrides::default(),
            Some(summary_out.as_path()),
        )
        .await
        .unwrap_err();

        let RuntimeError::PreparePlannerStateRoot { path, .. } = error else {
            panic!("expected planner-state-root preparation error");
        };
        assert!(path.ends_with("summary.planner-state"));
    }

    #[tokio::test]
    async fn diagnostics_write_failure_keeps_original_clustering_error_and_reports_write_failure() {
        let temp = tempdir().unwrap();
        seed_non_finite_leaf_blocks(temp.path(), &["alpha", "beta", "gamma"]);
        let request_path = temp.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&stored_leaf_clustering_request_json()).unwrap(),
        )
        .unwrap();
        let output_dir = temp.path().join("output");
        fs::create_dir_all(&output_dir).unwrap();
        let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_capture = Arc::clone(&progress);

        let bytes = fs::read(&request_path).unwrap();
        let request: BatchRequest = serde_json::from_slice(&bytes).unwrap();
        let blocked_parent = output_dir.join("blocked-parent");
        fs::write(&blocked_parent, b"not a directory").unwrap();
        let diagnostics_path = blocked_parent.join("summary.clustering-failure-diagnostics.json");
        let error = run_request_with_progress(
            temp.path(),
            request,
            ClusteringConfigOverrides::default(),
            RunRequestArtifactPaths {
                diagnostics_path: Some(diagnostics_path.as_path()),
                ..RunRequestArtifactPaths::default()
            },
            move |message| {
                progress_capture.lock().unwrap().push(message);
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(error, RuntimeError::ClusteringFailure { .. }));
        let progress = progress.lock().unwrap();
        let diagnostics_path_text = diagnostics_path.display().to_string();
        assert!(progress.iter().any(|line| {
            line.contains("Failed to write clustering failure diagnostics to")
                && line.contains(&diagnostics_path_text)
        }));
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Clustering failure diagnostics:"))
        );
    }

    #[tokio::test]
    async fn await_with_periodic_progress_emits_heartbeat_for_long_running_operation() {
        let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_capture = Arc::clone(&progress);
        let heartbeat_observed = Arc::new(tokio::sync::Notify::new());
        let heartbeat_observed_for_reporter = Arc::clone(&heartbeat_observed);
        let reporter: ProgressReporter = Arc::new(move |message| {
            progress_capture.lock().unwrap().push(message);
            heartbeat_observed_for_reporter.notify_one();
        });

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            await_with_periodic_progress(
                async {
                    heartbeat_observed.notified().await;
                    7usize
                },
                &reporter,
                Duration::from_millis(10),
                |elapsed| {
                    format!(
                        "Embedding batch still running after {} ms",
                        elapsed.as_millis()
                    )
                },
            ),
        )
        .await
        .unwrap();

        assert_eq!(result, 7);
        let progress = progress.lock().unwrap();
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Embedding batch still running after"))
        );
    }

    #[tokio::test]
    async fn ingestion_only_stage_returns_placeholder_root_id() {
        let temp = tempdir().unwrap();
        let document_a = temp.path().join("alpha.txt");
        let document_b = temp.path().join("beta.txt");
        fs::write(&document_a, b"alpha\n").unwrap();
        fs::write(&document_b, b"beta\n").unwrap();

        let server = spawn_embedding_server(2);
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::IngestionAndEmbedding,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: None,
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![
                BatchItemConfig::Document {
                    path: document_a.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Document {
                    path: document_b.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
            ],
        };

        let summary = run_request(temp.path(), request).await.unwrap();

        assert_eq!(summary.root_id, placeholder_root_id());
        assert_eq!(summary.block_ids.len(), 2);
        server.join();
    }

    #[tokio::test]
    async fn clustering_only_stage_reuses_store_leaf_blocks_and_skips_embedding_configuration() {
        let temp = tempdir().unwrap();
        let document_a = temp.path().join("alpha.txt");
        let document_b = temp.path().join("beta.txt");
        fs::write(&document_a, b"alpha\n").unwrap();
        fs::write(&document_b, b"beta\n").unwrap();

        let server = spawn_embedding_server(2);
        let full_request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: None,
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![
                BatchItemConfig::Document {
                    path: document_a.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Document {
                    path: document_b.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
            ],
        };
        let seeded = run_request(temp.path(), full_request).await.unwrap();
        fs::remove_file(&document_a).unwrap();
        fs::remove_file(&document_b).unwrap();

        let cluster_only_request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::ClusteringAndBlockAssembly,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: None,
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![],
        };

        let first = run_request(temp.path(), cluster_only_request.clone())
            .await
            .unwrap();
        let second = run_request(temp.path(), cluster_only_request)
            .await
            .unwrap();

        assert_eq!(first.root_id, seeded.root_id);
        assert_eq!(second.root_id, seeded.root_id);
        assert_eq!(first.block_ids, second.block_ids);
        server.join();
    }

    #[tokio::test]
    async fn clustering_only_stage_reuses_stored_email_leaf_content_without_artifact_decode() {
        let temp = tempdir().unwrap();
        let mailbox_path = temp.path().join("2026-01.mbox");
        fs::write(
            &mailbox_path,
            b"From user@example.com Sat Jan 01 00:00:00 2026\nSubject: Hello\n\nBody\n",
        )
        .unwrap();

        let server = spawn_embedding_server(1);
        let full_request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: None,
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![BatchItemConfig::Mailbox {
                path: mailbox_path
                    .strip_prefix(temp.path())
                    .unwrap()
                    .to_path_buf(),
                metadata: BTreeMap::new(),
            }],
        };
        let seeded = run_request(temp.path(), full_request).await.unwrap();

        let block_store_root = temp.path().join("blocks");
        let mutable_ref_store = local_mutable_ref_store_location(&block_store_root, TEST_REF_NAME);
        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let email_artifact_ref = load_replay_journal_records(&block_store, &mutable_ref_store)
            .unwrap()
            .into_iter()
            .find_map(|record| match record {
                ReplayJournalRecord::ReplayInput {
                    content_ref:
                        ReplayJournalContentRef::EmailChunk {
                            email_artifact_ref, ..
                        },
                    ..
                } => Some(email_artifact_ref),
                _ => None,
            })
            .unwrap();
        fs::remove_file(&mailbox_path).unwrap();
        fs::remove_file(local_block_path(&block_store_root, &email_artifact_ref)).unwrap();

        let clustering = run_request(
            temp.path(),
            BatchRequest {
                environment: EnvironmentConfig::Local {
                    block_store_root: Path::new("blocks").to_path_buf(),
                    embedding: LocalEmbeddingConfig {
                        base_url: String::new(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 0,
                        retry_delay_ms: 1,
                    },
                },
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                block_size_target: 65_536,
                stage: ExecutionStage::ClusteringAndBlockAssembly,
                profile_version: PUBLISHED_PROFILE_V0_1_0,
                max_concurrency: None,
                replay_batch_size: None,
                ref_name: TEST_REF_NAME.into(),
                items: vec![],
            },
        )
        .await
        .unwrap();

        assert_eq!(clustering.root_id, seeded.root_id);
        server.join();
    }

    #[tokio::test]
    async fn request_file_stage_override_allows_clustering_only_with_request_items_present() {
        let temp = tempdir().unwrap();
        let document_a = temp.path().join("alpha.txt");
        let document_b = temp.path().join("beta.txt");
        fs::write(&document_a, b"alpha\n").unwrap();
        fs::write(&document_b, b"beta\n").unwrap();

        let server = spawn_embedding_server(2);
        let seeded = run_request(
            temp.path(),
            BatchRequest {
                environment: EnvironmentConfig::Local {
                    block_store_root: Path::new("blocks").to_path_buf(),
                    embedding: LocalEmbeddingConfig {
                        base_url: server.base_url.clone(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 0,
                        retry_delay_ms: 1,
                    },
                },
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                block_size_target: 65_536,
                stage: ExecutionStage::FullPipeline,
                profile_version: PUBLISHED_PROFILE_V0_1_0,
                max_concurrency: None,
                replay_batch_size: None,
                ref_name: TEST_REF_NAME.into(),
                items: vec![
                    BatchItemConfig::Document {
                        path: document_a.strip_prefix(temp.path()).unwrap().to_path_buf(),
                        metadata: BTreeMap::new(),
                    },
                    BatchItemConfig::Document {
                        path: document_b.strip_prefix(temp.path()).unwrap().to_path_buf(),
                        metadata: BTreeMap::new(),
                    },
                ],
            },
        )
        .await
        .unwrap();

        let request_path = temp.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&json!({
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": {
                        "base_url": server.base_url,
                        "model": "all-MiniLM-L6-v2",
                        "request_timeout_secs": 5,
                        "max_retries": 0,
                        "retry_delay_ms": 1
                    }
                },
                "embedding_spec": {
                    "dims": 2,
                    "encoding": "f32le"
                },
                "profile_version": "0.1.0",
                "ref_name": "test-branch",
                "items": [
                    {
                        "kind": "document",
                        "path": "alpha.txt"
                    },
                    {
                        "kind": "document",
                        "path": "beta.txt"
                    }
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        let summary = run_request_file_with_stage(
            &request_path,
            Some(ExecutionStage::ClusteringAndBlockAssembly),
        )
        .await
        .unwrap();

        assert_eq!(summary.root_id, seeded.root_id);
        server.join();
    }

    #[tokio::test]
    async fn explicit_default_clustering_matches_omitted_clustering_options() {
        let temp = tempdir().unwrap();
        for name in ["alpha", "beta", "gamma"] {
            fs::write(temp.path().join(format!("{name}.txt")), format!("{name}\n")).unwrap();
        }

        let server = spawn_embedding_server(6);
        let request_path = temp.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&json!({
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": {
                        "base_url": server.base_url,
                        "model": "all-MiniLM-L6-v2",
                        "request_timeout_secs": 5,
                        "max_retries": 0,
                        "retry_delay_ms": 1
                    }
                },
                "embedding_spec": {
                    "dims": 2,
                    "encoding": "f32le"
                },
                "ref_name": "test-branch",
                "items": [
                    { "kind": "document", "path": "alpha.txt" },
                    { "kind": "document", "path": "beta.txt" },
                    { "kind": "document", "path": "gamma.txt" }
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        let defaulted = run_request_file(&request_path).await.unwrap();
        let explicit = run_request_file_with_overrides(
            &request_path,
            None,
            ClusteringConfigOverrides::default(),
        )
        .await
        .unwrap();

        assert_eq!(defaulted.root_id, explicit.root_id);
        assert_eq!(defaulted.block_ids, explicit.block_ids);
        server.join();
    }

    #[tokio::test]
    async fn published_profile_clustering_runs_end_to_end() {
        let temp = tempdir().unwrap();
        for name in ["alpha", "beta", "gamma"] {
            fs::write(temp.path().join(format!("{name}.txt")), format!("{name}\n")).unwrap();
        }

        let server = spawn_distinct_embedding_server(3);
        let request_path = temp.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&json!({
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": {
                        "base_url": server.base_url,
                        "model": "all-MiniLM-L6-v2",
                        "request_timeout_secs": 5,
                        "max_retries": 0,
                        "retry_delay_ms": 1
                    }
                },
                "embedding_spec": {
                    "dims": 2,
                    "encoding": "f32le"
                },
                "ref_name": "test-branch",
                "items": [
                    { "kind": "document", "path": "alpha.txt" },
                    { "kind": "document", "path": "beta.txt" },
                    { "kind": "document", "path": "gamma.txt" }
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        let summary = run_request_file_with_overrides(
            &request_path,
            None,
            ClusteringConfigOverrides::default(),
        )
        .await
        .unwrap();

        assert!(!summary.block_ids.is_empty());
        server.join();
    }

    #[tokio::test]
    async fn alternate_published_profile_runs_end_to_end() {
        let temp = tempdir().unwrap();
        for name in ["alpha", "beta", "gamma"] {
            fs::write(temp.path().join(format!("{name}.txt")), format!("{name}\n")).unwrap();
        }

        let server = spawn_distinct_embedding_server(3);
        let request_path = temp.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&json!({
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": {
                        "base_url": server.base_url,
                        "model": "all-MiniLM-L6-v2",
                        "request_timeout_secs": 5,
                        "max_retries": 0,
                        "retry_delay_ms": 1
                    }
                },
                "embedding_spec": {
                    "dims": 2,
                    "encoding": "f32le"
                },
                "profile_version": "0.5.0",
                "ref_name": "test-branch",
                "items": [
                    { "kind": "document", "path": "alpha.txt" },
                    { "kind": "document", "path": "beta.txt" },
                    { "kind": "document", "path": "gamma.txt" }
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        let summary = run_request_file_with_overrides(
            &request_path,
            None,
            ClusteringConfigOverrides::default(),
        )
        .await
        .unwrap();

        assert!(!summary.block_ids.is_empty());
        server.join();
    }

    #[tokio::test]
    async fn local_testing_cluster_override_is_rejected_for_published_profile_v0_7_0() {
        let temp = tempdir().unwrap();
        for index in 0..12 {
            fs::write(
                temp.path().join(format!("doc-{index}.txt")),
                format!("document {index}\n"),
            )
            .unwrap();
        }

        let request_path = temp.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&json!({
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": {
                        "base_url": UNUSED_LOCAL_EMBEDDING_BASE_URL,
                        "model": "all-MiniLM-L6-v2",
                        "request_timeout_secs": 5,
                        "max_retries": 0,
                        "retry_delay_ms": 1
                    }
                },
                "embedding_spec": {
                    "dims": 2,
                    "encoding": "f32le"
                },
                "profile_version": "0.7.0",
                "ref_name": "test-branch",
                "items": (0..12)
                    .map(|index| json!({ "kind": "document", "path": format!("doc-{index}.txt") }))
                    .collect::<Vec<_>>()
            }))
            .unwrap(),
        )
        .unwrap();

        let error = run_request_file_with_overrides(
            &request_path,
            None,
            ClusteringConfigOverrides {
                profile_version: None,
                local_testing_cluster_count: Some(32),
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            RuntimeError::Config(
                ConfigError::LocalTestingClusterCountUnsupportedForPublishedProfileV0_7_0
            )
        ));
    }

    #[tokio::test]
    async fn validate_only_reports_published_profile_block_size_conflict() {
        let temp = tempdir().unwrap();
        let request_path = temp.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&json!({
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": {
                        "base_url": UNUSED_LOCAL_EMBEDDING_BASE_URL,
                        "model": "all-MiniLM-L6-v2",
                        "request_timeout_secs": 5,
                        "max_retries": 0,
                        "retry_delay_ms": 1
                    }
                },
                "embedding_spec": {
                    "dims": 384,
                    "encoding": "f32le"
                },
                "block_size_target": 65536,
                "stage": "clustering-and-block-assembly",
                "profile_version": "0.6.0",
                "ref_name": "test-branch"
            }))
            .unwrap(),
        )
        .unwrap();

        let error = validate_request_file_with_overrides(
            &request_path,
            None,
            ClusteringConfigOverrides::default(),
            None,
        )
        .await
        .unwrap_err();

        let RuntimeError::StreamingIndexer(error) = error else {
            panic!("expected streaming indexer validation error");
        };
        let message = error.to_string();
        assert!(message.contains("requires cluster_count 64"));
        assert!(message.contains("block size target 65536"));
    }

    #[tokio::test]
    async fn validate_only_uses_summary_output_for_planner_state_root() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("alpha.txt"), b"alpha body\n").unwrap();
        let request_path = temp.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&json!({
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": {
                        "base_url": "http://unused.local",
                        "model": "all-MiniLM-L6-v2",
                        "request_timeout_secs": 5,
                        "max_retries": 0,
                        "retry_delay_ms": 1
                    }
                },
                "embedding_spec": {
                    "dims": 2,
                    "encoding": "f32le"
                },
                "block_size_target": 65536,
                "profile_version": "0.7.0",
                "ref_name": TEST_REF_NAME,
                "items": [
                    { "kind": "document", "path": "alpha.txt", "metadata": {} }
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        let output_dir = temp.path().join("output");
        fs::create_dir_all(&output_dir).unwrap();
        fs::write(output_dir.join("summary.planner-state"), b"not a directory").unwrap();
        let summary_out = output_dir.join("summary.json");

        let error = validate_request_file_with_overrides(
            &request_path,
            None,
            ClusteringConfigOverrides::default(),
            Some(summary_out.as_path()),
        )
        .await
        .unwrap_err();

        let RuntimeError::PreparePlannerStateRoot { path, .. } = error else {
            panic!("expected planner-state-root preparation error");
        };
        assert!(path.ends_with("summary.planner-state"));
    }

    #[tokio::test]
    async fn validate_only_uses_summary_output_for_replay_order_scratch_root() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("alpha.txt"), b"alpha body\n").unwrap();
        let request_path = temp.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&json!({
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": {
                        "base_url": "http://unused.local",
                        "model": "all-MiniLM-L6-v2",
                        "request_timeout_secs": 5,
                        "max_retries": 0,
                        "retry_delay_ms": 1
                    }
                },
                "embedding_spec": {
                    "dims": 2,
                    "encoding": "f32le"
                },
                "block_size_target": 65536,
                "profile_version": "0.7.0",
                "ref_name": TEST_REF_NAME,
                "items": [
                    { "kind": "document", "path": "alpha.txt", "metadata": {} }
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        let output_dir = temp.path().join("output");
        fs::create_dir_all(&output_dir).unwrap();
        fs::write(output_dir.join("summary.replay-order"), b"not a directory").unwrap();
        let summary_out = output_dir.join("summary.json");

        let error = validate_request_file_with_overrides(
            &request_path,
            None,
            ClusteringConfigOverrides::default(),
            Some(summary_out.as_path()),
        )
        .await
        .unwrap_err();

        let RuntimeError::PrepareReplayOrderScratchRoot { path, .. } = error else {
            panic!("expected replay-order scratch root preparation error");
        };
        assert!(path.ends_with("summary.replay-order"));
    }

    #[test]
    fn clustering_failure_diagnostics_path_prefers_summary_output_directory() {
        let request_path = Path::new("data").join("request.json");
        let summary_path = Path::new("output").join("summary.json");
        let path = clustering_failure_diagnostics_path(
            request_path.as_path(),
            Some(summary_path.as_path()),
        );

        assert_eq!(
            path,
            Path::new("output").join("summary.clustering-failure-diagnostics.json")
        );
    }

    #[test]
    fn clustering_failure_diagnostics_path_falls_back_to_request_directory() {
        let request_path = Path::new("data").join("request.json");
        let path = clustering_failure_diagnostics_path(request_path.as_path(), None);

        assert_eq!(
            path,
            Path::new("data").join("request.clustering-failure-diagnostics.json")
        );
    }

    #[test]
    fn planning_pass_telemetry_path_prefers_summary_output_directory() {
        let request_path = Path::new("data").join("request.json");
        let summary_path = Path::new("output").join("summary.json");
        let path =
            planning_pass_telemetry_path(request_path.as_path(), Some(summary_path.as_path()));

        assert_eq!(
            path,
            Path::new("output").join("summary.planning-pass-telemetry.jsonl")
        );
    }

    #[test]
    fn planning_pass_telemetry_path_falls_back_to_request_directory() {
        let request_path = Path::new("data").join("request.json");
        let path = planning_pass_telemetry_path(request_path.as_path(), None);

        assert_eq!(
            path,
            Path::new("data").join("request.planning-pass-telemetry.jsonl")
        );
    }

    #[test]
    fn planner_state_root_path_prefers_summary_output_directory() {
        let request_path = Path::new("data").join("request.json");
        let summary_path = Path::new("output").join("summary.json");
        let path = planner_state_root_path(request_path.as_path(), Some(summary_path.as_path()));

        assert_eq!(path, Path::new("output").join("summary.planner-state"));
    }

    #[test]
    fn planner_state_root_path_falls_back_to_request_directory() {
        let request_path = Path::new("data").join("request.json");
        let path = planner_state_root_path(request_path.as_path(), None);

        assert_eq!(path, Path::new("data").join("request.planner-state"));
    }

    #[test]
    fn replay_order_scratch_root_path_prefers_summary_output_directory() {
        let request_path = Path::new("data").join("request.json");
        let summary_path = Path::new("output").join("summary.json");
        let path =
            replay_order_scratch_root_path(request_path.as_path(), Some(summary_path.as_path()));

        assert_eq!(path, Path::new("output").join("summary.replay-order"));
    }

    #[test]
    fn replay_order_scratch_root_path_falls_back_to_request_directory() {
        let request_path = Path::new("data").join("request.json");
        let path = replay_order_scratch_root_path(request_path.as_path(), None);

        assert_eq!(path, Path::new("data").join("request.replay-order"));
    }

    #[test]
    fn prepare_planner_state_root_probes_and_cleans_up_existing_directory() {
        let temp = tempdir().unwrap();
        let planner_state_root = temp.path().join("planner-state");
        fs::create_dir_all(&planner_state_root).unwrap();

        prepare_planner_state_root(&planner_state_root).unwrap();

        let remaining_entries = fs::read_dir(&planner_state_root).unwrap().count();
        assert_eq!(remaining_entries, 0);
    }

    #[test]
    fn prepare_replay_order_scratch_root_probes_and_cleans_up_existing_directory() {
        let temp = tempdir().unwrap();
        let replay_order_root = temp.path().join("replay-order");
        fs::create_dir_all(&replay_order_root).unwrap();

        prepare_replay_order_scratch_root(&replay_order_root).unwrap();

        let remaining_entries = fs::read_dir(&replay_order_root).unwrap().count();
        assert_eq!(remaining_entries, 0);
    }

    #[test]
    fn merge_sorted_replay_order_runs_deduplicates_and_orders_entries() {
        let scratch = tempdir().unwrap();
        let block_a = BlockHash::from_bytes([1u8; 32]);
        let block_b = BlockHash::from_bytes([2u8; 32]);
        let block_c = BlockHash::from_bytes([3u8; 32]);
        let digest_a = BlockHash::from_bytes([11u8; 32]);
        let digest_b = BlockHash::from_bytes([12u8; 32]);
        let digest_c = BlockHash::from_bytes([13u8; 32]);

        let mut first_run = vec![
            ReplayOrderEntry::new(block_c, digest_c),
            ReplayOrderEntry::new(block_a, digest_a),
        ];
        let mut second_run = vec![
            ReplayOrderEntry::new(block_b, digest_b),
            ReplayOrderEntry::new(block_a, digest_a),
        ];
        let run_paths = vec![
            flush_sorted_replay_order_run(scratch.path(), 0, &mut first_run).unwrap(),
            flush_sorted_replay_order_run(scratch.path(), 1, &mut second_run).unwrap(),
        ];

        let merged_entries_path = scratch.path().join("ordered-replay.bin");
        let total_items = merge_sorted_replay_order_runs(&run_paths, &merged_entries_path).unwrap();
        assert_eq!(total_items, 3);

        let storage = ReplayOrderStorage::new(scratch, merged_entries_path, total_items);
        let entries = storage.read_all_entries().unwrap();
        assert_eq!(
            entries
                .iter()
                .copied()
                .map(ReplayOrderEntry::block_hash)
                .collect::<Vec<_>>(),
            vec![block_a, block_b, block_c]
        );
    }

    #[test]
    fn merge_sorted_replay_order_runs_handles_more_runs_than_fan_in() {
        let scratch = tempdir().unwrap();
        let run_paths = (0..=REPLAY_ORDER_MERGE_FAN_IN)
            .map(|index| {
                let block = BlockHash::from_bytes([index as u8; 32]);
                let digest = BlockHash::from_bytes([(index + 1) as u8; 32]);
                let mut run = vec![ReplayOrderEntry::new(block, digest)];
                flush_sorted_replay_order_run(scratch.path(), index, &mut run).unwrap()
            })
            .collect::<Vec<_>>();

        let merged_entries_path = scratch.path().join("ordered-replay-many.bin");
        let total_items = merge_sorted_replay_order_runs(&run_paths, &merged_entries_path).unwrap();
        assert_eq!(total_items, REPLAY_ORDER_MERGE_FAN_IN + 1);

        let storage = ReplayOrderStorage::new(scratch, merged_entries_path, total_items);
        let entries = storage.read_all_entries().unwrap();
        assert_eq!(entries.len(), REPLAY_ORDER_MERGE_FAN_IN + 1);
        assert!(entries.windows(2).all(|pair| pair[0] <= pair[1]));
    }

    #[test]
    fn replay_journal_record_sort_key_digest_matches_item_digest() {
        let cases = [
            IndexItem {
                metadata: vec![
                    (Value::Text("title".into()), Value::Text("Alpha".into())),
                    (
                        Value::Text("source_kind".into()),
                        Value::Text("document".into()),
                    ),
                ],
                content_ref: ContentRef::Document {
                    path: "C:\\docs\\alpha.txt".into(),
                },
            },
            IndexItem {
                metadata: vec![
                    (Value::Text("title".into()), Value::Text("Inline".into())),
                    (
                        Value::Text("source_kind".into()),
                        Value::Text("inline".into()),
                    ),
                ],
                content_ref: ContentRef::Inline {
                    media_type: "text/plain".into(),
                    body: b"alpha".to_vec(),
                },
            },
            IndexItem {
                metadata: vec![
                    (Value::Text("title".into()), Value::Text("Email".into())),
                    (
                        Value::Text("source_kind".into()),
                        Value::Text("email".into()),
                    ),
                ],
                content_ref: ContentRef::EmailChunk {
                    email_artifact_ref: "mail-123".into(),
                    chunk_index: 7,
                },
            },
            IndexItem {
                metadata: vec![
                    (Value::Text("title".into()), Value::Text("Stored".into())),
                    (
                        Value::Text("source_kind".into()),
                        Value::Text("document".into()),
                    ),
                ],
                content_ref: ContentRef::StoredReplay {
                    media_type: "text/plain".into(),
                    body: b"alpha".to_vec(),
                    identity: ReplayIdentity::Document {
                        source_path: "C:\\docs\\stored-alpha.txt".into(),
                    },
                },
            },
        ];

        for (index, item) in cases.into_iter().enumerate() {
            let record = replay_journal_record_from_item(
                BlockHash::from_bytes([index as u8 + 7; 32]),
                &item,
            );
            assert_eq!(
                replay_journal_record_sort_key_digest(&record),
                Some(replay_sort_key_digest(&item))
            );
        }
    }

    #[test]
    fn replay_journal_record_sort_key_digest_ignores_metadata_pair_order() {
        let item = IndexItem {
            metadata: vec![
                (Value::Text("zeta".into()), Value::Text("last".into())),
                (Value::Text("alpha".into()), Value::Text("first".into())),
            ],
            content_ref: ContentRef::StoredReplay {
                media_type: "text/plain".into(),
                body: b"alpha".to_vec(),
                identity: ReplayIdentity::Document {
                    source_path: "C:\\docs\\alpha.txt".into(),
                },
            },
        };
        let mut record = replay_journal_record_from_item(BlockHash::from_bytes([7u8; 32]), &item);
        let ReplayJournalRecord::ReplayInput { metadata, .. } = &mut record else {
            unreachable!();
        };
        metadata.reverse();

        assert_eq!(
            replay_journal_record_sort_key_digest(&record),
            Some(replay_sort_key_digest(&item))
        );
    }

    #[test]
    fn replay_journal_record_sort_key_digest_normalizes_document_paths() {
        let item = IndexItem {
            metadata: vec![
                (Value::Text("title".into()), Value::Text("Alpha".into())),
                (
                    Value::Text("source_kind".into()),
                    Value::Text("document".into()),
                ),
            ],
            content_ref: ContentRef::Document {
                path: "C:\\docs\\alpha.txt".into(),
            },
        };
        let mut record = replay_journal_record_from_item(BlockHash::from_bytes([7u8; 32]), &item);
        let ReplayJournalRecord::ReplayInput { content_ref, .. } = &mut record else {
            unreachable!();
        };
        *content_ref = ReplayJournalContentRef::Document {
            path: "C:\\docs\\alpha.txt".into(),
        };

        assert_eq!(
            replay_journal_record_sort_key_digest(&record),
            Some(replay_sort_key_digest(&item))
        );
    }

    #[test]
    fn flush_sorted_replay_order_run_deduplicates_entries_within_run() {
        let scratch = tempdir().unwrap();
        let block_a = BlockHash::from_bytes([1u8; 32]);
        let block_b = BlockHash::from_bytes([2u8; 32]);
        let digest_a = BlockHash::from_bytes([11u8; 32]);
        let digest_b = BlockHash::from_bytes([12u8; 32]);
        let mut entries = vec![
            ReplayOrderEntry::new(block_b, digest_b),
            ReplayOrderEntry::new(block_a, digest_a),
            ReplayOrderEntry::new(block_a, digest_a),
        ];

        let path = flush_sorted_replay_order_run(scratch.path(), 0, &mut entries).unwrap();
        let storage = ReplayOrderStorage::new(scratch, path, 2);
        let loaded = storage.read_all_entries().unwrap();
        assert_eq!(
            loaded
                .iter()
                .copied()
                .map(ReplayOrderEntry::block_hash)
                .collect::<Vec<_>>(),
            vec![block_a, block_b]
        );
    }

    #[test]
    fn flush_sorted_replay_order_run_rejects_conflicting_duplicate_digests() {
        let scratch = tempdir().unwrap();
        let block_a = BlockHash::from_bytes([1u8; 32]);
        let mut entries = vec![
            ReplayOrderEntry::new(block_a, BlockHash::from_bytes([11u8; 32])),
            ReplayOrderEntry::new(block_a, BlockHash::from_bytes([12u8; 32])),
        ];

        let error = flush_sorted_replay_order_run(scratch.path(), 0, &mut entries).unwrap_err();
        assert!(matches!(
            error,
            RuntimeError::InvalidReplayJournalHead { block_id, .. }
                if block_id == block_a.to_string()
        ));
    }

    #[test]
    fn planning_pass_telemetry_writes_jsonl_records_and_detailed_progress() {
        let temp = tempdir().unwrap();
        let telemetry_path = temp.path().join("planning-pass-telemetry.jsonl");
        let telemetry = PlanningTelemetryContext {
            run_identity: PlanningRunIdentity {
                effective_profile_version: "0.7.0".into(),
                delegated_contract_family: DelegatedContractFamily::V2,
            },
            sink_path: Some(telemetry_path.clone()),
            sink_initialized: Arc::new(AtomicBool::new(false)),
            sink_write_lock: Arc::new(Mutex::new(())),
            diagnosis_state: Arc::new(Mutex::new(PlanningTelemetryState::default())),
        };
        let progress_messages = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_progress = Arc::clone(&progress_messages);
        let progress: ProgressReporter = Arc::new(move |message| {
            captured_progress.lock().unwrap().push(message);
        });

        report_planning_pass_completion(
            &progress,
            Some(&telemetry),
            test_planning_pass_report(1, 3),
            &PlanningCompletionAction::ReplayRequired(
                "Planning pass 1 requires another full replay pass before v2 planning can complete: still pending"
                    .into(),
            ),
        )
        .unwrap();

        let progress_messages = progress_messages.lock().unwrap();
        assert!(progress_messages[0].contains("profile 0.7.0 via v2"));
        assert!(progress_messages[0].contains("terminal partitions 9/12"));
        assert!(progress_messages[0].contains("realized/requested clusters 48/64"));
        assert!(progress_messages[0].contains("diagnosis inconclusive"));
        assert!(progress_messages[1].contains("requires another full replay pass"));

        let written = fs::read_to_string(&telemetry_path).unwrap();
        assert!(written.contains("\"telemetry_kind\":\"pass-summary\""));
        assert!(written.contains("\"effective_profile_version\":\"0.7.0\""));
        assert!(written.contains("\"delegated_contract_family\":\"v2\""));
        assert!(written.contains("\"planning_completion_state\":\"replay-required\""));
        assert!(written.contains("\"planning_completion_reason\":\"Planning pass 1 requires another full replay pass before v2 planning can complete: still pending\""));
        assert!(written.contains("\"convergence_verdict\":\"inconclusive\""));
    }

    #[test]
    fn planning_pass_telemetry_first_pass_replaces_prior_run_file_contents() {
        let temp = tempdir().unwrap();
        let telemetry_path = temp.path().join("planning-pass-telemetry.jsonl");
        fs::write(&telemetry_path, "{\"stale\":true}\n").unwrap();
        let telemetry = PlanningTelemetryContext {
            run_identity: PlanningRunIdentity {
                effective_profile_version: "0.7.0".into(),
                delegated_contract_family: DelegatedContractFamily::V2,
            },
            sink_path: Some(telemetry_path.clone()),
            sink_initialized: Arc::new(AtomicBool::new(false)),
            sink_write_lock: Arc::new(Mutex::new(())),
            diagnosis_state: Arc::new(Mutex::new(PlanningTelemetryState::default())),
        };
        let progress: ProgressReporter = Arc::new(|_| {});

        report_planning_pass_completion(
            &progress,
            Some(&telemetry),
            test_planning_pass_report(1, 3),
            &PlanningCompletionAction::Complete,
        )
        .unwrap();

        let written = fs::read_to_string(&telemetry_path).unwrap();
        assert!(!written.contains("\"stale\":true"));
        assert!(written.contains("\"telemetry_kind\":\"pass-summary\""));
        assert!(written.contains("\"completed_pass_count\":1"));
        assert_eq!(written.lines().count(), 1);
    }

    #[test]
    fn planning_pass_telemetry_writes_v2_intra_pass_records() {
        let temp = tempdir().unwrap();
        let telemetry_path = temp.path().join("planning-pass-telemetry.jsonl");
        let telemetry = PlanningTelemetryContext {
            run_identity: PlanningRunIdentity {
                effective_profile_version: "0.7.0".into(),
                delegated_contract_family: DelegatedContractFamily::V2,
            },
            sink_path: Some(telemetry_path.clone()),
            sink_initialized: Arc::new(AtomicBool::new(false)),
            sink_write_lock: Arc::new(Mutex::new(())),
            diagnosis_state: Arc::new(Mutex::new(PlanningTelemetryState::default())),
        };
        let status = StreamingIndexingStatus {
            pending_partition_count: Some(1),
            v2_pending_partitions: Some(vec![test_pending_partition_status(
                "root/0",
                7,
                Some(3),
                None,
                Some(StreamingIndexingTrainerSubphase::PlanCuts),
            )]),
            suspected_stall: Some(
                lexongraph_streaming_indexer::StreamingIndexingSuspectedStall {
                    reason: StreamingIndexingSuspectedStallReason::UnchangedTrainerSubphase,
                    duration_without_progress: Duration::from_secs(7),
                },
            ),
            last_progress_at: Some(Duration::from_millis(120)),
            ..test_streaming_status(
                StreamingIndexingPhase::PlanningPass { pass_number: 2 },
                StreamingIndexingStatusState::InProgress,
                12,
                Some(12),
                4,
                Some(8),
                Duration::from_millis(250),
                None,
            )
        };

        let (record, diagnosis_message) = telemetry.project_planning_status(&status);
        telemetry.write_json_record(&record.unwrap()).unwrap();

        let written = fs::read_to_string(&telemetry_path).unwrap();
        assert!(written.contains("\"telemetry_kind\":\"intra-pass\""));
        assert!(written.contains("\"planning_status_state\":\"in-progress\""));
        assert!(written.contains("\"pending_partition_count\":1"));
        assert!(written.contains("\"pending_partition_preview_count\":1"));
        assert!(written.contains("\"partition_path\":\"root/0\""));
        assert!(written.contains("\"trainer_subphase\":\"plan-cuts\""));
        assert!(written.contains("\"suspected_stall_reason\":\"unchanged-trainer-subphase\""));
        assert!(written.contains("\"convergence_verdict\":\"inconclusive\""));
        assert!(written.contains("\"blocked_on_summary\":\"1 pending partition(s)"));
        assert!(
            diagnosis_message
                .unwrap()
                .contains("Planning diagnosis for pass 2: verdict inconclusive")
        );
    }

    #[test]
    fn planning_pass_telemetry_first_intra_pass_record_replaces_prior_run_file_contents() {
        let temp = tempdir().unwrap();
        let telemetry_path = temp.path().join("planning-pass-telemetry.jsonl");
        fs::write(&telemetry_path, "{\"stale\":true}\n").unwrap();
        let telemetry = PlanningTelemetryContext {
            run_identity: PlanningRunIdentity {
                effective_profile_version: "0.7.0".into(),
                delegated_contract_family: DelegatedContractFamily::V2,
            },
            sink_path: Some(telemetry_path.clone()),
            sink_initialized: Arc::new(AtomicBool::new(false)),
            sink_write_lock: Arc::new(Mutex::new(())),
            diagnosis_state: Arc::new(Mutex::new(PlanningTelemetryState::default())),
        };
        let status = StreamingIndexingStatus {
            pending_partition_count: Some(1),
            v2_pending_partitions: Some(vec![test_pending_partition_status(
                "root/0",
                7,
                Some(3),
                None,
                Some(StreamingIndexingTrainerSubphase::PlanCuts),
            )]),
            ..test_streaming_status(
                StreamingIndexingPhase::PlanningPass { pass_number: 2 },
                StreamingIndexingStatusState::InProgress,
                12,
                Some(12),
                4,
                Some(8),
                Duration::from_millis(250),
                None,
            )
        };

        let (record, _) = telemetry.project_planning_status(&status);
        telemetry.write_json_record(&record.unwrap()).unwrap();

        let written = fs::read_to_string(&telemetry_path).unwrap();
        assert!(!written.contains("\"stale\":true"));
        assert!(written.contains("\"telemetry_kind\":\"intra-pass\""));
        assert_eq!(written.lines().count(), 1);
    }

    #[test]
    fn planning_pass_telemetry_bounds_pending_partition_preview_in_jsonl_records() {
        let temp = tempdir().unwrap();
        let telemetry_path = temp.path().join("planning-pass-telemetry.jsonl");
        let telemetry = PlanningTelemetryContext {
            run_identity: PlanningRunIdentity {
                effective_profile_version: "0.7.0".into(),
                delegated_contract_family: DelegatedContractFamily::V2,
            },
            sink_path: Some(telemetry_path.clone()),
            sink_initialized: Arc::new(AtomicBool::new(false)),
            sink_write_lock: Arc::new(Mutex::new(())),
            diagnosis_state: Arc::new(Mutex::new(PlanningTelemetryState::default())),
        };
        let status = StreamingIndexingStatus {
            pending_partition_count: Some(3),
            v2_pending_partitions: Some(vec![
                test_pending_partition_status("root/0", 7, Some(3), None, None),
                test_pending_partition_status("root/1", 5, Some(2), Some(vec![1, 1]), None),
                test_pending_partition_status("root/2", 4, Some(1), None, None),
            ]),
            ..test_streaming_status(
                StreamingIndexingPhase::PlanningPass { pass_number: 2 },
                StreamingIndexingStatusState::InProgress,
                12,
                Some(12),
                4,
                Some(8),
                Duration::from_millis(250),
                None,
            )
        };

        let (record, _) = telemetry.project_planning_status(&status);
        telemetry.write_json_record(&record.unwrap()).unwrap();

        let written = fs::read_to_string(&telemetry_path).unwrap();
        assert!(written.contains("\"pending_partition_count\":3"));
        assert!(written.contains("\"pending_partition_preview_count\":2"));
        assert!(written.contains("\"partition_path\":\"root/0\""));
        assert!(written.contains("\"partition_path\":\"root/1\""));
        assert!(!written.contains("\"partition_path\":\"root/2\""));
    }

    #[test]
    fn planning_pass_telemetry_derives_pending_partition_count_from_preview() {
        let temp = tempdir().unwrap();
        let telemetry_path = temp.path().join("planning-pass-telemetry.jsonl");
        let telemetry = PlanningTelemetryContext {
            run_identity: PlanningRunIdentity {
                effective_profile_version: "0.7.0".into(),
                delegated_contract_family: DelegatedContractFamily::V2,
            },
            sink_path: Some(telemetry_path.clone()),
            sink_initialized: Arc::new(AtomicBool::new(false)),
            sink_write_lock: Arc::new(Mutex::new(())),
            diagnosis_state: Arc::new(Mutex::new(PlanningTelemetryState::default())),
        };
        let status = StreamingIndexingStatus {
            pending_partition_count: None,
            v2_pending_partitions: Some(vec![
                test_pending_partition_status("root/0", 7, Some(3), None, None),
                test_pending_partition_status("root/1", 5, Some(2), None, None),
            ]),
            ..test_streaming_status(
                StreamingIndexingPhase::PlanningPass { pass_number: 2 },
                StreamingIndexingStatusState::InProgress,
                12,
                Some(12),
                4,
                Some(8),
                Duration::from_millis(250),
                None,
            )
        };

        let (record, _) = telemetry.project_planning_status(&status);
        telemetry.write_json_record(&record.unwrap()).unwrap();

        let written = fs::read_to_string(&telemetry_path).unwrap();
        assert!(written.contains("\"pending_partition_count\":2"));
    }

    #[test]
    fn planning_pass_telemetry_uses_unknown_blocked_on_state_when_detail_missing() {
        let temp = tempdir().unwrap();
        let telemetry_path = temp.path().join("planning-pass-telemetry.jsonl");
        let telemetry = PlanningTelemetryContext {
            run_identity: PlanningRunIdentity {
                effective_profile_version: "0.7.0".into(),
                delegated_contract_family: DelegatedContractFamily::V2,
            },
            sink_path: Some(telemetry_path.clone()),
            sink_initialized: Arc::new(AtomicBool::new(false)),
            sink_write_lock: Arc::new(Mutex::new(())),
            diagnosis_state: Arc::new(Mutex::new(PlanningTelemetryState::default())),
        };
        let status = test_streaming_status(
            StreamingIndexingPhase::PlanningPass { pass_number: 2 },
            StreamingIndexingStatusState::InProgress,
            12,
            Some(12),
            4,
            Some(8),
            Duration::from_millis(250),
            None,
        );

        let (record, diagnosis_message) = telemetry.project_planning_status(&status);
        telemetry.write_json_record(&record.unwrap()).unwrap();

        let written = fs::read_to_string(&telemetry_path).unwrap();
        assert!(written.contains("\"blocked_on_summary\":\"unknown\""));
        assert!(diagnosis_message.unwrap().contains("blocked on unknown"));
    }

    #[test]
    fn planning_pass_summary_preserves_last_concrete_blocked_on_summary() {
        let telemetry = PlanningTelemetryContext {
            run_identity: PlanningRunIdentity {
                effective_profile_version: "0.7.0".into(),
                delegated_contract_family: DelegatedContractFamily::V2,
            },
            sink_path: None,
            sink_initialized: Arc::new(AtomicBool::new(false)),
            sink_write_lock: Arc::new(Mutex::new(())),
            diagnosis_state: Arc::new(Mutex::new(PlanningTelemetryState::default())),
        };
        let detailed_status = StreamingIndexingStatus {
            pending_partition_count: Some(1),
            v2_pending_partitions: Some(vec![test_pending_partition_status(
                "root/0",
                7,
                Some(3),
                None,
                Some(StreamingIndexingTrainerSubphase::PlanCuts),
            )]),
            ..test_streaming_status(
                StreamingIndexingPhase::PlanningPass { pass_number: 2 },
                StreamingIndexingStatusState::InProgress,
                12,
                Some(12),
                4,
                Some(8),
                Duration::from_millis(250),
                None,
            )
        };
        let unknown_status = test_streaming_status(
            StreamingIndexingPhase::PlanningPass { pass_number: 2 },
            StreamingIndexingStatusState::InProgress,
            12,
            Some(12),
            4,
            Some(8),
            Duration::from_millis(260),
            None,
        );

        let _ = telemetry.project_planning_status(&detailed_status);
        let _ = telemetry.project_planning_status(&unknown_status);
        let (message, record) = telemetry.project_pass_summary(
            test_planning_pass_report(1, 3),
            &PlanningCompletionAction::ReplayRequired("still pending".into()),
        );

        assert_eq!(
            record.last_known_blocked_on_summary.as_deref(),
            Some(
                "1 pending partition(s); pending detail root/0 expects 7 item(s), observed 3, subphase plan-cuts"
            )
        );
        assert!(message.contains(
            "last blocked on 1 pending partition(s); pending detail root/0 expects 7 item(s), observed 3, subphase plan-cuts"
        ));
    }

    #[test]
    fn planning_pass_telemetry_serializes_concurrent_jsonl_writes() {
        let temp = tempdir().unwrap();
        let telemetry_path = temp.path().join("planning-pass-telemetry.jsonl");
        let telemetry = PlanningTelemetryContext {
            run_identity: PlanningRunIdentity {
                effective_profile_version: "0.7.0".into(),
                delegated_contract_family: DelegatedContractFamily::V2,
            },
            sink_path: Some(telemetry_path.clone()),
            sink_initialized: Arc::new(AtomicBool::new(false)),
            sink_write_lock: Arc::new(Mutex::new(())),
            diagnosis_state: Arc::new(Mutex::new(PlanningTelemetryState::default())),
        };
        let workers = (0..8)
            .map(|index| {
                let telemetry = telemetry.clone();
                std::thread::spawn(move || {
                    let record = PlanningPassTelemetryRecord {
                        telemetry_kind: "pass-summary",
                        effective_profile_version: "0.7.0".into(),
                        delegated_contract_family: DelegatedContractFamily::V2,
                        completed_pass_count: index + 1,
                        observed_item_count: 2,
                        requested_planning_cluster_count: Some(64),
                        realized_planning_cluster_count: Some(48),
                        planning_quality_metric: 0.25,
                        planning_balance_metric: 0.125,
                        planned_partition_count: 12,
                        terminal_partition_count: 9,
                        hierarchy_depth: 4,
                        convergence_verdict: PlanningConvergenceVerdict::Inconclusive,
                        convergence_evidence_summary: format!("worker {index}"),
                        last_known_blocked_on_summary: Some(format!("worker {index} blocked")),
                        planning_completion_state: "replay-required".into(),
                        planning_completion_reason: Some("still pending".into()),
                    };
                    telemetry.write_json_record(&record).unwrap();
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }

        let written = fs::read_to_string(&telemetry_path).unwrap();
        let lines = written.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 8);
        for line in lines {
            let parsed = serde_json::from_str::<serde_json::Value>(line).unwrap();
            assert_eq!(parsed["telemetry_kind"], "pass-summary");
        }
    }

    #[test]
    fn delegated_contract_family_serialization_matches_operator_visible_strings() {
        assert_eq!(
            serde_json::to_string(&DelegatedContractFamily::LegacyNonV2).unwrap(),
            "\"legacy/non-v2\""
        );
        assert_eq!(
            serde_json::to_string(&DelegatedContractFamily::V2).unwrap(),
            "\"v2\""
        );
    }

    #[test]
    fn planning_pass_telemetry_write_failures_are_reported_without_failing_progress() {
        let temp = tempdir().unwrap();
        let occupied = temp.path().join("occupied");
        fs::write(&occupied, b"not a directory").unwrap();
        let telemetry = PlanningTelemetryContext {
            run_identity: PlanningRunIdentity {
                effective_profile_version: "0.7.0".into(),
                delegated_contract_family: DelegatedContractFamily::V2,
            },
            sink_path: Some(occupied.join("planning-pass-telemetry.jsonl")),
            sink_initialized: Arc::new(AtomicBool::new(false)),
            sink_write_lock: Arc::new(Mutex::new(())),
            diagnosis_state: Arc::new(Mutex::new(PlanningTelemetryState::default())),
        };
        let progress_messages = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_progress = Arc::clone(&progress_messages);
        let progress: ProgressReporter = Arc::new(move |message| {
            captured_progress.lock().unwrap().push(message);
        });

        report_planning_pass_completion(
            &progress,
            Some(&telemetry),
            test_planning_pass_report(1, 3),
            &PlanningCompletionAction::ReplayRequired(
                "Planning pass 1 requires another full replay pass before v2 planning can complete: still pending"
                    .into(),
            ),
        )
        .unwrap();

        let progress_messages = progress_messages.lock().unwrap();
        assert!(progress_messages[0].contains("profile 0.7.0 via v2"));
        assert!(
            progress_messages[1].contains("Failed to create planning pass telemetry directory for")
        );
        assert!(progress_messages[2].contains("requires another full replay pass"));
    }

    #[test]
    fn clustering_failure_input_uses_content_hash_for_inline_logical_id() {
        let alpha = IndexItem {
            metadata: vec![],
            content_ref: ContentRef::Inline {
                media_type: "text/plain".into(),
                body: b"alpha".to_vec(),
            },
        };
        let beta = IndexItem {
            metadata: vec![],
            content_ref: ContentRef::Inline {
                media_type: "text/plain".into(),
                body: b"beta".to_vec(),
            },
        };

        let alpha = clustering_failure_input(&alpha);
        let beta = clustering_failure_input(&beta);
        match (&alpha, &beta) {
            (
                ClusteringFailureInput::Inline {
                    logical_id: alpha_id,
                    media_type: alpha_type,
                },
                ClusteringFailureInput::Inline {
                    logical_id: beta_id,
                    media_type: beta_type,
                },
            ) => {
                assert_eq!(alpha_type, "text/plain");
                assert_eq!(beta_type, "text/plain");
                assert!(alpha_id.starts_with("inline:text/plain:"));
                assert!(beta_id.starts_with("inline:text/plain:"));
                assert_ne!(alpha_id, beta_id);
            }
            other => panic!("expected inline diagnostics, got {other:?}"),
        }
    }

    #[test]
    fn embedding_health_diagnostics_reports_degenerate_signals_and_samples() {
        let temp = tempdir().unwrap();
        let store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &local_test_environment(String::new()),
        )
        .unwrap();
        let resolver = LocalFilesystemContentResolver::new(store);
        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        let replay_batches = vec![ReplayBatch {
            items: vec![
                IndexItem {
                    metadata: vec![],
                    content_ref: ContentRef::Inline {
                        media_type: "text/plain".into(),
                        body: b"alpha".to_vec(),
                    },
                },
                IndexItem {
                    metadata: vec![],
                    content_ref: ContentRef::Inline {
                        media_type: "text/plain".into(),
                        body: b"beta".to_vec(),
                    },
                },
                IndexItem {
                    metadata: vec![],
                    content_ref: ContentRef::Inline {
                        media_type: "text/plain".into(),
                        body: b"gamma".to_vec(),
                    },
                },
                IndexItem {
                    metadata: vec![],
                    content_ref: ContentRef::Inline {
                        media_type: "text/plain".into(),
                        body: b"delta".to_vec(),
                    },
                },
            ],
            audit_records: Vec::new(),
            completion_message: None,
        }];
        let inputs = replay_batches[0]
            .items
            .iter()
            .map(clustering_failure_input)
            .collect::<Vec<_>>();
        let embeddings_by_input_hash = HashMap::from([
            (
                hash_embedding_content("text/plain", b"alpha").into_bytes(),
                [0.0f32, 0.0]
                    .into_iter()
                    .flat_map(f32::to_le_bytes)
                    .collect(),
            ),
            (
                hash_embedding_content("text/plain", b"beta").into_bytes(),
                [1.0f32, 1.0]
                    .into_iter()
                    .flat_map(f32::to_le_bytes)
                    .collect(),
            ),
            (
                hash_embedding_content("text/plain", b"gamma").into_bytes(),
                [1.0f32, 1.0]
                    .into_iter()
                    .flat_map(f32::to_le_bytes)
                    .collect(),
            ),
            (
                hash_embedding_content("text/plain", b"delta").into_bytes(),
                [f32::NAN, 0.0]
                    .into_iter()
                    .flat_map(f32::to_le_bytes)
                    .collect(),
            ),
        ]);
        let source = StoredLeafEmbeddingProvider {
            embeddings_by_input_hash: Arc::new(embeddings_by_input_hash),
        };

        let diagnostics = build_embedding_health_diagnostics(
            &resolver,
            &source,
            &replay_batches,
            &inputs,
            &embedding_spec,
        );

        assert_eq!(diagnostics.available_embedding_count, 4);
        assert_eq!(diagnostics.missing_embedding_count, 0);
        assert_eq!(diagnostics.undecodable_embedding_count, 0);
        assert_eq!(diagnostics.non_finite_embedding_count, 1);
        assert_eq!(diagnostics.zero_vector_count, 1);
        assert_eq!(diagnostics.repeated_embedding_count, 1);
        assert_eq!(diagnostics.unique_embedding_count, 2);
        assert_eq!(diagnostics.repeated_embedding_group_count, 1);
        assert_eq!(diagnostics.max_repeated_embedding_occurrence, Some(2));
        assert_eq!(diagnostics.top_repeated_embedding_groups.len(), 1);
        assert_eq!(
            diagnostics.top_repeated_embedding_groups[0].occurrence_count,
            2
        );
        assert_eq!(
            diagnostics.top_repeated_embedding_groups[0].sample_inputs[0].content_fingerprint,
            Some(hash_embedding_content("text/plain", b"beta").to_string())
        );
        assert_eq!(diagnostics.suspicious_input_sample.len(), 4);
        assert!(
            diagnostics
                .suspicious_input_sample
                .iter()
                .any(|sample| sample.reasons.iter().any(|reason| reason == "zero-vector"))
        );
        assert!(diagnostics.suspicious_input_sample.iter().any(|sample| {
            sample
                .reasons
                .iter()
                .any(|reason| reason == "repeated-embedding")
        }));
        assert!(diagnostics.suspicious_input_sample.iter().any(|sample| {
            sample
                .reasons
                .iter()
                .any(|reason| reason == "non-finite-embedding")
        }));
    }

    #[test]
    fn write_clustering_failure_diagnostics_file_creates_parent_directories() {
        let temp = tempdir().unwrap();
        let output_path = temp
            .path()
            .join("nested")
            .join("summary.clustering-failure-diagnostics.json");

        write_clustering_failure_diagnostics_file(
            &output_path,
            &sample_clustering_failure_diagnostics(),
        )
        .unwrap();

        let written = fs::read_to_string(&output_path).unwrap();
        assert!(written.contains("\"stage\": \"full-pipeline\""));
        assert!(written.contains("\"profile_version\": \"0.1.0\""));
        assert!(written.contains("\"planning_algorithm_id\": \"spherical-kmeans\""));
        assert!(written.contains("\"embedding_health\""));
        assert!(written.contains("\"source_path\": \"alpha.txt\""));
    }

    #[test]
    fn parent_directory_to_create_skips_empty_relative_parent() {
        let nested_summary = Path::new("nested").join("summary.json");
        assert_eq!(parent_directory_to_create(Path::new("summary.json")), None);
        assert_eq!(
            parent_directory_to_create(nested_summary.as_path()),
            Some(Path::new("nested"))
        );
    }

    fn sample_clustering_failure_diagnostics() -> ClusteringFailureDiagnostics {
        let embedding_health = EmbeddingHealthDiagnostics {
            available_embedding_count: 1,
            missing_embedding_count: 0,
            embedding_lookup_error_count: 0,
            undecodable_embedding_count: 0,
            non_finite_embedding_count: 0,
            zero_vector_count: 1,
            repeated_embedding_count: 0,
            unique_embedding_count: 1,
            repeated_embedding_group_count: 0,
            max_repeated_embedding_occurrence: None,
            min_l2_norm: Some(0.0),
            max_l2_norm: Some(0.0),
            mean_l2_norm: Some(0.0),
            non_zero_variance_dimension_count: Some(0),
            max_component_variance: Some(0.0),
            top_repeated_embedding_groups: Vec::new(),
            embedding_lookup_error_sample: Vec::new(),
            suspicious_input_sample: vec![SuspiciousClusteringFailureInput {
                input: ClusteringFailureInput::Document {
                    logical_id: "document:alpha.txt".into(),
                    source_path: "alpha.txt".into(),
                },
                reasons: vec!["zero-vector".into(), "collapsed-variance-population".into()],
                embedding_fingerprint: Some(
                    "af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc".into(),
                ),
                l2_norm: Some(0.0),
            }],
        };
        ClusteringFailureDiagnostics {
            stage: ExecutionStage::FullPipeline,
            embedding_spec: ClusteringFailureEmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            clustering: EffectiveClusteringDiagnostics {
                profile_version: "0.1.0".into(),
                planning_algorithm_id: "spherical-kmeans".into(),
                planning_direction: None,
                packing_strategy_id: Some("cluster-order-balanced-range-packer-v1".into()),
                hierarchy_strategy_id: "greedy-pack".into(),
                summary_policy_id: "exact-centroid".into(),
                cluster_count: Some(157),
                random_seed: Some(11),
            },
            embedding_health: embedding_health.clone(),
            failing_subset: Some(FailingSubsetDiagnostics {
                phase: FailingSubsetPhaseDiagnostics::HierarchyPlanning {
                    stage: "single-stage planning".into(),
                },
                provenance: FailingSubsetProvenance::NarrowestProvable,
                basis: "the upstream failure surface reported 1 active item(s) for the failing step but did not expose repository-visible identities for a narrower subset, so the top-level clustering attempt remains the narrowest provable repository-visible subset".into(),
                upstream_active_item_count: 1,
                upstream_completed_unit_count: 0,
                upstream_phase_total_unit_count: Some(1),
                repository_visible_subset: RepositoryVisibleSubsetDiagnostics::SameAsTopLevelAttempt {
                    input_count: 1,
                },
                embedding_health,
            }),
            input_count: 1,
            inputs: vec![ClusteringFailureInput::Document {
                logical_id: "document:alpha.txt".into(),
                source_path: "alpha.txt".into(),
            }],
        }
    }

    #[test]
    fn failing_subset_diagnostics_marks_exact_top_level_match() {
        let diagnostics = build_failing_subset_diagnostics(
            &test_streaming_status(
                StreamingIndexingPhase::HierarchyPlanning {
                    stage: PlanningStage::Single,
                },
                StreamingIndexingStatusState::Failed,
                3,
                Some(3),
                0,
                Some(3),
                Duration::from_secs(1),
                Some("boom"),
            ),
            3,
            &sample_clustering_failure_diagnostics().embedding_health,
        );

        assert_eq!(diagnostics.provenance, FailingSubsetProvenance::Exact);
        assert_eq!(
            diagnostics.repository_visible_subset,
            RepositoryVisibleSubsetDiagnostics::SameAsTopLevelAttempt { input_count: 3 }
        );
        assert!(
            diagnostics
                .basis
                .contains("same active item count as the top-level clustering attempt")
        );
    }

    #[test]
    fn failing_subset_diagnostics_falls_back_to_narrowest_provable_top_level_subset() {
        let diagnostics = build_failing_subset_diagnostics(
            &test_streaming_status(
                StreamingIndexingPhase::HierarchyPlanning {
                    stage: PlanningStage::Single,
                },
                StreamingIndexingStatusState::Failed,
                1,
                Some(1),
                0,
                Some(1),
                Duration::from_secs(1),
                Some("boom"),
            ),
            3,
            &sample_clustering_failure_diagnostics().embedding_health,
        );

        assert_eq!(
            diagnostics.provenance,
            FailingSubsetProvenance::NarrowestProvable
        );
        assert_eq!(diagnostics.upstream_active_item_count, 1);
        assert!(
            diagnostics
                .basis
                .contains("did not expose repository-visible identities")
        );
    }

    #[test]
    fn effective_clustering_diagnostics_uses_published_profile_metadata() {
        let clustering = ClusteringConfigOverrides::default()
            .to_configured_clustering(
                lexongraph_streaming_indexer::PUBLISHED_PROFILE_V0_1_0,
                &local_test_environment(String::new()),
            )
            .expect("published profile config");
        let diagnostics =
            effective_clustering_diagnostics(&clustering).expect("published profile diagnostics");

        assert_eq!(diagnostics.profile_version, "0.1.0");
        assert_eq!(diagnostics.planning_algorithm_id, "spherical-kmeans");
        assert_eq!(diagnostics.planning_direction, None);
        assert_eq!(
            diagnostics.packing_strategy_id,
            Some("cluster-order-balanced-range-packer-v1".into())
        );
        assert_eq!(diagnostics.hierarchy_strategy_id, "greedy-pack");
        assert_eq!(diagnostics.summary_policy_id, "exact-centroid");
        assert_eq!(diagnostics.cluster_count, Some(157));
        assert_eq!(diagnostics.random_seed, Some(11));
    }

    #[test]
    fn effective_clustering_diagnostics_reflects_local_testing_cluster_override() {
        let clustering = ClusteringConfigOverrides {
            profile_version: Some(PublishedProfileVersion::new(0, 6, 0)),
            local_testing_cluster_count: Some(32),
        }
        .to_configured_clustering(
            PublishedProfileVersion::new(0, 6, 0),
            &local_test_environment(String::new()),
        )
        .expect("published profile config");
        let diagnostics =
            effective_clustering_diagnostics(&clustering).expect("published profile diagnostics");

        assert_eq!(diagnostics.profile_version, "0.6.0");
        assert_eq!(diagnostics.cluster_count, Some(32));
    }

    #[test]
    fn streaming_indexer_v2_selection_follows_effective_profile_precedence() {
        let default_v2 = ClusteringConfigOverrides::default()
            .to_configured_clustering(
                PUBLISHED_PROFILE_V0_7_0,
                &local_test_environment(String::new()),
            )
            .expect("v2 published profile config");
        let default_legacy = ClusteringConfigOverrides::default()
            .to_configured_clustering(
                PUBLISHED_PROFILE_V0_1_0,
                &local_test_environment(String::new()),
            )
            .expect("legacy published profile config");
        let cli_to_v2 = ClusteringConfigOverrides {
            profile_version: Some(PUBLISHED_PROFILE_V0_7_0),
            local_testing_cluster_count: None,
        }
        .to_configured_clustering(
            PUBLISHED_PROFILE_V0_1_0,
            &local_test_environment(String::new()),
        )
        .expect("cli-selected v2 published profile config");
        let cli_to_legacy = ClusteringConfigOverrides {
            profile_version: Some(PUBLISHED_PROFILE_V0_1_0),
            local_testing_cluster_count: None,
        }
        .to_configured_clustering(
            PUBLISHED_PROFILE_V0_7_0,
            &local_test_environment(String::new()),
        )
        .expect("cli-selected legacy published profile config");

        assert!(uses_streaming_indexer_v2(&default_v2));
        assert!(!uses_streaming_indexer_v2(&default_legacy));
        assert!(uses_streaming_indexer_v2(&cli_to_v2));
        assert!(!uses_streaming_indexer_v2(&cli_to_legacy));
    }

    #[test]
    fn serialized_branch_size_matches_actual_serialization() {
        let embedding_spec = EmbeddingSpec {
            dims: 384,
            encoding: "f32le".into(),
        };
        let entry_count = 37;
        let embedding_len = expected_embedding_len(&embedding_spec).unwrap();
        let entries = (0..entry_count)
            .map(|index| lexongraph_block::BranchEntry {
                embedding: vec![0; embedding_len],
                child: BlockHash::from_bytes({
                    let mut bytes = [0_u8; 32];
                    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
                    bytes
                }),
            })
            .collect();
        let branch = lexongraph_block::build_branch_block(
            VERSION_1,
            1,
            embedding_spec.clone(),
            entries,
            None,
        )
        .unwrap();
        let serialized = lexongraph_block::serialize_block(&Block::Branch(branch)).unwrap();

        assert_eq!(
            serialized_branch_size(&embedding_spec, entry_count).unwrap(),
            serialized.bytes.len()
        );
    }

    #[test]
    fn planning_pass_progress_reports_v2_pending_partition_details_and_suspected_stall() {
        let status = StreamingIndexingStatus {
            pending_partition_count: Some(3),
            v2_pending_partitions: Some(vec![
                test_pending_partition_status(
                    "root/0",
                    7,
                    Some(3),
                    None,
                    Some(StreamingIndexingTrainerSubphase::PlanCuts),
                ),
                test_pending_partition_status("root/1", 5, Some(2), Some(vec![1, 1]), None),
                test_pending_partition_status("root/2", 4, Some(1), None, None),
            ]),
            suspected_stall: Some(
                lexongraph_streaming_indexer::StreamingIndexingSuspectedStall {
                    reason: StreamingIndexingSuspectedStallReason::UnchangedTrainerSubphase,
                    duration_without_progress: Duration::from_secs(7),
                },
            ),
            ..test_streaming_status(
                StreamingIndexingPhase::PlanningPass { pass_number: 2 },
                StreamingIndexingStatusState::InProgress,
                12,
                Some(12),
                4,
                Some(8),
                Duration::from_millis(250),
                None,
            )
        };

        assert_eq!(
            format_indexing_status(status),
            "Planning pass 2 still running after 250 ms for 12 item(s); completed 4 of 12 pass item(s); pending partition(s) 3; pending detail root/0 expects 7 item(s), observed 3, subphase plan-cuts | root/1 expects 5 item(s), observed 2, bucket fill [1, 1] | +1 more; suspected stall unchanged-trainer-subphase for 7000 ms"
        );
    }

    #[test]
    fn hierarchy_planning_progress_reports_stage_local_counts() {
        let status = test_streaming_status(
            StreamingIndexingPhase::HierarchyPlanning {
                stage: PlanningStage::Custom,
            },
            StreamingIndexingStatusState::InProgress,
            7,
            None,
            7,
            None,
            Duration::from_millis(125),
            None,
        );

        assert_eq!(
            format_indexing_status(status),
            "custom planning still running after 125 ms; processed 7 stage-local item(s)"
        );
    }

    #[test]
    fn final_materialization_progress_reports_replay_totals_when_available() {
        let status = test_streaming_status(
            StreamingIndexingPhase::FinalMaterializationReplay,
            StreamingIndexingStatusState::InProgress,
            9,
            Some(9),
            4,
            Some(5),
            Duration::from_millis(250),
            None,
        );

        assert_eq!(
            format_indexing_status(status),
            "Final materialization replay still running after 250 ms for 9 item(s); completed 4 of 9 replay item(s)"
        );
    }

    #[test]
    fn bottom_up_assembly_progress_distinguishes_input_blocks_from_groups() {
        let status = test_streaming_status(
            StreamingIndexingPhase::BottomUpAssembly { layer_index: 2 },
            StreamingIndexingStatusState::Completed,
            12,
            Some(3),
            3,
            Some(0),
            Duration::from_millis(88),
            None,
        );

        assert_eq!(
            format_indexing_status(status),
            "Bottom-up assembly for layer 2 completed in 88 ms: built 3 of 3 group(s) from 12 input block(s)"
        );
    }

    #[test]
    fn bottom_up_assembly_progress_handles_unknown_group_total() {
        let status = test_streaming_status(
            StreamingIndexingPhase::BottomUpAssembly { layer_index: 1 },
            StreamingIndexingStatusState::InProgress,
            8,
            None,
            2,
            None,
            Duration::from_millis(44),
            None,
        );

        assert_eq!(
            format_indexing_status(status),
            "Bottom-up assembly for layer 1 still running after 44 ms; completed 2 group(s) so far from 8 input block(s)"
        );
    }

    #[test]
    fn bottom_up_assembly_started_message_omits_elapsed_clause() {
        let status = test_streaming_status(
            StreamingIndexingPhase::BottomUpAssembly { layer_index: 2 },
            StreamingIndexingStatusState::Started,
            12,
            Some(3),
            0,
            Some(3),
            Duration::from_millis(0),
            None,
        );

        assert_eq!(
            format_indexing_status(status),
            "Bottom-up assembly for layer 2 started for 12 input block(s) across 3 group(s)"
        );
    }

    #[test]
    fn bottom_up_assembly_started_message_handles_unknown_group_total() {
        let status = test_streaming_status(
            StreamingIndexingPhase::BottomUpAssembly { layer_index: 1 },
            StreamingIndexingStatusState::Started,
            8,
            None,
            0,
            None,
            Duration::from_millis(0),
            None,
        );

        assert_eq!(
            format_indexing_status(status),
            "Bottom-up assembly for layer 1 started for 8 input block(s) across an unknown group total"
        );
    }

    #[test]
    fn hierarchy_planning_failure_uses_single_temporal_clause() {
        let status = test_streaming_status(
            StreamingIndexingPhase::HierarchyPlanning {
                stage: PlanningStage::Custom,
            },
            StreamingIndexingStatusState::Failed,
            7,
            None,
            3,
            None,
            Duration::from_millis(125),
            Some("boom"),
        );

        assert_eq!(
            format_indexing_status(status),
            "custom planning failed after 125 ms; processed 3 stage-local item(s): boom"
        );
    }

    #[test]
    fn bottom_up_assembly_failure_uses_single_temporal_clause() {
        let status = test_streaming_status(
            StreamingIndexingPhase::BottomUpAssembly { layer_index: 2 },
            StreamingIndexingStatusState::Failed,
            12,
            Some(3),
            2,
            Some(1),
            Duration::from_millis(88),
            Some("boom"),
        );

        assert_eq!(
            format_indexing_status(status),
            "Bottom-up assembly for layer 2 failed after 88 ms; completed 2 of 3 group(s) from 12 input block(s): boom"
        );
    }

    #[tokio::test]
    async fn ingestion_only_execution_ignores_default_clustering_profile() {
        let temp = tempdir().unwrap();
        let document = temp.path().join("alpha.txt");
        fs::write(&document, b"alpha\n").unwrap();

        let request_path = temp.path().join("request.json");
        fs::write(
            &request_path,
            serde_json::to_vec_pretty(&json!({
                "environment": {
                    "kind": "local",
                    "block_store_root": "blocks",
                    "embedding": {
                        "base_url": "http://localhost:9999",
                        "model": "all-MiniLM-L6-v2",
                        "request_timeout_secs": 5,
                        "max_retries": 0,
                        "retry_delay_ms": 1
                    }
                },
                "embedding_spec": {
                    "dims": 2,
                    "encoding": "f32le"
                },
                "ref_name": "test-branch",
                "items": [
                    { "kind": "document", "path": "alpha.txt" }
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        let error = run_request_file_with_overrides(
            &request_path,
            Some(ExecutionStage::IngestionAndEmbedding),
            ClusteringConfigOverrides::default(),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, RuntimeError::Provider(_)));
    }

    #[tokio::test]
    async fn higher_leaf_concurrency_preserves_outputs() {
        let temp = tempdir().unwrap();
        let document_a = temp.path().join("alpha.txt");
        let document_b = temp.path().join("beta.txt");
        let document_c = temp.path().join("gamma.txt");
        fs::write(&document_a, b"alpha\n").unwrap();
        fs::write(&document_b, b"beta\n").unwrap();
        fs::write(&document_c, b"gamma\n").unwrap();

        let server = spawn_embedding_server_with_delay(4, Duration::from_millis(10));
        let base_request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: Some(1),
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![
                BatchItemConfig::Document {
                    path: document_a.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Document {
                    path: document_b.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Document {
                    path: document_c.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
            ],
        };

        let serial = run_request(temp.path(), base_request.clone())
            .await
            .unwrap();
        let parallel = run_request(
            temp.path(),
            BatchRequest {
                max_concurrency: Some(3),
                ..base_request
            },
        )
        .await
        .unwrap();

        assert_eq!(serial.root_id, parallel.root_id);
        assert_eq!(serial.block_ids, parallel.block_ids);
        server.join();
    }

    #[tokio::test]
    async fn higher_leaf_concurrency_preserves_mailbox_outputs() {
        let temp = tempdir().unwrap();
        let mailbox_path = temp.path().join("2026-05.mbox");
        fs::write(
            &mailbox_path,
            concat!(
                "From alan@example.com Sat Jan 03 10:00:00 2026\n",
                "Subject: One\n",
                "\n",
                "First body.\n",
                "From alan@example.com Sat Jan 03 10:05:00 2026\n",
                "Subject: Two\n",
                "\n",
                "Second body.\n",
                "From alan@example.com Sat Jan 03 10:10:00 2026\n",
                "Subject: Three\n",
                "\n",
                "Third body.\n"
            ),
        )
        .unwrap();

        let server = spawn_embedding_server_with_delay(4, Duration::from_millis(10));
        let base_request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: Some(1),
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![BatchItemConfig::Mailbox {
                path: mailbox_path
                    .strip_prefix(temp.path())
                    .unwrap()
                    .to_path_buf(),
                metadata: BTreeMap::new(),
            }],
        };

        let serial = run_request(temp.path(), base_request.clone())
            .await
            .unwrap();
        let parallel = run_request(
            temp.path(),
            BatchRequest {
                max_concurrency: Some(3),
                ..base_request
            },
        )
        .await
        .unwrap();

        assert_eq!(serial.root_id, parallel.root_id);
        assert_eq!(serial.block_ids, parallel.block_ids);
        server.join();
    }

    #[tokio::test]
    async fn max_concurrency_allows_multiple_leaf_embeddings_in_flight() {
        let temp = tempdir().unwrap();
        let document_a = temp.path().join("alpha.txt");
        let document_b = temp.path().join("beta.txt");
        let document_c = temp.path().join("gamma.txt");
        fs::write(&document_a, b"alpha\n").unwrap();
        fs::write(&document_b, b"beta\n").unwrap();
        fs::write(&document_c, b"gamma\n").unwrap();

        let server = spawn_embedding_server_with_delay(3, Duration::from_millis(75));
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: Some(3),
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![
                BatchItemConfig::Document {
                    path: document_a.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Document {
                    path: document_b.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Document {
                    path: document_c.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
            ],
        };

        let summary = run_request(temp.path(), request).await.unwrap();
        assert!(!summary.block_ids.is_empty());
        assert!(server.max_in_flight() > 1);
        server.join();
    }

    #[tokio::test]
    async fn max_concurrency_caps_full_pipeline_embedding_requests() {
        let temp = tempdir().unwrap();
        let document_names = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];
        let items = document_names
            .iter()
            .map(|name| {
                let path = temp.path().join(format!("{name}.txt"));
                fs::write(&path, format!("{name}\n")).unwrap();
                BatchItemConfig::Document {
                    path: path.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                }
            })
            .collect::<Vec<_>>();

        let server = spawn_embedding_server_with_delay(6, Duration::from_millis(75));
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: Some(3),
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items,
        };

        let summary = run_request(temp.path(), request).await.unwrap();
        assert!(!summary.block_ids.is_empty());
        assert!(server.max_in_flight() <= 3);
        server.join();
    }

    #[tokio::test]
    async fn max_concurrency_caps_ingestion_only_embedding_requests() {
        let temp = tempdir().unwrap();
        let document_names = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];
        let items = document_names
            .iter()
            .map(|name| {
                let path = temp.path().join(format!("{name}.txt"));
                fs::write(&path, format!("{name}\n")).unwrap();
                BatchItemConfig::Document {
                    path: path.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                }
            })
            .collect::<Vec<_>>();

        let server = spawn_embedding_server_with_delay(6, Duration::from_millis(75));
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::IngestionAndEmbedding,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: Some(3),
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items,
        };

        let summary = run_request(temp.path(), request).await.unwrap();
        assert_eq!(summary.root_id, INGESTION_ONLY_ROOT_ID_PLACEHOLDER);
        assert!(summary.block_count > 0);
        assert!(server.max_in_flight() <= 3);
        server.join();
    }

    #[tokio::test]
    async fn clustering_only_stage_matches_full_pipeline_with_request_items_in_non_sorted_order() {
        let temp = tempdir().unwrap();
        let document_a = temp.path().join("alpha.txt");
        let document_b = temp.path().join("beta.txt");
        fs::write(&document_a, b"alpha\n").unwrap();
        fs::write(&document_b, b"beta\n").unwrap();

        let server = spawn_embedding_server(2);
        let full_request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: Some(2),
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![
                BatchItemConfig::Document {
                    path: document_b.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Document {
                    path: document_a.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
            ],
        };
        let seeded = run_request(temp.path(), full_request).await.unwrap();

        let cluster_only_request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::ClusteringAndBlockAssembly,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: None,
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![],
        };

        let clustered = run_request(temp.path(), cluster_only_request)
            .await
            .unwrap();

        assert_eq!(clustered.root_id, seeded.root_id);
        assert_eq!(clustered.block_ids, seeded.block_ids);
        server.join();
    }

    #[tokio::test]
    async fn clustering_only_replay_batches_respect_max_concurrency() {
        let temp = tempdir().unwrap();
        let document_names = ["alpha", "beta", "gamma", "delta", "epsilon"];
        let items = document_names
            .iter()
            .map(|name| {
                let path = temp.path().join(format!("{name}.txt"));
                fs::write(&path, format!("{name}\n")).unwrap();
                BatchItemConfig::Document {
                    path: path.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                }
            })
            .collect::<Vec<_>>();

        let server = spawn_embedding_server(document_names.len());
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::FullPipeline,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: Some(2),
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items,
        };
        run_request(temp.path(), request).await.unwrap();

        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        let progress: ProgressReporter = Arc::new(|_| {});
        let block_store_root = temp.path().join("blocks");
        let mutable_ref_store = local_mutable_ref_store_location(&block_store_root, TEST_REF_NAME);
        let io = RuntimeIo {
            mutable_ref_store: Some(&mutable_ref_store),
            mutable_ref_metadata: None,
            planning_telemetry: None,
            progress: &progress,
        };
        let (replay_batches, _) =
            load_replay_batches_from_store(&block_store, &embedding_spec, 2, 2, io).unwrap();

        assert_eq!(replay_batches.len(), 3);
        assert_eq!(replay_batches[0].items.len(), 2);
        assert_eq!(replay_batches[1].items.len(), 2);
        assert_eq!(replay_batches[2].items.len(), 1);
        server.join();
    }

    #[tokio::test]
    async fn clustering_only_requires_replay_journal_head() {
        let temp = tempdir().unwrap();
        let document_names = ["alpha", "beta"];
        let items = document_names
            .iter()
            .map(|name| {
                let path = temp.path().join(format!("{name}.txt"));
                fs::write(&path, format!("{name}\n")).unwrap();
                BatchItemConfig::Document {
                    path: path.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                }
            })
            .collect::<Vec<_>>();

        let server = spawn_embedding_server(document_names.len());
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::IngestionAndEmbedding,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: Some(2),
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items,
        };
        run_request(temp.path(), request).await.unwrap();

        let block_store_root = temp.path().join("blocks");
        assert!(mutable_ref_store_path(&block_store_root, TEST_REF_NAME).exists());

        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        let invalid_leaf = build_leaf_block(
            VERSION_1,
            embedding_spec.clone(),
            vec![LeafEntry {
                embedding: vec![0, 0, 0, 0, 0, 0, 128, 63],
                metadata: vec![(Value::Text("note".into()), Value::Text("orphan".into()))],
                content: Content {
                    media_type: "text/plain".into(),
                    body: b"orphan".to_vec(),
                },
            }],
            None,
        )
        .unwrap();
        put_block(&block_store, &Block::Leaf(invalid_leaf));

        let mutable_ref_store = local_mutable_ref_store_location(&block_store_root, TEST_REF_NAME);
        let progress: ProgressReporter = Arc::new(|_| {});
        let io = RuntimeIo {
            mutable_ref_store: Some(&mutable_ref_store),
            mutable_ref_metadata: None,
            planning_telemetry: None,
            progress: &progress,
        };
        let (replay_batches, _) =
            load_replay_batches_from_store(&block_store, &embedding_spec, 8, 2, io).unwrap();
        let replay_item_count: usize = replay_batches.iter().map(|batch| batch.items.len()).sum();
        assert_eq!(replay_item_count, document_names.len());

        fs::remove_file(mutable_ref_store_path(&block_store_root, TEST_REF_NAME)).unwrap();
        let error =
            load_replay_batches_from_store(&block_store, &embedding_spec, 8, 2, io).unwrap_err();
        assert!(matches!(
            error,
            RuntimeError::MissingReplayJournalHead { .. }
        ));
        server.join();
    }

    #[test]
    fn replay_journal_record_normalizes_document_paths() {
        let item = IndexItem {
            metadata: vec![],
            content_ref: ContentRef::Document {
                path: PathBuf::from(r"C:\temp\alpha.txt"),
            },
        };

        let record = replay_journal_record_from_item(BlockHash::from_bytes([7u8; 32]), &item);
        assert!(matches!(
            record,
            ReplayJournalRecord::ReplayInput {
                step_kind: ReplayJournalStepKind::Embedding,
                content_ref: ReplayJournalContentRef::Document { path },
                ..
            } if path == "C:/temp/alpha.txt"
        ));
    }

    #[test]
    fn replay_sort_key_sql_preserves_metadata_prefix_ordering() {
        let shorter = IndexItem {
            metadata: vec![(Value::Text("alpha".into()), Value::Text("beta".into()))],
            content_ref: ContentRef::Inline {
                media_type: "text/plain".into(),
                body: b"same".to_vec(),
            },
        };
        let longer = IndexItem {
            metadata: vec![
                (Value::Text("alpha".into()), Value::Text("beta".into())),
                (Value::Text("gamma".into()), Value::Text("delta".into())),
            ],
            content_ref: ContentRef::Inline {
                media_type: "text/plain".into(),
                body: b"same".to_vec(),
            },
        };

        let shorter_key = replay_sort_key_sql(&shorter).unwrap();
        let longer_key = replay_sort_key_sql(&longer).unwrap();
        assert!(replay_sort_key(&shorter) < replay_sort_key(&longer));
        assert!(shorter_key < longer_key);
    }

    #[test]
    fn replay_journal_writer_rejects_oversized_payload() {
        let temp = tempdir().unwrap();
        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let block_store_root = temp.path().join("blocks");
        let mutable_ref_store = local_mutable_ref_store_location(&block_store_root, TEST_REF_NAME);
        prepare_mutable_ref_store(&mutable_ref_store).unwrap();

        let record = ReplayJournalRecord::ReplayInput {
            step_kind: ReplayJournalStepKind::Embedding,
            block_id: BlockHash::from_bytes([9u8; 32]).to_string(),
            metadata: vec![],
            content_ref: ReplayJournalContentRef::Inline {
                media_type: "text/plain".into(),
                body: vec![b'x'; REPLAY_JOURNAL_BLOCK_MAX_BYTES + 1],
            },
        };

        let error =
            append_replay_journal_records(&block_store, &mutable_ref_store, &[record]).unwrap_err();
        assert!(matches!(error, RuntimeError::WriteReplayJournal { .. }));
    }

    #[test]
    fn stored_replay_document_identity_normalizes_windows_paths() {
        let item = IndexItem {
            metadata: vec![],
            content_ref: ContentRef::StoredReplay {
                media_type: "text/plain".into(),
                body: b"alpha".to_vec(),
                identity: ReplayIdentity::Document {
                    source_path: r"C:\temp\alpha.txt".into(),
                },
            },
        };

        assert_eq!(
            clustering_failure_input(&item),
            ClusteringFailureInput::Document {
                logical_id: "document:C:/temp/alpha.txt".into(),
                source_path: "C:/temp/alpha.txt".into(),
            }
        );
        assert_eq!(
            replay_sort_key(&item).0,
            "document:C:/temp/alpha.txt".to_string()
        );
        assert!(matches!(
            replay_journal_record_from_item(BlockHash::from_bytes([7u8; 32]), &item),
            ReplayJournalRecord::ReplayInput {
                content_ref: ReplayJournalContentRef::Document { path },
                ..
            } if path == "C:/temp/alpha.txt"
        ));
    }

    #[test]
    fn externalized_replay_state_defers_payload_reads_until_batch_processing() {
        let temp = tempdir().unwrap();
        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let block_store_root = temp.path().join("blocks");
        let mutable_ref_store = local_mutable_ref_store_location(&block_store_root, TEST_REF_NAME);
        prepare_mutable_ref_store(&mutable_ref_store).unwrap();

        let missing_block_id = BlockHash::from_bytes([9u8; 32]);
        let record = ReplayJournalRecord::ReplayInput {
            step_kind: ReplayJournalStepKind::Embedding,
            block_id: missing_block_id.to_string(),
            metadata: vec![("source_kind".into(), "document".into())],
            content_ref: ReplayJournalContentRef::Document {
                path: "missing.txt".into(),
            },
        };
        let replay_journal_head_block_id =
            append_replay_journal_records(&block_store, &mutable_ref_store, &[record]).unwrap();
        update_mutable_ref_store(
            &mutable_ref_store,
            MutableRefStoreUpdate {
                replay_journal_head_block_id,
                ..MutableRefStoreUpdate::default()
            },
        )
        .unwrap();

        let progress: ProgressReporter = Arc::new(|_| {});
        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        let (replay_state, _) = externalize_replay_batches_from_journal(
            &block_store,
            &embedding_spec,
            1,
            1,
            &mutable_ref_store,
            temp.path(),
            &progress,
        )
        .unwrap();
        assert_eq!(replay_state.total_items, 1);
        assert_eq!(
            replay_state.replay_input_block_ids().unwrap(),
            vec![missing_block_id.to_string()]
        );

        let mut iterator = replay_state.batch_iterator().unwrap();
        let error = iterator.next_batch().unwrap_err();
        assert!(
            matches!(error, RuntimeError::ReadReplayJournal { block_id, .. } if block_id == missing_block_id.to_string())
        );
    }

    #[test]
    fn replay_batch_materialization_worker_count_respects_configured_cap() {
        assert_eq!(replay_batch_materialization_worker_count(0, 4), 0);
        assert!(replay_batch_materialization_worker_count(32, 2) <= 2);
        assert!(replay_batch_materialization_worker_count(3, 8) <= 3);
    }

    #[test]
    fn externalized_replay_batch_materialization_reports_first_batch_order_error() {
        let temp = tempdir().unwrap();
        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        let make_block = |path: &str, body: &[u8]| {
            Block::Leaf(
                build_leaf_block(
                    VERSION_1,
                    embedding_spec.clone(),
                    vec![LeafEntry {
                        embedding: vec![0, 0, 0, 0, 0, 0, 128, 63],
                        metadata: vec![
                            (
                                Value::Text("source_kind".into()),
                                Value::Text("document".into()),
                            ),
                            (Value::Text("source_path".into()), Value::Text(path.into())),
                        ],
                        content: Content {
                            media_type: "text/plain".into(),
                            body: body.to_vec(),
                        },
                    }],
                    None,
                )
                .unwrap(),
            )
        };
        let first_block = make_block("C:/docs/first.txt", b"first");
        let second_block = make_block("C:/docs/second.txt", b"second");
        let first_block_id = put_block(&block_store, &first_block);
        let second_block_id = put_block(&block_store, &second_block);
        let first_item = replay_item_from_validated_block(
            &get_block(&block_store, &first_block_id).unwrap(),
            &embedding_spec,
        )
        .unwrap()
        .unwrap()
        .0;
        let second_item = replay_item_from_validated_block(
            &get_block(&block_store, &second_block_id).unwrap(),
            &embedding_spec,
        )
        .unwrap()
        .unwrap()
        .0;
        let replay_order = replay_order_storage_for_entries(&[
            ReplayOrderEntry::new(first_block_id, replay_sort_key_digest(&second_item)),
            ReplayOrderEntry::new(second_block_id, replay_sort_key_digest(&first_item)),
        ]);
        let mut iterator = ExternalizedReplayBatchIterator {
            replay_order_reader: replay_order.open_reader().unwrap(),
            batch_size: 2,
            materialization_max_concurrency: 2,
            block_store,
            embedding_spec,
            current_batch_embeddings: Arc::new(Mutex::new(HashMap::new())),
        };

        let error = iterator.load_next_batch().unwrap_err();
        assert!(
            matches!(error, RuntimeError::InvalidReplayJournalHead { block_id, .. } if block_id == first_block_id.to_string())
        );
    }

    #[test]
    fn externalized_replay_provider_reuses_current_batch_embeddings_without_rereading_blocks() {
        let temp = tempdir().unwrap();
        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let block_store_root = temp.path().join("blocks");
        let mutable_ref_store = local_mutable_ref_store_location(&block_store_root, TEST_REF_NAME);
        prepare_mutable_ref_store(&mutable_ref_store).unwrap();

        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        let metadata = vec![
            (
                Value::Text("source_kind".into()),
                Value::Text("document".into()),
            ),
            (
                Value::Text("source_path".into()),
                Value::Text("C:/docs/alpha.txt".into()),
            ),
        ];
        let expected_embedding = vec![0, 0, 0, 0, 0, 0, 128, 63];
        let block = Block::Leaf(
            build_leaf_block(
                VERSION_1,
                embedding_spec.clone(),
                vec![LeafEntry {
                    embedding: expected_embedding.clone(),
                    metadata,
                    content: Content {
                        media_type: "text/plain".into(),
                        body: b"alpha".to_vec(),
                    },
                }],
                None,
            )
            .unwrap(),
        );
        let block_id = put_block(&block_store, &block);
        let validated = get_block(&block_store, &block_id).unwrap();
        let (item, _) = replay_item_from_validated_block(&validated, &embedding_spec)
            .unwrap()
            .unwrap();
        assert!(matches!(
            &item.content_ref,
            ContentRef::StoredReplay {
                media_type,
                body,
                identity: ReplayIdentity::Document { source_path },
            } if media_type == "text/plain"
                && body == b"alpha"
                && source_path == "C:/docs/alpha.txt"
        ));
        let replay_journal_head_block_id = append_replay_journal_records(
            &block_store,
            &mutable_ref_store,
            &[replay_journal_record_from_item(block_id, &item)],
        )
        .unwrap();
        update_mutable_ref_store(
            &mutable_ref_store,
            MutableRefStoreUpdate {
                replay_journal_head_block_id,
                ..MutableRefStoreUpdate::default()
            },
        )
        .unwrap();

        let progress: ProgressReporter = Arc::new(|_| {});
        let (replay_state, embedding_provider) = externalize_replay_batches_from_journal(
            &block_store,
            &embedding_spec,
            1,
            1,
            &mutable_ref_store,
            temp.path(),
            &progress,
        )
        .unwrap();
        let mut iterator = replay_state.batch_iterator().unwrap();
        let batch = iterator.next_batch().unwrap().unwrap();
        assert_eq!(batch.items.len(), 1);

        fs::remove_dir_all(&block_store_root).unwrap();

        let embedding = embedding_provider
            .load_embedding_for_hash(&hash_embedding_content("text/plain", b"alpha").into_bytes())
            .unwrap()
            .unwrap();
        assert_eq!(embedding, expected_embedding);
    }

    #[test]
    fn externalized_replay_prefetch_keeps_active_batch_embeddings_published_until_handoff() {
        let temp = tempdir().unwrap();
        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();

        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        let alpha_embedding = vec![0, 0, 0, 0, 0, 0, 128, 63];
        let beta_embedding = vec![0, 0, 0, 64, 0, 0, 128, 63];
        let gamma_embedding = vec![0, 0, 128, 64, 0, 0, 128, 63];
        let make_block = |path: &str, body: &[u8], embedding: Vec<u8>| {
            Block::Leaf(
                build_leaf_block(
                    VERSION_1,
                    embedding_spec.clone(),
                    vec![LeafEntry {
                        embedding,
                        metadata: vec![
                            (
                                Value::Text("source_kind".into()),
                                Value::Text("document".into()),
                            ),
                            (Value::Text("source_path".into()), Value::Text(path.into())),
                        ],
                        content: Content {
                            media_type: "text/plain".into(),
                            body: body.to_vec(),
                        },
                    }],
                    None,
                )
                .unwrap(),
            )
        };
        let alpha_block = make_block("C:/docs/alpha.txt", b"alpha", alpha_embedding.clone());
        let beta_block = make_block("C:/docs/beta.txt", b"beta", beta_embedding.clone());
        let gamma_block = make_block("C:/docs/gamma.txt", b"gamma", gamma_embedding.clone());
        let alpha_block_id = put_block(&block_store, &alpha_block);
        let beta_block_id = put_block(&block_store, &beta_block);
        let gamma_block_id = put_block(&block_store, &gamma_block);
        let alpha_validated = get_block(&block_store, &alpha_block_id).unwrap();
        let beta_validated = get_block(&block_store, &beta_block_id).unwrap();
        let gamma_validated = get_block(&block_store, &gamma_block_id).unwrap();
        let alpha_item = replay_item_from_validated_block(&alpha_validated, &embedding_spec)
            .unwrap()
            .unwrap()
            .0;
        let beta_item = replay_item_from_validated_block(&beta_validated, &embedding_spec)
            .unwrap()
            .unwrap()
            .0;
        let gamma_item = replay_item_from_validated_block(&gamma_validated, &embedding_spec)
            .unwrap()
            .unwrap()
            .0;
        let replay_order = replay_order_storage_for_entries(&[
            ReplayOrderEntry::new(alpha_block_id, replay_sort_key_digest(&alpha_item)),
            ReplayOrderEntry::new(beta_block_id, replay_sort_key_digest(&beta_item)),
            ReplayOrderEntry::new(gamma_block_id, replay_sort_key_digest(&gamma_item)),
        ]);
        let current_batch_embeddings = Arc::new(Mutex::new(HashMap::new()));
        let mut iterator = ExternalizedReplayBatchIterator {
            replay_order_reader: replay_order.open_reader().unwrap(),
            batch_size: 1,
            materialization_max_concurrency: 1,
            block_store: block_store.clone(),
            embedding_spec: embedding_spec.clone(),
            current_batch_embeddings: Arc::clone(&current_batch_embeddings),
        };
        let provider = ExternalizedStoredLeafEmbeddingProvider {
            block_store,
            embedding_spec,
            replay_order,
            current_batch_embeddings: Arc::clone(&current_batch_embeddings),
            fallback_embeddings: Arc::new(Mutex::new(None)),
        };

        let first_batch = iterator.load_next_batch().unwrap().unwrap();
        iterator.publish_batch_embeddings(&first_batch.embeddings_by_input_hash);
        assert_eq!(
            provider
                .load_embedding_for_hash(
                    &hash_embedding_content("text/plain", b"alpha").into_bytes()
                )
                .unwrap()
                .unwrap(),
            alpha_embedding
        );

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (next_iterator, mut prefetched_batches) = runtime
            .block_on(async {
                spawn_externalized_replay_batch_prefetches(
                    iterator,
                    EXTERNALIZED_REPLAY_PREFETCH_FUTURE_BATCHES,
                )
                .await
                .unwrap()
            })
            .unwrap();
        iterator = next_iterator;
        assert_eq!(
            prefetched_batches.len(),
            EXTERNALIZED_REPLAY_PREFETCH_FUTURE_BATCHES
        );
        assert_eq!(
            provider
                .load_embedding_for_hash(
                    &hash_embedding_content("text/plain", b"alpha").into_bytes()
                )
                .unwrap()
                .unwrap(),
            alpha_embedding
        );
        assert!(
            !lock_unpoisoned(&current_batch_embeddings)
                .contains_key(&hash_embedding_content("text/plain", b"beta").into_bytes())
        );
        assert!(
            !lock_unpoisoned(&current_batch_embeddings)
                .contains_key(&hash_embedding_content("text/plain", b"gamma").into_bytes())
        );

        let second_batch = prefetched_batches.pop_front().unwrap();
        iterator.publish_batch_embeddings(&second_batch.embeddings_by_input_hash);
        assert_eq!(
            provider
                .load_embedding_for_hash(
                    &hash_embedding_content("text/plain", b"beta").into_bytes()
                )
                .unwrap()
                .unwrap(),
            beta_embedding
        );
        assert!(
            !lock_unpoisoned(&current_batch_embeddings)
                .contains_key(&hash_embedding_content("text/plain", b"gamma").into_bytes())
        );

        let third_batch = prefetched_batches.pop_front().unwrap();
        iterator.publish_batch_embeddings(&third_batch.embeddings_by_input_hash);
        assert_eq!(
            provider
                .load_embedding_for_hash(
                    &hash_embedding_content("text/plain", b"gamma").into_bytes()
                )
                .unwrap()
                .unwrap(),
            gamma_embedding
        );
    }

    #[test]
    fn externalized_replay_ready_batch_does_not_wait_for_top_up() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut iterator = None;
            let mut prefetched_batches = VecDeque::from([ReplayBatchLoad {
                batch: ReplayBatch {
                    items: Vec::new(),
                    audit_records: Vec::new(),
                    completion_message: None,
                },
                embeddings_by_input_hash: Vec::new(),
            }]);
            let mut pending_prefetch: Option<ExternalizedReplayBatchPrefetchHandle> =
                Some(tokio::spawn(async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    unreachable!()
                }));

            let next_batch = tokio::time::timeout(
                Duration::from_millis(50),
                take_next_externalized_replay_batch(
                    &mut iterator,
                    &mut prefetched_batches,
                    &mut pending_prefetch,
                ),
            )
            .await
            .expect("ready batch should not wait for top-up prefetch")
            .unwrap()
            .expect("expected queued replay batch");

            assert!(next_batch.batch.items.is_empty());
            assert!(pending_prefetch.is_some());
            pending_prefetch.take().unwrap().abort();
        });
    }

    #[test]
    fn parallel_replay_batch_materialization_preserves_entry_order() {
        let temp = tempdir().unwrap();
        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();

        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        let make_block = |path: &str, body: &[u8], embedding: Vec<u8>| {
            Block::Leaf(
                build_leaf_block(
                    VERSION_1,
                    embedding_spec.clone(),
                    vec![LeafEntry {
                        embedding,
                        metadata: vec![
                            (
                                Value::Text("source_kind".into()),
                                Value::Text("document".into()),
                            ),
                            (Value::Text("source_path".into()), Value::Text(path.into())),
                        ],
                        content: Content {
                            media_type: "text/plain".into(),
                            body: body.to_vec(),
                        },
                    }],
                    None,
                )
                .unwrap(),
            )
        };
        let alpha_block = make_block(
            "C:/docs/alpha.txt",
            b"alpha",
            vec![0, 0, 0, 0, 0, 0, 128, 63],
        );
        let beta_block = make_block(
            "C:/docs/beta.txt",
            b"beta",
            vec![0, 0, 0, 64, 0, 0, 128, 63],
        );
        let gamma_block = make_block(
            "C:/docs/gamma.txt",
            b"gamma",
            vec![0, 0, 64, 64, 0, 0, 128, 63],
        );
        let alpha_block_id = put_block(&block_store, &alpha_block);
        let beta_block_id = put_block(&block_store, &beta_block);
        let gamma_block_id = put_block(&block_store, &gamma_block);
        let alpha_item = replay_item_from_validated_block(
            &get_block(&block_store, &alpha_block_id).unwrap(),
            &embedding_spec,
        )
        .unwrap()
        .unwrap()
        .0;
        let beta_item = replay_item_from_validated_block(
            &get_block(&block_store, &beta_block_id).unwrap(),
            &embedding_spec,
        )
        .unwrap()
        .unwrap()
        .0;
        let gamma_item = replay_item_from_validated_block(
            &get_block(&block_store, &gamma_block_id).unwrap(),
            &embedding_spec,
        )
        .unwrap()
        .unwrap()
        .0;
        let replay_order = replay_order_storage_for_entries(&[
            ReplayOrderEntry::new(beta_block_id, replay_sort_key_digest(&beta_item)),
            ReplayOrderEntry::new(alpha_block_id, replay_sort_key_digest(&alpha_item)),
            ReplayOrderEntry::new(gamma_block_id, replay_sort_key_digest(&gamma_item)),
        ]);
        let mut replay_order_reader = replay_order.open_reader().unwrap();
        let loaded_entries = load_replay_batch_entries_in_parallel(
            &replay_order_reader.read_next_entries(3).unwrap(),
            &block_store,
            &embedding_spec,
            2,
        )
        .unwrap();
        let observed_paths = loaded_entries
            .iter()
            .map(|entry| match &entry.item.content_ref {
                ContentRef::StoredReplay {
                    identity: ReplayIdentity::Document { source_path },
                    ..
                } => source_path.clone(),
                other => panic!("expected stored replay document, got {other:?}"),
            })
            .collect::<Vec<_>>();

        assert_eq!(
            observed_paths,
            vec![
                "C:/docs/beta.txt".to_string(),
                "C:/docs/alpha.txt".to_string(),
                "C:/docs/gamma.txt".to_string()
            ]
        );
    }

    #[test]
    fn externalized_replay_provider_caches_small_fallback_scan_results() {
        let temp = tempdir().unwrap();
        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let expected_alpha_embedding = vec![0, 0, 0, 0, 0, 0, 128, 63];
        let expected_beta_embedding = vec![0, 0, 0, 64, 0, 0, 128, 63];
        let alpha_block = Block::Leaf(
            build_leaf_block(
                VERSION_1,
                EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                vec![LeafEntry {
                    embedding: expected_alpha_embedding.clone(),
                    metadata: vec![
                        (
                            Value::Text("source_kind".into()),
                            Value::Text("document".into()),
                        ),
                        (
                            Value::Text("source_path".into()),
                            Value::Text("C:/docs/alpha.txt".into()),
                        ),
                    ],
                    content: Content {
                        media_type: "text/plain".into(),
                        body: b"alpha".to_vec(),
                    },
                }],
                None,
            )
            .unwrap(),
        );
        let beta_block = Block::Leaf(
            build_leaf_block(
                VERSION_1,
                EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                vec![LeafEntry {
                    embedding: expected_beta_embedding.clone(),
                    metadata: vec![
                        (
                            Value::Text("source_kind".into()),
                            Value::Text("document".into()),
                        ),
                        (
                            Value::Text("source_path".into()),
                            Value::Text("C:/docs/beta.txt".into()),
                        ),
                    ],
                    content: Content {
                        media_type: "text/plain".into(),
                        body: b"beta".to_vec(),
                    },
                }],
                None,
            )
            .unwrap(),
        );
        let alpha_block_id = put_block(&block_store, &alpha_block);
        let beta_block_id = put_block(&block_store, &beta_block);
        let provider = ExternalizedStoredLeafEmbeddingProvider {
            block_store,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            replay_order: replay_order_storage_for_block_ids(&[alpha_block_id, beta_block_id]),
            current_batch_embeddings: Arc::new(Mutex::new(HashMap::new())),
            fallback_embeddings: Arc::new(Mutex::new(None)),
        };
        let alpha_input_hash = hash_embedding_content("text/plain", b"alpha").into_bytes();
        let beta_input_hash = hash_embedding_content("text/plain", b"beta").into_bytes();

        let first = provider
            .load_embedding_for_hash(&alpha_input_hash)
            .unwrap()
            .unwrap();
        assert_eq!(first, expected_alpha_embedding);

        let block_store_root = temp.path().join("blocks");
        fs::remove_dir_all(&block_store_root).unwrap();

        let second = provider
            .load_embedding_for_hash(&beta_input_hash)
            .unwrap()
            .unwrap();
        assert_eq!(second, expected_beta_embedding);
    }

    #[test]
    fn externalized_replay_provider_skips_unrelated_missing_blocks_during_small_fallback_scans() {
        let temp = tempdir().unwrap();
        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let expected_embedding = vec![0, 0, 0, 0, 0, 0, 128, 63];
        let block = Block::Leaf(
            build_leaf_block(
                VERSION_1,
                EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                vec![LeafEntry {
                    embedding: expected_embedding.clone(),
                    metadata: vec![
                        (
                            Value::Text("source_kind".into()),
                            Value::Text("document".into()),
                        ),
                        (
                            Value::Text("source_path".into()),
                            Value::Text("C:/docs/alpha.txt".into()),
                        ),
                    ],
                    content: Content {
                        media_type: "text/plain".into(),
                        body: b"alpha".to_vec(),
                    },
                }],
                None,
            )
            .unwrap(),
        );
        let block_id = put_block(&block_store, &block);
        let provider = ExternalizedStoredLeafEmbeddingProvider {
            block_store,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            replay_order: replay_order_storage_for_block_ids(&[
                BlockHash::from_bytes([9u8; 32]),
                block_id,
            ]),
            current_batch_embeddings: Arc::new(Mutex::new(HashMap::new())),
            fallback_embeddings: Arc::new(Mutex::new(None)),
        };

        let embedding = provider
            .load_embedding_for_hash(&hash_embedding_content("text/plain", b"alpha").into_bytes())
            .unwrap()
            .unwrap();
        assert_eq!(embedding, expected_embedding);
    }

    #[test]
    fn externalized_replay_provider_skips_full_corpus_fallback_scans_for_large_replays() {
        let temp = tempdir().unwrap();
        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let mut ordered_block_ids =
            Vec::with_capacity(EXTERNALIZED_CLUSTERING_DIAGNOSTIC_INPUT_LIMIT + 1);
        for index in 0..=EXTERNALIZED_CLUSTERING_DIAGNOSTIC_INPUT_LIMIT {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
            ordered_block_ids.push(BlockHash::from_bytes(bytes));
        }
        let provider = ExternalizedStoredLeafEmbeddingProvider {
            block_store,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            replay_order: replay_order_storage_for_block_ids(&ordered_block_ids),
            current_batch_embeddings: Arc::new(Mutex::new(HashMap::new())),
            fallback_embeddings: Arc::new(Mutex::new(None)),
        };

        let result = provider
            .load_embedding_for_hash(&hash_embedding_content("text/plain", b"missing").into_bytes())
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn clustering_only_rejects_journal_record_mismatch() {
        let temp = tempdir().unwrap();
        let document_path = temp.path().join("alpha.txt");
        fs::write(&document_path, "alpha\n").unwrap();

        let server = spawn_embedding_server(1);
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: server.base_url.clone(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
            embedding_spec: EmbeddingSpecConfig {
                dims: 2,
                encoding: "f32le".into(),
            },
            block_size_target: 65_536,
            stage: ExecutionStage::IngestionAndEmbedding,
            profile_version: PUBLISHED_PROFILE_V0_1_0,
            max_concurrency: Some(1),
            replay_batch_size: None,
            ref_name: TEST_REF_NAME.into(),
            items: vec![BatchItemConfig::Document {
                path: document_path
                    .strip_prefix(temp.path())
                    .unwrap()
                    .to_path_buf(),
                metadata: BTreeMap::new(),
            }],
        };
        run_request(temp.path(), request).await.unwrap();

        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let block_store_root = temp.path().join("blocks");
        let mutable_ref_store = local_mutable_ref_store_location(&block_store_root, TEST_REF_NAME);
        let mut record = load_replay_journal_records(&block_store, &mutable_ref_store)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        if let ReplayJournalRecord::ReplayInput { content_ref, .. } = &mut record {
            *content_ref = ReplayJournalContentRef::Document {
                path: "bogus.txt".into(),
            };
        }
        let forged_head = store_replay_journal_block(&block_store, None, vec![record]).unwrap();
        let mut refs = load_mutable_ref_store(&mutable_ref_store).unwrap();
        refs.replay_journal_head_block_id = Some(forged_head.to_string());
        write_mutable_ref_store(&mutable_ref_store, &refs).unwrap();
        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        let progress: ProgressReporter = Arc::new(|_| {});
        let io = RuntimeIo {
            mutable_ref_store: Some(&mutable_ref_store),
            mutable_ref_metadata: None,
            planning_telemetry: None,
            progress: &progress,
        };
        let error =
            load_replay_batches_from_store(&block_store, &embedding_spec, 1, 1, io).unwrap_err();
        assert!(matches!(
            error,
            RuntimeError::InvalidReplayJournalHead { .. }
        ));
        server.join();
    }

    #[test]
    fn journal_replay_uses_sorted_unique_block_ids_for_duplicate_input_identity() {
        let temp = tempdir().unwrap();
        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let block_store_root = temp.path().join("blocks");
        let mutable_ref_store = local_mutable_ref_store_location(&block_store_root, TEST_REF_NAME);
        prepare_mutable_ref_store(&mutable_ref_store).unwrap();

        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        let metadata = vec![
            (
                Value::Text("source_kind".into()),
                Value::Text("document".into()),
            ),
            (
                Value::Text("source_path".into()),
                Value::Text("C:/shared/duplicate.txt".into()),
            ),
        ];
        let block_one = Block::Leaf(
            build_leaf_block(
                VERSION_1,
                embedding_spec.clone(),
                vec![LeafEntry {
                    embedding: vec![0, 0, 0, 0, 0, 0, 128, 63],
                    metadata: metadata.clone(),
                    content: Content {
                        media_type: "text/plain".into(),
                        body: b"first body".to_vec(),
                    },
                }],
                None,
            )
            .unwrap(),
        );
        let block_two = Block::Leaf(
            build_leaf_block(
                VERSION_1,
                embedding_spec.clone(),
                vec![LeafEntry {
                    embedding: vec![0, 0, 0, 64, 0, 0, 128, 63],
                    metadata: metadata.clone(),
                    content: Content {
                        media_type: "text/plain".into(),
                        body: b"second body".to_vec(),
                    },
                }],
                None,
            )
            .unwrap(),
        );
        let block_one_id = put_block(&block_store, &block_one);
        let block_two_id = put_block(&block_store, &block_two);
        let validated_one = get_block(&block_store, &block_one_id).unwrap();
        let validated_two = get_block(&block_store, &block_two_id).unwrap();
        let (item_one, _) = replay_item_from_validated_block(&validated_one, &embedding_spec)
            .unwrap()
            .unwrap();
        let (item_two, _) = replay_item_from_validated_block(&validated_two, &embedding_spec)
            .unwrap()
            .unwrap();
        let records = [
            replay_journal_record_from_item(block_one_id, &item_one),
            replay_journal_record_from_item(block_two_id, &item_two),
        ];
        let replay_journal_head_block_id =
            append_replay_journal_records(&block_store, &mutable_ref_store, &[records[0].clone()])
                .unwrap();
        update_mutable_ref_store(
            &mutable_ref_store,
            MutableRefStoreUpdate {
                replay_journal_head_block_id,
                ..MutableRefStoreUpdate::default()
            },
        )
        .unwrap();
        let replay_journal_head_block_id =
            append_replay_journal_records(&block_store, &mutable_ref_store, &[records[1].clone()])
                .unwrap();
        update_mutable_ref_store(
            &mutable_ref_store,
            MutableRefStoreUpdate {
                replay_journal_head_block_id,
                ..MutableRefStoreUpdate::default()
            },
        )
        .unwrap();

        let progress: ProgressReporter = Arc::new(|_| {});
        let io = RuntimeIo {
            mutable_ref_store: Some(&mutable_ref_store),
            mutable_ref_metadata: None,
            planning_telemetry: None,
            progress: &progress,
        };
        let (replay_batches, _) =
            load_replay_batches_from_store(&block_store, &embedding_spec, 8, 8, io).unwrap();

        let replay_item_count: usize = replay_batches.iter().map(|batch| batch.items.len()).sum();
        assert_eq!(replay_item_count, 2);
        let replay_block_ids = replay_batches
            .iter()
            .flat_map(|batch| batch.audit_records.iter())
            .map(|record| match record {
                ReplayJournalRecord::ReplayInput { block_id, .. } => block_id.clone(),
                ReplayJournalRecord::IndexingOutcome { .. } => {
                    unreachable!("replay batches only contain replay inputs")
                }
            })
            .collect::<Vec<_>>();
        let mut expected_block_ids = vec![block_one_id.to_string(), block_two_id.to_string()];
        expected_block_ids.sort();
        assert_eq!(replay_block_ids, expected_block_ids);
    }

    #[test]
    fn azure_mutable_ref_store_round_trips_json_state() {
        let server = spawn_ref_blob_server(3);
        let ref_path = format!("{MUTABLE_REF_ROOT_DIR}/{TEST_REF_NAME}");
        let mutable_ref_store = MutableRefStoreLocation::AzureBlob {
            url: format!("{}/archive-sync/{}?sig=test", server.base_url, ref_path),
            display_path: format!("{}/archive-sync/{}", server.base_url, ref_path),
        };

        assert_eq!(
            load_mutable_ref_store(&mutable_ref_store).unwrap(),
            MutableRefStoreState::default()
        );
        let state = MutableRefStoreState {
            current_root_block_id: Some(BlockHash::from_bytes([1u8; 32]).to_string()),
            replay_journal_head_block_id: Some(BlockHash::from_bytes([2u8; 32]).to_string()),
            metadata: Some(BTreeMap::from([("stage".into(), "full-pipeline".into())])),
        };
        write_mutable_ref_store(&mutable_ref_store, &state).unwrap();
        assert_eq!(load_mutable_ref_store(&mutable_ref_store).unwrap(), state);
        server.join();
    }

    #[tokio::test]
    async fn clustering_only_run_advances_replay_journal_head() {
        let temp = tempdir().unwrap();
        let document_a = temp.path().join("alpha.txt");
        let document_b = temp.path().join("beta.txt");
        fs::write(&document_a, b"alpha\n").unwrap();
        fs::write(&document_b, b"beta\n").unwrap();

        let server = spawn_embedding_server(2);
        let full_summary = run_request(
            temp.path(),
            BatchRequest {
                environment: EnvironmentConfig::Local {
                    block_store_root: Path::new("blocks").to_path_buf(),
                    embedding: LocalEmbeddingConfig {
                        base_url: server.base_url.clone(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 0,
                        retry_delay_ms: 1,
                    },
                },
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                block_size_target: 65_536,
                stage: ExecutionStage::FullPipeline,
                profile_version: PUBLISHED_PROFILE_V0_1_0,
                max_concurrency: Some(2),
                replay_batch_size: None,
                ref_name: TEST_REF_NAME.into(),
                items: vec![
                    BatchItemConfig::Document {
                        path: document_a.strip_prefix(temp.path()).unwrap().to_path_buf(),
                        metadata: BTreeMap::new(),
                    },
                    BatchItemConfig::Document {
                        path: document_b.strip_prefix(temp.path()).unwrap().to_path_buf(),
                        metadata: BTreeMap::new(),
                    },
                ],
            },
        )
        .await
        .unwrap();

        let block_store_root = temp.path().join("blocks");
        let mutable_ref_store = local_mutable_ref_store_location(&block_store_root, TEST_REF_NAME);
        let refs_after_full = load_mutable_ref_store(&mutable_ref_store).unwrap();

        let clustering_summary = run_request(
            temp.path(),
            BatchRequest {
                environment: EnvironmentConfig::Local {
                    block_store_root: Path::new("blocks").to_path_buf(),
                    embedding: LocalEmbeddingConfig {
                        base_url: String::new(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 0,
                        retry_delay_ms: 1,
                    },
                },
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                block_size_target: 65_536,
                stage: ExecutionStage::ClusteringAndBlockAssembly,
                profile_version: PUBLISHED_PROFILE_V0_1_0,
                max_concurrency: Some(2),
                replay_batch_size: None,
                ref_name: TEST_REF_NAME.into(),
                items: vec![],
            },
        )
        .await
        .unwrap();

        let refs_after_clustering = load_mutable_ref_store(&mutable_ref_store).unwrap();
        assert_eq!(clustering_summary.root_id, full_summary.root_id);
        assert_ne!(
            refs_after_clustering.replay_journal_head_block_id,
            refs_after_full.replay_journal_head_block_id
        );

        let block_store = ConfiguredBlockStore::from_environment(
            temp.path(),
            &EnvironmentConfig::Local {
                block_store_root: Path::new("blocks").to_path_buf(),
                embedding: LocalEmbeddingConfig {
                    base_url: String::new(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 5,
                    max_retries: 0,
                    retry_delay_ms: 1,
                },
            },
        )
        .unwrap();
        let records = load_replay_journal_records(&block_store, &mutable_ref_store).unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    record,
                    ReplayJournalRecord::IndexingOutcome {
                        step_kind: ReplayJournalStepKind::Indexing,
                        root_block_id,
                        ..
                    } if root_block_id == &full_summary.root_id
                ))
                .count(),
            2
        );
        server.join();
    }

    #[tokio::test]
    async fn current_root_ref_is_published_and_ingestion_only_does_not_rewrite_it() {
        let temp = tempdir().unwrap();
        let document_a = temp.path().join("alpha.txt");
        let document_b = temp.path().join("beta.txt");
        let document_c = temp.path().join("gamma.txt");
        fs::write(&document_a, b"alpha\n").unwrap();
        fs::write(&document_b, b"beta\n").unwrap();
        fs::write(&document_c, b"gamma\n").unwrap();

        let server = spawn_embedding_server(3);
        let full_summary = run_request(
            temp.path(),
            BatchRequest {
                environment: EnvironmentConfig::Local {
                    block_store_root: Path::new("blocks").to_path_buf(),
                    embedding: LocalEmbeddingConfig {
                        base_url: server.base_url.clone(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 0,
                        retry_delay_ms: 1,
                    },
                },
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                block_size_target: 65_536,
                stage: ExecutionStage::FullPipeline,
                profile_version: PUBLISHED_PROFILE_V0_1_0,
                max_concurrency: Some(2),
                replay_batch_size: None,
                ref_name: TEST_REF_NAME.into(),
                items: vec![
                    BatchItemConfig::Document {
                        path: document_a.strip_prefix(temp.path()).unwrap().to_path_buf(),
                        metadata: BTreeMap::new(),
                    },
                    BatchItemConfig::Document {
                        path: document_b.strip_prefix(temp.path()).unwrap().to_path_buf(),
                        metadata: BTreeMap::new(),
                    },
                ],
            },
        )
        .await
        .unwrap();

        let block_store_root = temp.path().join("blocks");
        let mutable_ref_store = local_mutable_ref_store_location(&block_store_root, TEST_REF_NAME);
        let refs_after_full = load_mutable_ref_store(&mutable_ref_store).unwrap();
        assert_eq!(
            refs_after_full.current_root_block_id.as_deref(),
            Some(full_summary.root_id.as_str())
        );
        assert!(refs_after_full.replay_journal_head_block_id.is_some());

        let ingestion_only_summary = run_request(
            temp.path(),
            BatchRequest {
                environment: EnvironmentConfig::Local {
                    block_store_root: Path::new("blocks").to_path_buf(),
                    embedding: LocalEmbeddingConfig {
                        base_url: server.base_url.clone(),
                        model: "all-MiniLM-L6-v2".into(),
                        api_key_env: None,
                        request_timeout_secs: 5,
                        max_retries: 0,
                        retry_delay_ms: 1,
                    },
                },
                embedding_spec: EmbeddingSpecConfig {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                block_size_target: 65_536,
                stage: ExecutionStage::IngestionAndEmbedding,
                profile_version: PUBLISHED_PROFILE_V0_1_0,
                max_concurrency: Some(1),
                replay_batch_size: None,
                ref_name: TEST_REF_NAME.into(),
                items: vec![BatchItemConfig::Document {
                    path: document_c.strip_prefix(temp.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                }],
            },
        )
        .await
        .unwrap();
        assert_eq!(ingestion_only_summary.root_id, placeholder_root_id());

        let refs_after_ingestion = load_mutable_ref_store(&mutable_ref_store).unwrap();
        assert_eq!(
            refs_after_ingestion.current_root_block_id.as_deref(),
            Some(full_summary.root_id.as_str())
        );
        assert!(refs_after_ingestion.replay_journal_head_block_id.is_some());
        server.join();
    }

    struct TestServer {
        base_url: String,
        handle: Option<thread::JoinHandle<()>>,
        max_in_flight: Arc<AtomicUsize>,
    }

    struct RefBlobServer {
        base_url: String,
        handle: thread::JoinHandle<()>,
    }

    impl TestServer {
        fn join(mut self) {
            self.handle.take().unwrap().join().unwrap();
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(Ordering::SeqCst)
        }
    }

    impl RefBlobServer {
        fn join(self) {
            self.handle.join().unwrap();
        }
    }

    struct InFlightGuard {
        counter: Arc<AtomicUsize>,
    }

    type EmbeddingResponseBuilder = Arc<dyn Fn(&[u8]) -> String + Send + Sync + 'static>;

    impl Drop for InFlightGuard {
        fn drop(&mut self) {
            self.counter.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn request_is_complete(request: &[u8]) -> bool {
        let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            return false;
        };
        let body_start = header_end + 4;
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        });

        match content_length {
            Some(length) => request.len() >= body_start + length,
            None => true,
        }
    }

    fn count_files_recursively(root: &Path) -> usize {
        fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .map(|path| {
                if path.is_dir() {
                    count_files_recursively(&path)
                } else {
                    1
                }
            })
            .sum()
    }

    fn spawn_embedding_server(expected_requests: usize) -> TestServer {
        spawn_embedding_server_with_delay(expected_requests, Duration::ZERO)
    }

    fn spawn_ref_blob_server(expected_requests: usize) -> RefBlobServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let state = Arc::new(std::sync::Mutex::new(None::<Vec<u8>>));
        let state_for_thread = Arc::clone(&state);
        let handle = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            for _ in 0..expected_requests {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                reader.read_line(&mut request_line).unwrap();
                assert!(request_line.contains(&format!("{MUTABLE_REF_ROOT_DIR}/{TEST_REF_NAME}")));
                let method = request_line
                    .split_whitespace()
                    .next()
                    .expect("request line includes method")
                    .to_string();
                let mut content_length = 0usize;
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    if header == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = header.split_once(':')
                        && name.eq_ignore_ascii_case("content-length")
                    {
                        content_length = value.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0u8; content_length];
                reader.read_exact(&mut body).unwrap();
                let (status, response_body) = match method.as_str() {
                    "GET" => match state_for_thread.lock().unwrap().clone() {
                        Some(body) => ("200 OK", body),
                        None => ("404 Not Found", Vec::new()),
                    },
                    "PUT" => {
                        *state_for_thread.lock().unwrap() = Some(body);
                        ("201 Created", Vec::new())
                    }
                    other => panic!("unexpected ref-blob method {other}"),
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response_body.len()
                );
                stream.write_all(response.as_bytes()).unwrap();
                if !response_body.is_empty() {
                    stream.write_all(&response_body).unwrap();
                }
                stream.flush().unwrap();
            }
        });
        ready_rx.recv().unwrap();
        RefBlobServer {
            base_url: format!("http://{}", address),
            handle,
        }
    }

    fn spawn_distinct_embedding_server(expected_requests: usize) -> TestServer {
        spawn_embedding_server_with_delay_and_responder(
            expected_requests,
            Duration::ZERO,
            Arc::new(|request| {
                let request = String::from_utf8_lossy(request);
                if request.contains("alpha") {
                    r#"{"data":[{"embedding":[1.0,0.0]}]}"#.to_string()
                } else if request.contains("beta") {
                    r#"{"data":[{"embedding":[0.0,1.0]}]}"#.to_string()
                } else if request.contains("gamma") {
                    r#"{"data":[{"embedding":[1.0,1.0]}]}"#.to_string()
                } else {
                    r#"{"data":[{"embedding":[0.25,0.75]}]}"#.to_string()
                }
            }),
        )
    }

    fn spawn_embedding_server_with_delay(
        expected_requests: usize,
        response_delay: Duration,
    ) -> TestServer {
        spawn_embedding_server_with_delay_and_responder(
            expected_requests,
            response_delay,
            Arc::new(|_| r#"{"data":[{"embedding":[0.25,0.75]}]}"#.to_string()),
        )
    }

    fn spawn_embedding_server_with_delay_and_responder(
        expected_requests: usize,
        response_delay: Duration,
        responder: EmbeddingResponseBuilder,
    ) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(AtomicUsize::new(0));
        let seen_for_thread = Arc::clone(&seen);
        let current_in_flight = Arc::new(AtomicUsize::new(0));
        let current_in_flight_for_thread = Arc::clone(&current_in_flight);
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight_for_thread = Arc::clone(&max_in_flight);
        let (ready_tx, ready_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let idle_after_expected = Duration::from_millis(200);
            let deadline = Instant::now() + Duration::from_secs(60);
            let mut last_activity = Instant::now();
            loop {
                if Instant::now() >= deadline {
                    panic!("timed out waiting for runtime test server termination");
                }
                let no_in_flight = current_in_flight_for_thread.load(Ordering::SeqCst) == 0;
                if no_in_flight
                    && seen_for_thread.load(Ordering::SeqCst) >= expected_requests
                    && Instant::now().duration_since(last_activity) >= idle_after_expected
                {
                    break;
                }
                let (mut stream, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    Err(error) => panic!("failed to accept runtime test connection: {error}"),
                };
                last_activity = Instant::now();
                let seen_for_connection = Arc::clone(&seen_for_thread);
                let current_in_flight_for_connection = Arc::clone(&current_in_flight_for_thread);
                let max_in_flight_for_connection = Arc::clone(&max_in_flight_for_thread);
                let responder_for_connection = Arc::clone(&responder);
                thread::spawn(move || {
                    let current =
                        current_in_flight_for_connection.fetch_add(1, Ordering::SeqCst) + 1;
                    let _in_flight_guard = InFlightGuard {
                        counter: Arc::clone(&current_in_flight_for_connection),
                    };
                    loop {
                        let previous_max = max_in_flight_for_connection.load(Ordering::SeqCst);
                        if current <= previous_max {
                            break;
                        }
                        if max_in_flight_for_connection
                            .compare_exchange(
                                previous_max,
                                current,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok()
                        {
                            break;
                        }
                    }

                    stream.set_nonblocking(true).unwrap();
                    let mut request = Vec::new();
                    let mut buffer = [0u8; 1024];
                    let request_deadline = Instant::now() + Duration::from_secs(5);
                    loop {
                        if request_is_complete(&request) {
                            break;
                        }
                        if Instant::now() >= request_deadline {
                            panic!("timed out waiting for runtime test request body");
                        }
                        match stream.read(&mut buffer) {
                            Ok(0) => break,
                            Ok(read) => {
                                request.extend_from_slice(&buffer[..read]);
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                thread::sleep(Duration::from_millis(10));
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                                continue;
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => break,
                            Err(error) => panic!("failed to read runtime test request: {error}"),
                        }
                    }
                    stream.set_nonblocking(false).unwrap();
                    if !response_delay.is_zero() {
                        thread::sleep(response_delay);
                    }
                    let body = responder_for_connection(&request);
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).unwrap();
                    stream.flush().unwrap();
                    seen_for_connection.fetch_add(1, Ordering::SeqCst);
                });
            }
        });
        ready_rx.recv().unwrap();

        TestServer {
            base_url: format!("http://{}", address),
            handle: Some(handle),
            max_in_flight,
        }
    }
}
