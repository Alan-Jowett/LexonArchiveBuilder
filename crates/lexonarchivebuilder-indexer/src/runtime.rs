use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use lexongraph_block::{
    Block, BlockError, BlockHash, EmbeddingSpec, LeafEntry, SerializedBlock, VERSION_1,
    build_leaf_block, deserialize_block, serialize_block,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_streaming_indexer::{
    ContentResolver, IndexItem, PlanningStage, PublishedIndexingProfile, StreamingIndexerError,
    StreamingIndexingPhase, StreamingIndexingRun, StreamingIndexingStatus,
    StreamingIndexingStatusObserver, StreamingIndexingStatusState, published_indexing_profile,
};
use serde::Serialize;
use thiserror::Error;
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Instant as TokioInstant, MissedTickBehavior, interval_at};

use crate::block_store::ConfiguredBlockStore;
use crate::config::{
    BatchItemConfig, BatchRequest, BatchSummary, ClusteringConfigOverrides, ConfigError,
    ConfiguredClustering, ExecutionStage, metadata_to_text_map,
};
use crate::embedding::{ConfiguredEmbeddingProvider, ConfiguredEmbeddingProviderError};
use crate::mailbox::{MailboxExpansionError, expand_mailbox_item_with_stats};
use crate::paths::resolve_path;
use crate::resolver::{
    ContentRef, LocalFilesystemContentResolver, LocalFilesystemContentResolverError,
};
use crate::tree_tools::decode_embedding_values;

type ProgressReporter = Arc<dyn Fn(String) + Send + Sync + 'static>;

pub const INGESTION_ONLY_ROOT_ID_PLACEHOLDER: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
const PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

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
    completion_message: Option<String>,
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
    leaf_algorithm_id: String,
    packing_strategy_id: String,
    hierarchy_strategy_id: String,
    summary_policy_id: String,
    leaf_cluster_count: u32,
    leaf_random_seed: Option<u64>,
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

#[derive(Clone, Debug)]
struct StreamingStageConfig {
    stage: ExecutionStage,
    clustering: ConfiguredClustering,
    block_size_target: usize,
    submission_progress_kind: SubmissionProgressKind,
}

type ReplayedLeaf = (IndexItem<ContentRef>, Vec<u8>);

#[derive(Clone, Debug)]
struct StoredLeafEmbeddingProvider {
    embeddings_by_input_hash: Arc<HashMap<[u8; 32], Vec<u8>>>,
}

#[derive(Clone, Debug)]
struct RecordingEmbeddingProvider<EP> {
    inner: EP,
    embeddings_by_input_hash: Arc<Mutex<HashMap<[u8; 32], Vec<u8>>>>,
}

#[derive(Debug, Error)]
enum StoredLeafEmbeddingProviderError {
    #[error("no stored embedding was available for the requested replay input")]
    MissingStoredEmbedding,
}

#[cfg(test)]
#[derive(Debug, Error)]
enum AutoSizingBuiltInPlanningError {
    #[error("{0}")]
    DeriveClusterCount(String),
}

trait ClusteringFailureEmbeddingSource {
    fn embedding_for_hash(&self, input_hash: &[u8; 32]) -> Option<Vec<u8>>;
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

impl ClusteringFailureEmbeddingSource for StoredLeafEmbeddingProvider {
    fn embedding_for_hash(&self, input_hash: &[u8; 32]) -> Option<Vec<u8>> {
        self.embeddings_by_input_hash.get(input_hash).cloned()
    }
}

impl<EP> RecordingEmbeddingProvider<EP> {
    fn new(inner: EP) -> Self {
        Self {
            inner,
            embeddings_by_input_hash: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<EP> ClusteringFailureEmbeddingSource for RecordingEmbeddingProvider<EP> {
    fn embedding_for_hash(&self, input_hash: &[u8; 32]) -> Option<Vec<u8>> {
        lock_unpoisoned(&self.embeddings_by_input_hash)
            .get(input_hash)
            .cloned()
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
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
    published_indexing_profile(clustering.profile_version)
}

fn clustering_failure_input(item: &IndexItem<ContentRef>) -> ClusteringFailureInput {
    match &item.content_ref {
        ContentRef::Document { path } => {
            let source_path = path.to_string_lossy().replace('\\', "/");
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
        leaf_algorithm_id: profile.leaf_algorithm_id.to_string(),
        packing_strategy_id: profile.packing_strategy_id.to_string(),
        hierarchy_strategy_id: profile.hierarchy_strategy_id.to_string(),
        summary_policy_id: profile.summary_policy_id.to_string(),
        leaf_cluster_count: profile.leaf_cluster_count,
        leaf_random_seed: profile.leaf_random_seed,
    })
}

const SUSPICIOUS_INPUT_SAMPLE_LIMIT: usize = 5;
const VARIANCE_EPSILON: f64 = 1e-12;

#[derive(Clone, Debug, Default)]
struct EmbeddingObservation {
    fingerprint: Option<String>,
    l2_norm: Option<f64>,
    content_fingerprint: Option<String>,
    missing: bool,
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
        let Some(embedding_bytes) = embedding_source.embedding_for_hash(&input_hash.into_bytes())
        else {
            missing_embedding_count += 1;
            observations.push(EmbeddingObservation {
                content_fingerprint,
                missing: true,
                ..EmbeddingObservation::default()
            });
            continue;
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
    for (input, observation) in inputs.iter().zip(observations.iter()) {
        let mut reasons = Vec::new();
        if observation.missing {
            reasons.push("missing-embedding".to_string());
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

    run_request_with_progress(
        request_dir,
        request,
        clustering_overrides,
        Some(diagnostics_path.as_path()),
        |message| {
            eprintln!("{message}");
        },
    )
    .await
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
        None,
        |message| eprintln!("{message}"),
    )
    .await
}

async fn run_request_with_progress<F>(
    request_dir: &Path,
    request: BatchRequest,
    clustering_overrides: ClusteringConfigOverrides,
    diagnostics_path: Option<&Path>,
    progress: F,
) -> Result<BatchSummary, RuntimeError>
where
    F: Fn(String) + Send + Sync + 'static,
{
    let progress: ProgressReporter = Arc::new(progress);
    request.validate()?;
    let clustering = clustering_overrides.to_configured_clustering()?;
    let stage = request.stage;
    let block_store = ConfiguredBlockStore::from_environment(request_dir, &request.environment)?;
    let embedding_spec = request.to_embedding_spec();
    let resolver = LocalFilesystemContentResolver::new(block_store.clone());
    let max_concurrency = request.effective_max_concurrency();

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
            &progress,
        )
        .await;
    }

    let result = if stage.includes_ingestion() {
        let replay_batches = prepare_request_replay_batches(
            request_dir,
            &request,
            &block_store,
            max_concurrency,
            &progress,
        )?;
        request.environment.local_embedding()?;
        let embedding_provider = RecordingEmbeddingProvider::new(
            ConfiguredEmbeddingProvider::from_environment(&request.environment)?,
        );
        run_streaming_stage(
            resolver,
            embedding_provider,
            StreamingStageConfig {
                stage,
                clustering,
                block_size_target: request.block_size_target,
                submission_progress_kind: SubmissionProgressKind::Embedding,
            },
            replay_batches,
            &block_store,
            &embedding_spec,
            &progress,
        )
        .await
    } else {
        let (replay_batches, embedding_provider) = load_replay_batches_from_store(
            &block_store,
            &embedding_spec,
            max_concurrency,
            &progress,
        )?;
        run_streaming_stage(
            resolver,
            embedding_provider,
            StreamingStageConfig {
                stage,
                clustering,
                block_size_target: request.block_size_target,
                submission_progress_kind: SubmissionProgressKind::Replay,
            },
            replay_batches,
            &block_store,
            &embedding_spec,
            &progress,
        )
        .await
    };

    if let Err(error) = &result {
        persist_clustering_failure_diagnostics(diagnostics_path, error, &progress);
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
    progress: &ProgressReporter,
) -> Result<BatchSummary, RuntimeError> {
    let mut staged = StagedBlocks::default();
    let total_batches = replay_batches.len();
    let total_items: usize = replay_batches.iter().map(|batch| batch.items.len()).sum();
    let mut completed_items = 0usize;
    for (batch_index, batch) in replay_batches.into_iter().enumerate() {
        let batch_number = batch_index + 1;
        let batch_item_count = batch.items.len();
        report_progress(
            progress,
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
            progress,
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
        completed_items += batch_item_count;
        if let Some(message) = batch.completion_message {
            report_progress(
                progress,
                format!("{message} into {} leaf block(s)", constructed.blocks.len()),
            );
        }
        staged.extend_constructed(&constructed);
    }
    report_progress(
        progress,
        format!(
            "Skipped clustering and block assembly; returning placeholder root_id {}",
            placeholder_root_id()
        ),
    );
    Ok(staged.into_summary(placeholder_root_id()))
}

fn prepare_request_replay_batches(
    request_dir: &Path,
    request: &BatchRequest,
    block_store: &ConfiguredBlockStore,
    max_concurrency: usize,
    progress: &ProgressReporter,
) -> Result<Vec<ReplayBatch>, RuntimeError> {
    let mut items = Vec::new();

    let document_items = request.to_document_index_items(request_dir);
    if !document_items.is_empty() {
        let document_item_count = document_items.len();
        report_progress(
            progress,
            format!(
                "Preparing {} document item(s) with up to {} concurrent leaf worker(s)",
                document_item_count, max_concurrency
            ),
        );
        report_progress(
            progress,
            format!("Prepared {} document item(s)", document_item_count),
        );
        items.extend(document_items);
    }

    for item in &request.items {
        if let BatchItemConfig::Mailbox { path, metadata } = item {
            let resolved = resolve_path(request_dir, path);
            report_progress(
                progress,
                format!("Processing mailbox {}", resolved.display()),
            );
            let expansion = expand_mailbox_item_with_stats(&resolved, metadata, block_store)?;
            report_progress(
                progress,
                format!(
                    "Processed mailbox {}: {} message(s), {} delegated item(s)",
                    resolved.display(),
                    expansion.message_count,
                    expansion.items.len()
                ),
            );
            report_progress(
                progress,
                format!(
                    "Prepared {} delegated item(s) from mailbox {}",
                    expansion.items.len(),
                    resolved.display()
                ),
            );
            items.extend(expansion.items);
        }
    }

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

fn replay_sort_key(item: &IndexItem<ContentRef>) -> (String, Vec<(String, String)>) {
    let content_key = match &item.content_ref {
        ContentRef::Document { path } => format!("document:{}", path.to_string_lossy()),
        ContentRef::Inline { media_type, body } => {
            format!("inline:{media_type}:{:?}", body)
        }
        ContentRef::EmailChunk {
            email_artifact_ref,
            chunk_index,
        } => format!("email:{email_artifact_ref}:{chunk_index:020}"),
    };
    let metadata_key = metadata_to_text_map(&item.metadata).into_iter().collect();
    (content_key, metadata_key)
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
        let serialized =
            serialize_block(&block).map_err(|source| RuntimeError::SerializeIteratedBlock {
                block_id: "<leaf>".into(),
                source,
            })?;
        constructed.block_ids.push(serialized.hash);
        constructed.blocks.push(serialized);
    }
    Ok(constructed)
}

async fn run_streaming_stage<EP>(
    resolver: LocalFilesystemContentResolver,
    embedding_provider: EP,
    config: StreamingStageConfig,
    replay_batches: Vec<ReplayBatch>,
    block_store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
    progress: &ProgressReporter,
) -> Result<BatchSummary, RuntimeError>
where
    EP: EmbeddingProvider + ClusteringFailureEmbeddingSource + Clone,
{
    let latest_failed_status = Arc::new(Mutex::new(None));
    let observer = Some(make_status_observer(
        Arc::clone(progress),
        Arc::clone(&latest_failed_status),
    ));
    let total_batches = replay_batches.len();
    let total_items: usize = replay_batches.iter().map(|batch| batch.items.len()).sum();
    let clustering_failure_diagnostics = OnceLock::new();
    let diagnostics_resolver = resolver.clone();
    let diagnostics_embedding_provider = embedding_provider.clone();

    let mut indexer = StreamingIndexingRun::with_published_profile(
        resolver,
        embedding_provider,
        config.clustering.profile_version,
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
            progress,
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
            progress,
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
            report_progress(progress, message.clone());
        }
    }
    report_progress(
        progress,
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
            progress,
        )
    })?;
    report_progress(
        progress,
        format!(
            "Completed planning pass {} over {} item(s)",
            pass_report.completed_pass_count, pass_report.observed_item_count
        ),
    );
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
            progress,
        )
    })?;
    report_progress(
        progress,
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
                progress,
            )
        })?;

    if result.block_ids.is_empty() {
        return Err(RuntimeError::EmptyDelegatedOutput);
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

fn load_replay_batches_from_store(
    store: &ConfiguredBlockStore,
    embedding_spec: &EmbeddingSpec,
    max_concurrency: usize,
    progress: &ProgressReporter,
) -> Result<(Vec<ReplayBatch>, StoredLeafEmbeddingProvider), RuntimeError> {
    report_progress(
        progress,
        "Scanning the configured block store for clustering-eligible leaf blocks".to_string(),
    );

    let mut items = Vec::new();
    let mut embeddings_by_input_hash = HashMap::new();
    for block_id in store.iter_block_ids()? {
        let block_id = block_id?;
        let Some(validated) = store.get(&block_id)? else {
            return Err(RuntimeError::MissingIteratedBlock {
                block_id: block_id.to_string(),
            });
        };
        let Some((item, embedding)) = replay_item_from_validated_block(&validated, embedding_spec)?
        else {
            continue;
        };
        let content = match &validated.block {
            Block::Leaf(block) => &block.entries[0].content,
            Block::Branch(_) => unreachable!("filtered above"),
        };
        let key = hash_embedding_input(&EmbeddingInput {
            media_type: content.media_type.clone(),
            body: content.body.clone(),
        })
        .into_bytes();
        embeddings_by_input_hash.insert(key, embedding);
        items.push(item);
    }

    if items.is_empty() {
        return Err(RuntimeError::NoClusterableBlocks);
    }

    sort_replay_items(&mut items);

    report_progress(
        progress,
        format!(
            "Loaded {} replay item(s) from clustering-eligible leaf blocks in the configured block store",
            items.len()
        ),
    );
    let mut replay_batches = chunk_replay_items(items, max_concurrency);
    annotate_submission_progress_batches(&mut replay_batches, SubmissionProgressKind::Replay);
    Ok((
        replay_batches,
        StoredLeafEmbeddingProvider {
            embeddings_by_input_hash: Arc::new(embeddings_by_input_hash),
        },
    ))
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
            ContentRef::Document {
                path: source_path.into(),
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
            ContentRef::EmailChunk {
                email_artifact_ref: email_artifact_ref.clone(),
                chunk_index,
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
) -> StreamingIndexingStatusObserver {
    Arc::new(move |status| {
        if status.state == StreamingIndexingStatusState::Failed {
            let mut captured = lock_unpoisoned(&latest_failed_status);
            match captured.as_ref() {
                Some(existing) if !prefer_failed_status(&status, existing) => {}
                _ => *captured = Some(status.clone()),
            }
        }
        report_progress(&progress, format_indexing_status(status));
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

fn format_indexing_status(status: StreamingIndexingStatus) -> String {
    let elapsed_ms = status.elapsed.as_millis();
    match (status.phase, status.state) {
        (
            StreamingIndexingPhase::PlanningPass { pass_number },
            StreamingIndexingStatusState::Started,
        ) => format!(
            "Planning pass {pass_number} started for {} item(s)",
            status.item_count
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
                "Planning pass {pass_number} still running after {elapsed_ms} ms for {} item(s){}",
                status.item_count, progress_suffix
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
                "Planning pass {pass_number} completed in {elapsed_ms} ms for {} item(s){}",
                status.item_count, progress_suffix
            )
        }
        (
            StreamingIndexingPhase::PlanningPass { pass_number },
            StreamingIndexingStatusState::Failed,
        ) => format!(
            "Planning pass {pass_number} failed after {elapsed_ms} ms: {}",
            status.error.unwrap_or_else(|| "unknown error".into())
        ),
        (
            StreamingIndexingPhase::HierarchyPlanning { stage },
            StreamingIndexingStatusState::Started,
        ) => {
            format!(
                "{} started for {} item(s)",
                format_planning_stage(stage),
                status.item_count,
            )
        }
        (
            StreamingIndexingPhase::HierarchyPlanning { stage },
            StreamingIndexingStatusState::InProgress,
        ) => {
            format!(
                "{} still running after {elapsed_ms} ms; processed {} stage-local item(s)",
                format_planning_stage(stage),
                status.completed_unit_count,
            )
        }
        (
            StreamingIndexingPhase::HierarchyPlanning { stage },
            StreamingIndexingStatusState::Completed,
        ) => {
            format!(
                "{} completed in {elapsed_ms} ms after processing {} stage-local item(s)",
                format_planning_stage(stage),
                status.completed_unit_count,
            )
        }
        (
            StreamingIndexingPhase::HierarchyPlanning { stage },
            StreamingIndexingStatusState::Failed,
        ) => {
            format!(
                "{} failed after {elapsed_ms} ms; processed {} stage-local item(s): {}",
                format_planning_stage(stage),
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
        let persisted = store.put(&validated.block)?;
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

fn parent_directory_to_create(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
}

pub fn clustering_failure_diagnostics_path(
    request_path: &Path,
    summary_out: Option<&Path>,
) -> PathBuf {
    let anchor_path = summary_out.unwrap_or(request_path);
    let base_name = anchor_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(|stem| format!("{stem}.clustering-failure-diagnostics.json"))
        .unwrap_or_else(|| "clustering-failure-diagnostics.json".to_string());
    adjacent_output_directory(anchor_path).join(base_name)
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
    use std::io::{Read, Write};
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
    use serde_json::json;
    use tempfile::tempdir;

    use crate::config::{
        BatchItemConfig, ClusteringConfigOverrides, EmbeddingSpecConfig, EnvironmentConfig,
        ExecutionStage, LocalEmbeddingConfig,
    };

    use super::*;

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
            max_concurrency: None,
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

        assert_eq!(first.root_id, second.root_id);
        assert_eq!(first.block_ids, second.block_ids);
        assert_eq!(
            stored_block_count_after_first,
            stored_block_count_after_second
        );
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
            max_concurrency: None,
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
            max_concurrency: None,
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
            None,
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
                .any(|line| line.contains("Embedding batch 1 of "))
        );
        assert!(progress.iter().any(|line| {
            line.contains("Embedded batch") && line.contains("completed 2 of 2 delegated item(s)")
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
            line.contains("embedding batch(es); waiting for planning pass completion")
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
            max_concurrency: Some(2),
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
            max_concurrency: Some(2),
            items: vec![],
        };

        let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_capture = Arc::clone(&progress);
        let _summary = run_request_with_progress(
            temp.path(),
            cluster_only_request,
            ClusteringConfigOverrides::default(),
            None,
            move |message| {
                progress_capture.lock().unwrap().push(message);
            },
        )
        .await
        .unwrap();

        let progress = progress.lock().unwrap();
        assert!(progress.iter().any(|line| {
            line.contains("Submitting replay batch 1 of 3")
                && line.contains("completed 0 of 5 delegated item(s)")
        }));
        assert!(progress.iter().any(|line| {
            line.contains("Submitted replay batch 1 of 3")
                && line.contains("completed 2 of 5 delegated item(s)")
        }));
        assert!(progress.iter().any(|line| {
            line.contains("Submitted replay batch 3 of 3")
                && line.contains("completed 5 of 5 delegated item(s)")
        }));
        assert!(progress.iter().any(|line| {
            line.contains("Submitted all 3 replay batch(es); waiting for planning pass completion")
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
            max_concurrency: None,
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

    fn seed_non_finite_leaf_blocks(root: &Path, names: &[&str]) {
        let store =
            ConfiguredBlockStore::from_environment(root, &local_test_environment(String::new()))
                .unwrap();
        let embedding_spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };

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
            store.put(&Block::Leaf(block)).unwrap();
        }
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
            None,
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
        assert_eq!(diagnostics.clustering.leaf_algorithm_id, "spherical-kmeans");
        assert_eq!(
            diagnostics.clustering.packing_strategy_id,
            "cluster-order-balanced-range-packer-v1"
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
        assert!(written.contains("\"leaf_algorithm_id\": \"spherical-kmeans\""));
        assert!(written.contains("\"embedding_health\""));
        assert!(written.contains("\"failing_subset\""));
        assert!(written.contains("\"provenance\": \"exact\""));
        assert!(written.contains("\"non-finite-embedding\""));
        assert!(written.contains("alpha.txt"));
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
        let occupied = temp.path().join("occupied");
        fs::write(&occupied, b"not a directory").unwrap();
        let summary_out = occupied.join("summary.json");
        let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_capture = Arc::clone(&progress);

        let bytes = fs::read(&request_path).unwrap();
        let request: BatchRequest = serde_json::from_slice(&bytes).unwrap();
        let diagnostics_path =
            clustering_failure_diagnostics_path(&request_path, Some(summary_out.as_path()));
        let error = run_request_with_progress(
            temp.path(),
            request,
            ClusteringConfigOverrides::default(),
            Some(diagnostics_path.as_path()),
            move |message| {
                progress_capture.lock().unwrap().push(message);
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(error, RuntimeError::ClusteringFailure { .. }));
        let progress = progress.lock().unwrap();
        assert!(progress.iter().any(|line| {
            line.contains("Failed to write clustering failure diagnostics to")
                && line.contains("summary.clustering-failure-diagnostics.json")
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
            max_concurrency: None,
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
            max_concurrency: None,
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
            max_concurrency: None,
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
                max_concurrency: None,
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
        assert!(written.contains("\"leaf_algorithm_id\": \"spherical-kmeans\""));
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
                leaf_algorithm_id: "spherical-kmeans".into(),
                packing_strategy_id: "cluster-order-balanced-range-packer-v1".into(),
                hierarchy_strategy_id: "greedy-pack".into(),
                summary_policy_id: "exact-centroid".into(),
                leaf_cluster_count: 157,
                leaf_random_seed: Some(11),
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
            &StreamingIndexingStatus {
                phase: StreamingIndexingPhase::HierarchyPlanning {
                    stage: PlanningStage::Single,
                },
                state: StreamingIndexingStatusState::Failed,
                item_count: 3,
                phase_total_unit_count: Some(3),
                completed_unit_count: 0,
                remaining_unit_count: Some(3),
                elapsed: Duration::from_secs(1),
                error: Some("boom".into()),
            },
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
            &StreamingIndexingStatus {
                phase: StreamingIndexingPhase::HierarchyPlanning {
                    stage: PlanningStage::Single,
                },
                state: StreamingIndexingStatusState::Failed,
                item_count: 1,
                phase_total_unit_count: Some(1),
                completed_unit_count: 0,
                remaining_unit_count: Some(1),
                elapsed: Duration::from_secs(1),
                error: Some("boom".into()),
            },
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
            .to_configured_clustering()
            .expect("published profile config");
        let diagnostics =
            effective_clustering_diagnostics(&clustering).expect("published profile diagnostics");

        assert_eq!(diagnostics.profile_version, "0.1.0");
        assert_eq!(diagnostics.leaf_algorithm_id, "spherical-kmeans");
        assert_eq!(
            diagnostics.packing_strategy_id,
            "cluster-order-balanced-range-packer-v1"
        );
        assert_eq!(diagnostics.hierarchy_strategy_id, "greedy-pack");
        assert_eq!(diagnostics.summary_policy_id, "exact-centroid");
        assert_eq!(diagnostics.leaf_cluster_count, 157);
        assert_eq!(diagnostics.leaf_random_seed, Some(11));
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
        let serialized = serialize_block(&Block::Branch(branch)).unwrap();

        assert_eq!(
            serialized_branch_size(&embedding_spec, entry_count).unwrap(),
            serialized.bytes.len()
        );
    }

    #[test]
    fn hierarchy_planning_progress_reports_stage_local_counts() {
        let status = StreamingIndexingStatus {
            phase: StreamingIndexingPhase::HierarchyPlanning {
                stage: PlanningStage::Custom,
            },
            state: StreamingIndexingStatusState::InProgress,
            item_count: 7,
            phase_total_unit_count: None,
            completed_unit_count: 7,
            remaining_unit_count: None,
            elapsed: Duration::from_millis(125),
            error: None,
        };

        assert_eq!(
            format_indexing_status(status),
            "custom planning still running after 125 ms; processed 7 stage-local item(s)"
        );
    }

    #[test]
    fn final_materialization_progress_reports_replay_totals_when_available() {
        let status = StreamingIndexingStatus {
            phase: StreamingIndexingPhase::FinalMaterializationReplay,
            state: StreamingIndexingStatusState::InProgress,
            item_count: 9,
            phase_total_unit_count: Some(9),
            completed_unit_count: 4,
            remaining_unit_count: Some(5),
            elapsed: Duration::from_millis(250),
            error: None,
        };

        assert_eq!(
            format_indexing_status(status),
            "Final materialization replay still running after 250 ms for 9 item(s); completed 4 of 9 replay item(s)"
        );
    }

    #[test]
    fn bottom_up_assembly_progress_distinguishes_input_blocks_from_groups() {
        let status = StreamingIndexingStatus {
            phase: StreamingIndexingPhase::BottomUpAssembly { layer_index: 2 },
            state: StreamingIndexingStatusState::Completed,
            item_count: 12,
            phase_total_unit_count: Some(3),
            completed_unit_count: 3,
            remaining_unit_count: Some(0),
            elapsed: Duration::from_millis(88),
            error: None,
        };

        assert_eq!(
            format_indexing_status(status),
            "Bottom-up assembly for layer 2 completed in 88 ms: built 3 of 3 group(s) from 12 input block(s)"
        );
    }

    #[test]
    fn bottom_up_assembly_progress_handles_unknown_group_total() {
        let status = StreamingIndexingStatus {
            phase: StreamingIndexingPhase::BottomUpAssembly { layer_index: 1 },
            state: StreamingIndexingStatusState::InProgress,
            item_count: 8,
            phase_total_unit_count: None,
            completed_unit_count: 2,
            remaining_unit_count: None,
            elapsed: Duration::from_millis(44),
            error: None,
        };

        assert_eq!(
            format_indexing_status(status),
            "Bottom-up assembly for layer 1 still running after 44 ms; completed 2 group(s) so far from 8 input block(s)"
        );
    }

    #[test]
    fn bottom_up_assembly_started_message_omits_elapsed_clause() {
        let status = StreamingIndexingStatus {
            phase: StreamingIndexingPhase::BottomUpAssembly { layer_index: 2 },
            state: StreamingIndexingStatusState::Started,
            item_count: 12,
            phase_total_unit_count: Some(3),
            completed_unit_count: 0,
            remaining_unit_count: Some(3),
            elapsed: Duration::from_millis(0),
            error: None,
        };

        assert_eq!(
            format_indexing_status(status),
            "Bottom-up assembly for layer 2 started for 12 input block(s) across 3 group(s)"
        );
    }

    #[test]
    fn bottom_up_assembly_started_message_handles_unknown_group_total() {
        let status = StreamingIndexingStatus {
            phase: StreamingIndexingPhase::BottomUpAssembly { layer_index: 1 },
            state: StreamingIndexingStatusState::Started,
            item_count: 8,
            phase_total_unit_count: None,
            completed_unit_count: 0,
            remaining_unit_count: None,
            elapsed: Duration::from_millis(0),
            error: None,
        };

        assert_eq!(
            format_indexing_status(status),
            "Bottom-up assembly for layer 1 started for 8 input block(s) across an unknown group total"
        );
    }

    #[test]
    fn hierarchy_planning_failure_uses_single_temporal_clause() {
        let status = StreamingIndexingStatus {
            phase: StreamingIndexingPhase::HierarchyPlanning {
                stage: PlanningStage::Custom,
            },
            state: StreamingIndexingStatusState::Failed,
            item_count: 7,
            phase_total_unit_count: None,
            completed_unit_count: 3,
            remaining_unit_count: None,
            elapsed: Duration::from_millis(125),
            error: Some("boom".into()),
        };

        assert_eq!(
            format_indexing_status(status),
            "custom planning failed after 125 ms; processed 3 stage-local item(s): boom"
        );
    }

    #[test]
    fn bottom_up_assembly_failure_uses_single_temporal_clause() {
        let status = StreamingIndexingStatus {
            phase: StreamingIndexingPhase::BottomUpAssembly { layer_index: 2 },
            state: StreamingIndexingStatusState::Failed,
            item_count: 12,
            phase_total_unit_count: Some(3),
            completed_unit_count: 2,
            remaining_unit_count: Some(1),
            elapsed: Duration::from_millis(88),
            error: Some("boom".into()),
        };

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
            max_concurrency: Some(1),
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
            max_concurrency: Some(1),
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
            max_concurrency: Some(3),
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
            max_concurrency: Some(3),
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
            max_concurrency: Some(3),
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
            max_concurrency: Some(2),
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
            max_concurrency: None,
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
            max_concurrency: Some(2),
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
        let (replay_batches, _) =
            load_replay_batches_from_store(&block_store, &embedding_spec, 2, &progress).unwrap();

        assert_eq!(replay_batches.len(), 3);
        assert_eq!(replay_batches[0].items.len(), 2);
        assert_eq!(replay_batches[1].items.len(), 2);
        assert_eq!(replay_batches[2].items.len(), 1);
        server.join();
    }

    struct TestServer {
        base_url: String,
        handle: thread::JoinHandle<()>,
        max_in_flight: Arc<AtomicUsize>,
    }

    impl TestServer {
        fn join(self) {
            self.handle.join().unwrap();
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(Ordering::SeqCst)
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
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut last_activity = Instant::now();
            while Instant::now() < deadline {
                if seen_for_thread.load(Ordering::SeqCst) >= expected_requests
                    && current_in_flight_for_thread.load(Ordering::SeqCst) == 0
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
            handle,
            max_in_flight,
        }
    }
}
