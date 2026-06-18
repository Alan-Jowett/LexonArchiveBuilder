use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use half::f16;
use lexongraph_block::{Block, BlockHash, BranchEntry, EmbeddingSpec, LeafBlock, LeafEntry};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_search::{
    CandidateScorer, CosineScore, DefaultCandidateScorer, DefaultEmbeddingCompatibility,
    EncodedTargetEmbedding, Searcher,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::search::default_traversal_width as default_search_traversal_width;
use crate::tree_tools::{
    decode_embedding_values, metadata_values_to_text_map, search_with_partial_retry,
};

const DEFAULT_QUANTILE_BIN_COUNT: usize = 4;
const DEFAULT_TNN_RECALL_SAMPLE_SIZE: usize = 100;
const DEFAULT_TNN_RECALL_SEED: u64 = 0;
const REQUIRED_RECALL_AT: [usize; 3] = [1, 5, 10];
const POWER_ITERATION_STEPS: usize = 8;
const EPSILON: f32 = 1.0e-6;

#[derive(Debug, Error)]
pub enum TreeQualityError {
    #[error("root block {root_id} was not found")]
    MissingRootBlock { root_id: String },
    #[error("block {block_id} uses unsupported embedding spec {encoding}/{dims}")]
    UnsupportedEmbeddingSpec {
        block_id: String,
        encoding: String,
        dims: u64,
    },
    #[error(
        "block {block_id} embedding payload length {actual_bytes} does not match expected length {expected_bytes} for {encoding}/{dims}"
    )]
    InvalidEmbeddingLength {
        block_id: String,
        encoding: String,
        dims: u64,
        expected_bytes: usize,
        actual_bytes: usize,
    },
    #[error("block {block_id} contains a non-finite embedding value")]
    NonFiniteEmbedding { block_id: String },
    #[error("tnn recall sample_size must be at least 1")]
    InvalidTnnRecallSampleSize,
    #[error("tnn recall traversal_width must be at least 1")]
    InvalidTnnRecallTraversalWidth,
    #[error(
        "block {block_id} contains a zero-magnitude embedding that cannot be scored for tnn recall"
    )]
    ZeroMagnitudeEmbedding { block_id: String },
    #[error("tnn recall search failed: {message}")]
    Search { message: String },
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error("failed to render tree quality report")]
    Render(#[from] serde_json::Error),
    #[error("failed to write tree quality report {path}: {source}")]
    WriteArtifact {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum FindingSeverity {
    Error,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum FindingKind {
    MissingChildBlock,
    ChildLevelNotLowerThanParent,
    EmbeddingSpecMismatch,
    CycleDetected,
    SharedChildReference,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct SpreadMetrics {
    #[serde(skip_serializing)]
    pub centroid: Vec<f32>,
    pub mean_centroid_distance: f32,
    pub max_centroid_distance: f32,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct QuantileOccupancyMetrics {
    pub bin_count: usize,
    pub occupancies: Vec<usize>,
    pub occupancy_variance: f32,
    pub empty_bin_count: usize,
    pub overfull_bin_count: usize,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct EmbeddingSpecReport {
    pub dims: u64,
    pub encoding: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct BlockQualityMetrics {
    pub block_id: String,
    pub kind: String,
    pub level: u64,
    pub entry_count: usize,
    pub parent_block_id: Option<String>,
    pub reachable_depth: usize,
    pub embedding_spec: EmbeddingSpecReport,
    pub spread: SpreadMetrics,
    pub pca_first_component_variance_fraction: f32,
    pub quantile_occupancy: QuantileOccupancyMetrics,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TreeQualityFinding {
    pub severity: FindingSeverity,
    pub kind: FindingKind,
    pub block_id: String,
    pub parent_block_id: Option<String>,
    pub message: String,
    pub parent_mean_centroid_distance: Option<f32>,
    pub child_mean_centroid_distance: Option<f32>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct LayerQualityMetrics {
    pub level: u64,
    pub block_count: usize,
    pub mean_intra_block_dispersion: f32,
    pub stdev_intra_block_dispersion: f32,
    pub mean_sibling_centroid_distance: f32,
    pub stdev_sibling_centroid_distance: f32,
    pub mean_pca_axis_strength: f32,
    pub stdev_pca_axis_strength: f32,
    pub mean_quantile_occupancy_variance: f32,
    pub stdev_quantile_occupancy_variance: f32,
    pub blocks_with_empty_bins: usize,
    pub blocks_with_overfull_bins: usize,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct SplitEffectivenessMetrics {
    pub parent_block_id: String,
    pub parent_level: u64,
    pub child_count: usize,
    pub child_dispersion_exceeds_parent_count: usize,
    pub child_dispersion_exceeds_parent_percentage: f32,
    pub mean_dispersion_increase_for_exceeding_children: f32,
    pub max_dispersion_increase_for_exceeding_children: f32,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TreeQualitySummary {
    pub block_count: usize,
    pub branch_count: usize,
    pub leaf_count: usize,
    pub edge_count: usize,
    pub max_depth: usize,
    pub structural_finding_count: usize,
    pub child_dispersion_inversion_count: usize,
    pub parent_split_count: usize,
    pub mean_block_mean_centroid_distance: f32,
    pub max_block_max_centroid_distance: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TnnRecallConfig {
    pub sample_size: usize,
    pub seed: u64,
    pub traversal_width: usize,
}

impl Default for TnnRecallConfig {
    fn default() -> Self {
        Self {
            sample_size: DEFAULT_TNN_RECALL_SAMPLE_SIZE,
            seed: DEFAULT_TNN_RECALL_SEED,
            traversal_width: default_search_traversal_width(),
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TnnRecallHistogramBin {
    pub matched_neighbor_count: usize,
    pub recall: f32,
    pub sample_count: usize,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TnnRecallAtMetrics {
    pub k: usize,
    pub mean_recall: f32,
    pub stdev_recall: f32,
    pub histogram: Vec<TnnRecallHistogramBin>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct CorpusTnnRecallReport {
    pub query_source: String,
    pub corpus_size: usize,
    pub requested_sample_size: usize,
    pub effective_sample_size: usize,
    pub seed: u64,
    pub traversal_width: usize,
    pub recall_at: Vec<TnnRecallAtMetrics>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TnnMatchedNeighborCount {
    pub k: usize,
    pub matched_neighbor_count: usize,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TnnOraclePathRound {
    pub round_index: usize,
    pub parent_block_id: String,
    pub parent_level: u64,
    pub child_block_id: String,
    pub child_level: u64,
    pub local_sibling_count: usize,
    pub local_sibling_rank: usize,
    pub frontier_branch_rank: usize,
    pub selected_for_expansion: bool,
    pub local_routing_score: f32,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TnnBlockingPointSummary {
    pub blocked_child_level: u64,
    pub blocked_parent_level: u64,
    pub query_count: usize,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TnnFalsificationSummary {
    pub sample_count: usize,
    pub approximate_top1_hit_count: usize,
    pub approximate_top1_failure_count: usize,
    pub oracle_leaf_visited_failure_count: usize,
    pub oracle_leaf_unvisited_failure_count: usize,
    pub visited_leaf_oracle_top1_hit_count: usize,
    pub visited_leaf_oracle_top1_repaired_failure_count: usize,
    pub first_blocking_points: Vec<TnnBlockingPointSummary>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TnnFalsificationQueryReport {
    pub query_neighbor_id: String,
    pub oracle_neighbor_id: String,
    pub oracle_leaf_block_id: String,
    pub approximate_top1_hit: bool,
    pub oracle_leaf_visited: bool,
    pub deepest_visited_oracle_block_id: String,
    pub deepest_visited_oracle_level: u64,
    pub first_blocked_round_index: Option<usize>,
    pub first_blocked_child_level: Option<u64>,
    pub first_blocked_parent_level: Option<u64>,
    pub approximate_matches_at: Vec<TnnMatchedNeighborCount>,
    pub visited_leaf_oracle_matches_at: Vec<TnnMatchedNeighborCount>,
    pub oracle_path_rounds: Vec<TnnOraclePathRound>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TnnFalsificationReport {
    pub root_id: String,
    pub corpus_size: usize,
    pub requested_sample_size: usize,
    pub effective_sample_size: usize,
    pub seed: u64,
    pub traversal_width: usize,
    pub approximate_recall_at: Vec<TnnRecallAtMetrics>,
    pub visited_leaf_oracle_recall_at: Vec<TnnRecallAtMetrics>,
    pub summary: TnnFalsificationSummary,
    pub queries: Vec<TnnFalsificationQueryReport>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TreeQualityReport {
    pub root_id: String,
    pub summary: TreeQualitySummary,
    pub corpus_tnn_recall: CorpusTnnRecallReport,
    pub findings: Vec<TreeQualityFinding>,
    pub layers: Vec<LayerQualityMetrics>,
    pub splits: Vec<SplitEffectivenessMetrics>,
    pub blocks: Vec<BlockQualityMetrics>,
}

#[derive(Clone, Debug)]
struct TraversalState {
    blocks: Vec<BlockQualityMetrics>,
    corpus_entries: Vec<CorpusLeafEntry>,
    findings: Vec<TreeQualityFinding>,
    metrics_by_id: HashMap<BlockHash, BlockQualityMetrics>,
    child_ids_by_parent: HashMap<BlockHash, Vec<BlockHash>>,
    parent_by_id: HashMap<BlockHash, Option<BlockHash>>,
    visited: HashSet<BlockHash>,
    has_zero_magnitude_tnn_entry: bool,
    structural_finding_count: usize,
    edge_count: usize,
    max_depth: usize,
}

#[derive(Clone, Debug)]
struct BlockComputedMetrics {
    spread: SpreadMetrics,
    pca_first_component_variance_fraction: f32,
    quantile_occupancy: QuantileOccupancyMetrics,
}

#[derive(Clone, Debug)]
struct CorpusLeafEntry {
    neighbor_id: String,
    leaf_block_id: BlockHash,
    embedding: Vec<f32>,
}

#[derive(Clone, Debug)]
struct OracleChildLocalRank {
    parent_block_id: BlockHash,
    parent_level: u64,
    child_block_id: BlockHash,
    child_level: u64,
    local_sibling_count: usize,
    local_sibling_rank: usize,
    local_routing_score: f32,
}

#[derive(Clone, Debug)]
struct OraclePathRoundTrace {
    round_index: usize,
    local_rank: OracleChildLocalRank,
    frontier_branch_rank: usize,
    selected_for_expansion: bool,
}

#[derive(Clone, Debug)]
struct SearchTraceOutcome {
    approximate_neighbors: Vec<CorpusLeafEntry>,
    visited_leaf_neighbors: Vec<CorpusLeafEntry>,
    deepest_visited_oracle_index: usize,
    oracle_leaf_visited: bool,
    oracle_path_rounds: Vec<OraclePathRoundTrace>,
}

#[derive(Clone, Copy, Debug, Default)]
struct RunningStats {
    count: usize,
    sum: f64,
    sum_squares: f64,
}

impl RunningStats {
    fn push(&mut self, value: f32) {
        let value = f64::from(value);
        self.count += 1;
        self.sum += value;
        self.sum_squares += value * value;
    }

    fn mean(self) -> f32 {
        if self.count == 0 {
            0.0
        } else {
            (self.sum / self.count as f64) as f32
        }
    }

    fn stdev(self) -> f32 {
        if self.count <= 1 {
            0.0
        } else {
            let count = self.count as f64;
            let mean = self.sum / count;
            ((self.sum_squares / count) - (mean * mean)).max(0.0).sqrt() as f32
        }
    }
}

impl TraversalState {
    fn push_finding(&mut self, finding: TreeQualityFinding) {
        self.structural_finding_count += 1;
        self.findings.push(finding);
    }
}

pub fn assess_rooted_tree(
    root_id: &BlockHash,
    store: &dyn BlockStore,
) -> Result<TreeQualityReport, TreeQualityError> {
    assess_rooted_tree_with_config(root_id, store, TnnRecallConfig::default())
}

pub fn assess_rooted_tree_with_config(
    root_id: &BlockHash,
    store: &dyn BlockStore,
    tnn_recall: TnnRecallConfig,
) -> Result<TreeQualityReport, TreeQualityError> {
    if tnn_recall.sample_size == 0 {
        return Err(TreeQualityError::InvalidTnnRecallSampleSize);
    }
    if tnn_recall.traversal_width == 0 {
        return Err(TreeQualityError::InvalidTnnRecallTraversalWidth);
    }
    let Some(root) = store.get(root_id)? else {
        return Err(TreeQualityError::MissingRootBlock {
            root_id: root_id.to_string(),
        });
    };

    let mut state = TraversalState {
        blocks: Vec::new(),
        corpus_entries: Vec::new(),
        findings: Vec::new(),
        metrics_by_id: HashMap::new(),
        child_ids_by_parent: HashMap::new(),
        parent_by_id: HashMap::new(),
        visited: HashSet::new(),
        has_zero_magnitude_tnn_entry: false,
        structural_finding_count: 0,
        edge_count: 0,
        max_depth: 0,
    };
    let mut ancestry = Vec::new();
    traverse_block(
        root.hash,
        &root.block,
        None,
        0,
        store,
        &mut ancestry,
        &mut state,
    )?;
    state
        .blocks
        .sort_by(|left, right| left.block_id.cmp(&right.block_id));
    state.findings.sort_by(|left, right| {
        left.severity
            .cmp(&right.severity)
            .then_with(|| left.block_id.cmp(&right.block_id))
            .then_with(|| left.message.cmp(&right.message))
    });

    let layers = build_layer_metrics(&state);
    let splits = build_split_metrics(&state);
    let corpus_tnn_recall = build_corpus_tnn_recall_report(
        root_id,
        &embedding_spec_for_block(&root.block),
        &state,
        store,
        tnn_recall,
    )?;
    let child_dispersion_inversion_count = splits
        .iter()
        .map(|split| split.child_dispersion_exceeds_parent_count)
        .sum();
    let block_count = state.blocks.len();
    let branch_count = state
        .blocks
        .iter()
        .filter(|block| block.kind == "branch")
        .count();
    let leaf_count = block_count - branch_count;
    let mean_block_mean_centroid_distance = mean(
        &state
            .blocks
            .iter()
            .map(|block| block.spread.mean_centroid_distance)
            .collect::<Vec<_>>(),
    );
    let max_block_max_centroid_distance = state
        .blocks
        .iter()
        .map(|block| block.spread.max_centroid_distance)
        .fold(0.0f32, f32::max);

    Ok(TreeQualityReport {
        root_id: root_id.to_string(),
        summary: TreeQualitySummary {
            block_count,
            branch_count,
            leaf_count,
            edge_count: state.edge_count,
            max_depth: state.max_depth,
            structural_finding_count: state.structural_finding_count,
            child_dispersion_inversion_count,
            parent_split_count: splits.len(),
            mean_block_mean_centroid_distance,
            max_block_max_centroid_distance,
        },
        corpus_tnn_recall,
        findings: state.findings,
        layers,
        splits,
        blocks: state.blocks,
    })
}

pub fn default_report_path(root_id: &BlockHash) -> PathBuf {
    PathBuf::from(format!(
        "block-tree-quality-{}.json",
        &root_id.to_string()[..8]
    ))
}

pub fn default_tnn_recall_sample_size() -> usize {
    DEFAULT_TNN_RECALL_SAMPLE_SIZE
}

pub fn default_tnn_recall_seed() -> u64 {
    DEFAULT_TNN_RECALL_SEED
}

pub fn assess_tnn_falsification(
    root_id: &BlockHash,
    store: &dyn BlockStore,
    tnn_recall: TnnRecallConfig,
) -> Result<TnnFalsificationReport, TreeQualityError> {
    if tnn_recall.sample_size == 0 {
        return Err(TreeQualityError::InvalidTnnRecallSampleSize);
    }
    if tnn_recall.traversal_width == 0 {
        return Err(TreeQualityError::InvalidTnnRecallTraversalWidth);
    }
    let Some(root) = store.get(root_id)? else {
        return Err(TreeQualityError::MissingRootBlock {
            root_id: root_id.to_string(),
        });
    };

    let mut state = TraversalState {
        blocks: Vec::new(),
        corpus_entries: Vec::new(),
        findings: Vec::new(),
        metrics_by_id: HashMap::new(),
        child_ids_by_parent: HashMap::new(),
        parent_by_id: HashMap::new(),
        visited: HashSet::new(),
        has_zero_magnitude_tnn_entry: false,
        structural_finding_count: 0,
        edge_count: 0,
        max_depth: 0,
    };
    let mut ancestry = Vec::new();
    traverse_block(
        root.hash,
        &root.block,
        None,
        0,
        store,
        &mut ancestry,
        &mut state,
    )?;

    let corpus_size = state.corpus_entries.len();
    let can_compute_recall = corpus_size >= 2
        && !has_embedding_spec_mismatch(&state)
        && !state.has_zero_magnitude_tnn_entry;
    let effective_sample_size = if can_compute_recall {
        tnn_recall.sample_size.min(corpus_size)
    } else {
        0
    };
    if effective_sample_size == 0 {
        return Ok(TnnFalsificationReport {
            root_id: root_id.to_string(),
            corpus_size,
            requested_sample_size: tnn_recall.sample_size,
            effective_sample_size,
            seed: tnn_recall.seed,
            traversal_width: tnn_recall.traversal_width,
            approximate_recall_at: zeroed_corpus_tnn_recall_report(
                corpus_size,
                tnn_recall.sample_size,
                effective_sample_size,
                tnn_recall.seed,
                tnn_recall.traversal_width,
            )
            .recall_at,
            visited_leaf_oracle_recall_at: zeroed_corpus_tnn_recall_report(
                corpus_size,
                tnn_recall.sample_size,
                effective_sample_size,
                tnn_recall.seed,
                tnn_recall.traversal_width,
            )
            .recall_at,
            summary: TnnFalsificationSummary {
                sample_count: 0,
                approximate_top1_hit_count: 0,
                approximate_top1_failure_count: 0,
                oracle_leaf_visited_failure_count: 0,
                oracle_leaf_unvisited_failure_count: 0,
                visited_leaf_oracle_top1_hit_count: 0,
                visited_leaf_oracle_top1_repaired_failure_count: 0,
                first_blocking_points: Vec::new(),
            },
            queries: Vec::new(),
        });
    }

    let root_embedding_spec = embedding_spec_for_block(&root.block);
    let sampled_queries = select_corpus_sample(&state.corpus_entries, tnn_recall);
    let max_k = REQUIRED_RECALL_AT
        .iter()
        .copied()
        .max()
        .unwrap_or(1)
        .min(corpus_size.saturating_sub(1));
    let mut approximate_counts = REQUIRED_RECALL_AT
        .into_iter()
        .map(|k| (k, Vec::<usize>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut visited_counts = REQUIRED_RECALL_AT
        .into_iter()
        .map(|k| (k, Vec::<usize>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut blocking_counts = BTreeMap::<(u64, u64), usize>::new();
    let mut queries = Vec::with_capacity(sampled_queries.len());
    let mut approximate_top1_hit_count = 0usize;
    let mut oracle_leaf_visited_failure_count = 0usize;
    let mut oracle_leaf_unvisited_failure_count = 0usize;
    let mut visited_leaf_oracle_top1_hit_count = 0usize;

    for query in sampled_queries {
        let exact_neighbors = exact_neighbors(&state.corpus_entries, query, max_k)?;
        let oracle_neighbor = exact_neighbors
            .first()
            .expect("effective sample size implies a non-empty exact neighbor set");
        let oracle_path = ancestry_path_for_leaf(root_id, oracle_neighbor.leaf_block_id, &state)?;
        let trace = trace_approximate_neighbors(TraceSearchRequest {
            root_id,
            root_embedding_spec: &root_embedding_spec,
            query,
            max_k,
            traversal_width: tnn_recall.traversal_width,
            store,
            oracle_path: &oracle_path,
            state: &state,
        })?;

        let approximate_match_counts =
            matched_neighbor_counts(&exact_neighbors, &trace.approximate_neighbors);
        let visited_match_counts =
            matched_neighbor_counts(&exact_neighbors, &trace.visited_leaf_neighbors);

        for count in &approximate_match_counts {
            approximate_counts
                .entry(count.k)
                .or_default()
                .push(count.matched_neighbor_count);
        }
        for count in &visited_match_counts {
            visited_counts
                .entry(count.k)
                .or_default()
                .push(count.matched_neighbor_count);
        }

        let approximate_top1_hit = approximate_match_counts
            .iter()
            .find(|count| count.k == 1)
            .map(|count| count.matched_neighbor_count == 1)
            .unwrap_or(false);
        let visited_top1_hit = visited_match_counts
            .iter()
            .find(|count| count.k == 1)
            .map(|count| count.matched_neighbor_count == 1)
            .unwrap_or(false);

        if approximate_top1_hit {
            approximate_top1_hit_count += 1;
        } else if trace.oracle_leaf_visited {
            oracle_leaf_visited_failure_count += 1;
        } else {
            oracle_leaf_unvisited_failure_count += 1;
        }
        if visited_top1_hit {
            visited_leaf_oracle_top1_hit_count += 1;
        }

        let deepest_visited_oracle_block_id = oracle_path[trace.deepest_visited_oracle_index];
        let deepest_visited_oracle_level = state
            .metrics_by_id
            .get(&deepest_visited_oracle_block_id)
            .expect("oracle path block metrics must exist")
            .level;
        let first_blocking = (!approximate_top1_hit && !trace.oracle_leaf_visited)
            .then(|| first_blocking_round(&trace, &oracle_path))
            .flatten();
        if let Some(blocking) = &first_blocking {
            *blocking_counts
                .entry((
                    blocking.local_rank.parent_level,
                    blocking.local_rank.child_level,
                ))
                .or_default() += 1;
        }

        queries.push(TnnFalsificationQueryReport {
            query_neighbor_id: query.neighbor_id.clone(),
            oracle_neighbor_id: oracle_neighbor.neighbor_id.clone(),
            oracle_leaf_block_id: oracle_neighbor.leaf_block_id.to_string(),
            approximate_top1_hit,
            oracle_leaf_visited: trace.oracle_leaf_visited,
            deepest_visited_oracle_block_id: deepest_visited_oracle_block_id.to_string(),
            deepest_visited_oracle_level,
            first_blocked_round_index: first_blocking.as_ref().map(|round| round.round_index),
            first_blocked_child_level: first_blocking
                .as_ref()
                .map(|round| round.local_rank.child_level),
            first_blocked_parent_level: first_blocking
                .as_ref()
                .map(|round| round.local_rank.parent_level),
            approximate_matches_at: approximate_match_counts,
            visited_leaf_oracle_matches_at: visited_match_counts,
            oracle_path_rounds: trace
                .oracle_path_rounds
                .into_iter()
                .map(|round| TnnOraclePathRound {
                    round_index: round.round_index,
                    parent_block_id: round.local_rank.parent_block_id.to_string(),
                    parent_level: round.local_rank.parent_level,
                    child_block_id: round.local_rank.child_block_id.to_string(),
                    child_level: round.local_rank.child_level,
                    local_sibling_count: round.local_rank.local_sibling_count,
                    local_sibling_rank: round.local_rank.local_sibling_rank,
                    frontier_branch_rank: round.frontier_branch_rank,
                    selected_for_expansion: round.selected_for_expansion,
                    local_routing_score: round.local_rank.local_routing_score,
                })
                .collect(),
        });
    }

    let approximate_recall_at = REQUIRED_RECALL_AT
        .into_iter()
        .map(|k| {
            let counts = approximate_counts.remove(&k).unwrap_or_default();
            tnn_recall_metrics(k, k.min(corpus_size.saturating_sub(1)), &counts)
        })
        .collect();
    let visited_leaf_oracle_recall_at = REQUIRED_RECALL_AT
        .into_iter()
        .map(|k| {
            let counts = visited_counts.remove(&k).unwrap_or_default();
            tnn_recall_metrics(k, k.min(corpus_size.saturating_sub(1)), &counts)
        })
        .collect();
    let approximate_top1_failure_count = queries.len().saturating_sub(approximate_top1_hit_count);
    let visited_leaf_oracle_top1_repaired_failure_count = queries
        .iter()
        .filter(|query| !query.approximate_top1_hit)
        .filter(|query| {
            query
                .visited_leaf_oracle_matches_at
                .iter()
                .find(|count| count.k == 1)
                .map(|count| count.matched_neighbor_count == 1)
                .unwrap_or(false)
        })
        .count();

    Ok(TnnFalsificationReport {
        root_id: root_id.to_string(),
        corpus_size,
        requested_sample_size: tnn_recall.sample_size,
        effective_sample_size,
        seed: tnn_recall.seed,
        traversal_width: tnn_recall.traversal_width,
        approximate_recall_at,
        visited_leaf_oracle_recall_at,
        summary: TnnFalsificationSummary {
            sample_count: queries.len(),
            approximate_top1_hit_count,
            approximate_top1_failure_count,
            oracle_leaf_visited_failure_count,
            oracle_leaf_unvisited_failure_count,
            visited_leaf_oracle_top1_hit_count,
            visited_leaf_oracle_top1_repaired_failure_count,
            first_blocking_points: blocking_counts
                .into_iter()
                .map(
                    |((blocked_parent_level, blocked_child_level), query_count)| {
                        TnnBlockingPointSummary {
                            blocked_child_level,
                            blocked_parent_level,
                            query_count,
                        }
                    },
                )
                .collect(),
        },
        queries,
    })
}

pub fn default_falsification_report_path(root_id: &BlockHash) -> PathBuf {
    PathBuf::from(format!(
        "tnn-falsification-{}.json",
        &root_id.to_string()[..8]
    ))
}

pub fn write_report(path: &Path, report: &TreeQualityReport) -> Result<(), TreeQualityError> {
    let rendered = serde_json::to_vec_pretty(report)?;
    fs::write(path, rendered).map_err(|source| TreeQualityError::WriteArtifact {
        path: path.display().to_string(),
        source,
    })
}

pub fn write_falsification_report(
    path: &Path,
    report: &TnnFalsificationReport,
) -> Result<(), TreeQualityError> {
    let rendered = serde_json::to_vec_pretty(report)?;
    fs::write(path, rendered).map_err(|source| TreeQualityError::WriteArtifact {
        path: path.display().to_string(),
        source,
    })
}

pub fn render_falsification_report_summary(report: &TnnFalsificationReport) -> String {
    let mut lines = vec![
        format!("TNN falsification report for {}", report.root_id),
        format!(
            "Corpus {} sampled {}/{} query(ies), seed {}, traversal width {}",
            report.corpus_size,
            report.effective_sample_size,
            report.requested_sample_size,
            report.seed,
            report.traversal_width
        ),
        format!(
            "Top-1 outcomes: hit {}, fail {}, oracle leaf visited on fail {}, oracle leaf unvisited on fail {}, visited-leaf oracle repaired {}",
            report.summary.approximate_top1_hit_count,
            report.summary.approximate_top1_failure_count,
            report.summary.oracle_leaf_visited_failure_count,
            report.summary.oracle_leaf_unvisited_failure_count,
            report
                .summary
                .visited_leaf_oracle_top1_repaired_failure_count
        ),
        "Approximate TNN recall:".into(),
    ];
    for recall_at in &report.approximate_recall_at {
        lines.push(format!(
            "- TNN Recall@{}: mean {:.6} stdev {:.6}",
            recall_at.k, recall_at.mean_recall, recall_at.stdev_recall
        ));
    }
    lines.push("Visited-leaf oracle TNN recall:".into());
    for recall_at in &report.visited_leaf_oracle_recall_at {
        lines.push(format!(
            "- TNN Recall@{}: mean {:.6} stdev {:.6}",
            recall_at.k, recall_at.mean_recall, recall_at.stdev_recall
        ));
    }
    if !report.summary.first_blocking_points.is_empty() {
        lines
            .push("First blocking points for top-1 failures with an unvisited oracle leaf:".into());
        for point in &report.summary.first_blocking_points {
            lines.push(format!(
                "- parent level {} -> child level {}: {} query(ies)",
                point.blocked_parent_level, point.blocked_child_level, point.query_count
            ));
        }
    }
    lines.join("\n")
}

pub fn render_report_summary(report: &TreeQualityReport) -> String {
    let mut lines = vec![
        format!("Block-tree quality report for {}", report.root_id),
        format!(
            "Blocks: {} total ({} branch, {} leaf), {} edge(s), max depth {}, structural finding(s) {}, child-dispersion inversion(s) {}, parent split(s) {}",
            report.summary.block_count,
            report.summary.branch_count,
            report.summary.leaf_count,
            report.summary.edge_count,
            report.summary.max_depth,
            report.summary.structural_finding_count,
            report.summary.child_dispersion_inversion_count,
            report.summary.parent_split_count
        ),
        format!(
            "Aggregate spread: mean block mean-centroid-distance {:.6}, max block max-centroid-distance {:.6}",
            report.summary.mean_block_mean_centroid_distance,
            report.summary.max_block_max_centroid_distance
        ),
        format!(
            "Corpus TNN-recall [{}]: corpus {}, sample {}/{}, seed {}, traversal width {}",
            report.corpus_tnn_recall.query_source,
            report.corpus_tnn_recall.corpus_size,
            report.corpus_tnn_recall.effective_sample_size,
            report.corpus_tnn_recall.requested_sample_size,
            report.corpus_tnn_recall.seed,
            report.corpus_tnn_recall.traversal_width
        ),
        "Layer statistics:".into(),
    ];

    for recall_at in &report.corpus_tnn_recall.recall_at {
        let histogram = recall_at
            .histogram
            .iter()
            .map(|bin| {
                format!(
                    "{} match(es) ({:.3}) => {} sample(s)",
                    bin.matched_neighbor_count, bin.recall, bin.sample_count
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!(
            "- TNN Recall@{}: mean {:.6} stdev {:.6}, histogram [{}]",
            recall_at.k, recall_at.mean_recall, recall_at.stdev_recall, histogram
        ));
    }

    for layer in &report.layers {
        lines.push(format!(
            "- level {}: blocks {}, intra-block mean {:.6} stdev {:.6}, sibling-centroid mean {:.6} stdev {:.6}, pca-axis mean {:.6} stdev {:.6}, quantile-var mean {:.6} stdev {:.6}, empty-bin blocks {}, overfull-bin blocks {}",
            layer.level,
            layer.block_count,
            layer.mean_intra_block_dispersion,
            layer.stdev_intra_block_dispersion,
            layer.mean_sibling_centroid_distance,
            layer.stdev_sibling_centroid_distance,
            layer.mean_pca_axis_strength,
            layer.stdev_pca_axis_strength,
            layer.mean_quantile_occupancy_variance,
            layer.stdev_quantile_occupancy_variance,
            layer.blocks_with_empty_bins,
            layer.blocks_with_overfull_bins
        ));
    }

    lines.push("Per-parent split effectiveness:".into());
    for split in &report.splits {
        lines.push(format!(
            "- {} [level {} children {}] exceed-parent {} ({:.2}%), mean increase {:.6}, max increase {:.6}",
            split.parent_block_id,
            split.parent_level,
            split.child_count,
            split.child_dispersion_exceeds_parent_count,
            split.child_dispersion_exceeds_parent_percentage,
            split.mean_dispersion_increase_for_exceeding_children,
            split.max_dispersion_increase_for_exceeding_children
        ));
    }

    lines.push("Per-block statistics:".into());
    for block in &report.blocks {
        lines.push(format!(
            "- {} [{} level {} depth {} entries {} parent {}] mean {:.6}, max {:.6}, pca-axis {:.6}, quantile occupancies {:?}, quantile-var {:.6}, empty bins {}, overfull bins {}",
            block.block_id,
            block.kind,
            block.level,
            block.reachable_depth,
            block.entry_count,
            block.parent_block_id.as_deref().unwrap_or("<root>"),
            block.spread.mean_centroid_distance,
            block.spread.max_centroid_distance,
            block.pca_first_component_variance_fraction,
            block.quantile_occupancy.occupancies,
            block.quantile_occupancy.occupancy_variance,
            block.quantile_occupancy.empty_bin_count,
            block.quantile_occupancy.overfull_bin_count
        ));
    }

    if !report.findings.is_empty() {
        lines.push("Findings:".into());
        for finding in &report.findings {
            lines.push(format!(
                "- {:?} {:?}: {}",
                finding.severity, finding.kind, finding.message
            ));
        }
    }

    lines.join("\n")
}

fn traverse_block(
    block_id: BlockHash,
    block: &Block,
    parent: Option<(BlockHash, &BlockQualityMetrics)>,
    depth: usize,
    store: &dyn BlockStore,
    ancestry: &mut Vec<BlockHash>,
    state: &mut TraversalState,
) -> Result<(), TreeQualityError> {
    if state.visited.contains(&block_id) {
        return Ok(());
    }
    state.max_depth = state.max_depth.max(depth);
    state.visited.insert(block_id);
    ancestry.push(block_id);

    let metrics = block_metrics(block_id, block, parent.as_ref().map(|(id, _)| *id), depth)?;
    if let Some((parent_id, parent_metrics)) = parent {
        if metrics.level >= parent_metrics.level {
            state.push_finding(TreeQualityFinding {
                severity: FindingSeverity::Error,
                kind: FindingKind::ChildLevelNotLowerThanParent,
                block_id: block_id.to_string(),
                parent_block_id: Some(parent_id.to_string()),
                message: format!(
                    "child {} level {} is not lower than parent {} level {}",
                    block_id, metrics.level, parent_id, parent_metrics.level
                ),
                parent_mean_centroid_distance: Some(parent_metrics.spread.mean_centroid_distance),
                child_mean_centroid_distance: Some(metrics.spread.mean_centroid_distance),
            });
        }
        if metrics.embedding_spec != parent_metrics.embedding_spec {
            state.push_finding(TreeQualityFinding {
                severity: FindingSeverity::Error,
                kind: FindingKind::EmbeddingSpecMismatch,
                block_id: block_id.to_string(),
                parent_block_id: Some(parent_id.to_string()),
                message: format!(
                    "child {} embedding spec {}/{} does not match parent {} embedding spec {}/{}",
                    block_id,
                    metrics.embedding_spec.encoding,
                    metrics.embedding_spec.dims,
                    parent_id,
                    parent_metrics.embedding_spec.encoding,
                    parent_metrics.embedding_spec.dims
                ),
                parent_mean_centroid_distance: Some(parent_metrics.spread.mean_centroid_distance),
                child_mean_centroid_distance: Some(metrics.spread.mean_centroid_distance),
            });
        }
    }

    state
        .parent_by_id
        .entry(block_id)
        .or_insert(parent.map(|(id, _)| id));
    state.metrics_by_id.insert(block_id, metrics.clone());
    state.blocks.push(metrics.clone());

    match block {
        Block::Branch(branch) => {
            for entry in &branch.entries {
                state.edge_count += 1;
                handle_child_entry(block_id, &metrics, entry, depth + 1, store, ancestry, state)?;
            }
        }
        Block::Leaf(leaf) => collect_corpus_entries(block_id, leaf, state)?,
    }

    ancestry.pop();
    Ok(())
}

fn handle_child_entry(
    parent_id: BlockHash,
    parent_metrics: &BlockQualityMetrics,
    entry: &BranchEntry,
    depth: usize,
    store: &dyn BlockStore,
    ancestry: &mut Vec<BlockHash>,
    state: &mut TraversalState,
) -> Result<(), TreeQualityError> {
    if ancestry.contains(&entry.child) {
        state.push_finding(TreeQualityFinding {
            severity: FindingSeverity::Error,
            kind: FindingKind::CycleDetected,
            block_id: entry.child.to_string(),
            parent_block_id: Some(parent_id.to_string()),
            message: format!(
                "child {} closes a reachable cycle from parent {}",
                entry.child, parent_id
            ),
            parent_mean_centroid_distance: Some(parent_metrics.spread.mean_centroid_distance),
            child_mean_centroid_distance: None,
        });
        return Ok(());
    }
    if state.visited.contains(&entry.child) {
        state.push_finding(TreeQualityFinding {
            severity: FindingSeverity::Error,
            kind: FindingKind::SharedChildReference,
            block_id: entry.child.to_string(),
            parent_block_id: Some(parent_id.to_string()),
            message: format!(
                "child {} is reachable from multiple parent paths, so the rooted snapshot is not a tree",
                entry.child
            ),
            parent_mean_centroid_distance: Some(parent_metrics.spread.mean_centroid_distance),
            child_mean_centroid_distance: state
                .metrics_by_id
                .get(&entry.child)
                .map(|metrics| metrics.spread.mean_centroid_distance),
        });
        return Ok(());
    }
    let Some(validated_child) = store.get(&entry.child)? else {
        state.push_finding(TreeQualityFinding {
            severity: FindingSeverity::Error,
            kind: FindingKind::MissingChildBlock,
            block_id: entry.child.to_string(),
            parent_block_id: Some(parent_id.to_string()),
            message: format!(
                "parent {} references missing child block {}",
                parent_id, entry.child
            ),
            parent_mean_centroid_distance: Some(parent_metrics.spread.mean_centroid_distance),
            child_mean_centroid_distance: None,
        });
        return Ok(());
    };

    state
        .child_ids_by_parent
        .entry(parent_id)
        .or_default()
        .push(validated_child.hash);
    state
        .parent_by_id
        .insert(validated_child.hash, Some(parent_id));

    traverse_block(
        validated_child.hash,
        &validated_child.block,
        Some((parent_id, parent_metrics)),
        depth,
        store,
        ancestry,
        state,
    )
}

fn build_layer_metrics(state: &TraversalState) -> Vec<LayerQualityMetrics> {
    let mut dispersion_by_layer = BTreeMap::<u64, Vec<f32>>::new();
    let mut pca_by_layer = BTreeMap::<u64, Vec<f32>>::new();
    let mut quantile_variance_by_layer = BTreeMap::<u64, Vec<f32>>::new();
    let mut empty_bins_by_layer = BTreeMap::<u64, usize>::new();
    let mut overfull_bins_by_layer = BTreeMap::<u64, usize>::new();
    let mut sibling_distances_by_layer = BTreeMap::<u64, RunningStats>::new();

    for block in &state.blocks {
        dispersion_by_layer
            .entry(block.level)
            .or_default()
            .push(block.spread.mean_centroid_distance);
        pca_by_layer
            .entry(block.level)
            .or_default()
            .push(block.pca_first_component_variance_fraction);
        quantile_variance_by_layer
            .entry(block.level)
            .or_default()
            .push(block.quantile_occupancy.occupancy_variance);
        if block.quantile_occupancy.empty_bin_count > 0 {
            *empty_bins_by_layer.entry(block.level).or_default() += 1;
        }
        if block.quantile_occupancy.overfull_bin_count > 0 {
            *overfull_bins_by_layer.entry(block.level).or_default() += 1;
        }
    }

    for child_ids in state.child_ids_by_parent.values() {
        let mut by_child_level = BTreeMap::<u64, Vec<&BlockQualityMetrics>>::new();
        for child_id in child_ids {
            if let Some(metrics) = state.metrics_by_id.get(child_id) {
                by_child_level
                    .entry(metrics.level)
                    .or_default()
                    .push(metrics);
            }
        }
        for (level, children) in by_child_level {
            if children.len() < 2 {
                continue;
            }
            let distances = sibling_distances_by_layer.entry(level).or_default();
            for left_index in 0..children.len() {
                for right_index in (left_index + 1)..children.len() {
                    distances.push(euclidean_distance(
                        &children[left_index].spread.centroid,
                        &children[right_index].spread.centroid,
                    ));
                }
            }
        }
    }

    dispersion_by_layer
        .into_iter()
        .map(|(level, dispersions)| LayerQualityMetrics {
            level,
            block_count: dispersions.len(),
            mean_intra_block_dispersion: mean(&dispersions),
            stdev_intra_block_dispersion: stdev(&dispersions),
            mean_sibling_centroid_distance: sibling_distances_by_layer
                .get(&level)
                .copied()
                .unwrap_or_default()
                .mean(),
            stdev_sibling_centroid_distance: sibling_distances_by_layer
                .get(&level)
                .copied()
                .unwrap_or_default()
                .stdev(),
            mean_pca_axis_strength: mean(
                pca_by_layer.get(&level).map(Vec::as_slice).unwrap_or(&[]),
            ),
            stdev_pca_axis_strength: stdev(
                pca_by_layer.get(&level).map(Vec::as_slice).unwrap_or(&[]),
            ),
            mean_quantile_occupancy_variance: mean(
                quantile_variance_by_layer
                    .get(&level)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            ),
            stdev_quantile_occupancy_variance: stdev(
                quantile_variance_by_layer
                    .get(&level)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            ),
            blocks_with_empty_bins: empty_bins_by_layer.get(&level).copied().unwrap_or(0),
            blocks_with_overfull_bins: overfull_bins_by_layer.get(&level).copied().unwrap_or(0),
        })
        .collect()
}

fn build_split_metrics(state: &TraversalState) -> Vec<SplitEffectivenessMetrics> {
    let mut splits = state
        .child_ids_by_parent
        .iter()
        .filter_map(|(parent_id, child_ids)| {
            let parent = state.metrics_by_id.get(parent_id)?;
            if child_ids.is_empty() {
                return None;
            }
            let deltas = child_ids
                .iter()
                .filter_map(|child_id| {
                    state.metrics_by_id.get(child_id).map(|child| {
                        child.spread.mean_centroid_distance - parent.spread.mean_centroid_distance
                    })
                })
                .collect::<Vec<_>>();
            let exceeding = deltas
                .iter()
                .copied()
                .filter(|delta| *delta > EPSILON)
                .collect::<Vec<_>>();
            Some(SplitEffectivenessMetrics {
                parent_block_id: parent.block_id.clone(),
                parent_level: parent.level,
                child_count: deltas.len(),
                child_dispersion_exceeds_parent_count: exceeding.len(),
                child_dispersion_exceeds_parent_percentage: if deltas.is_empty() {
                    0.0
                } else {
                    exceeding.len() as f32 * 100.0 / deltas.len() as f32
                },
                mean_dispersion_increase_for_exceeding_children: mean(&exceeding),
                max_dispersion_increase_for_exceeding_children: exceeding
                    .iter()
                    .copied()
                    .fold(0.0f32, f32::max),
            })
        })
        .collect::<Vec<_>>();
    splits.sort_by(|left, right| left.parent_block_id.cmp(&right.parent_block_id));
    splits
}

fn build_corpus_tnn_recall_report(
    root_id: &BlockHash,
    root_embedding_spec: &EmbeddingSpec,
    state: &TraversalState,
    store: &dyn BlockStore,
    config: TnnRecallConfig,
) -> Result<CorpusTnnRecallReport, TreeQualityError> {
    let traversal_width = config.traversal_width;
    let corpus_size = state.corpus_entries.len();
    let can_compute_recall = corpus_size >= 2
        && !has_embedding_spec_mismatch(state)
        && !state.has_zero_magnitude_tnn_entry;
    let effective_sample_size = if can_compute_recall {
        config.sample_size.min(corpus_size)
    } else {
        0
    };
    if effective_sample_size == 0 {
        return Ok(zeroed_corpus_tnn_recall_report(
            corpus_size,
            config.sample_size,
            effective_sample_size,
            config.seed,
            traversal_width,
        ));
    }

    let sampled_queries = select_corpus_sample(&state.corpus_entries, config);
    let max_k = REQUIRED_RECALL_AT
        .iter()
        .copied()
        .max()
        .unwrap_or(1)
        .min(corpus_size.saturating_sub(1));
    let searcher = Searcher::new(DefaultEmbeddingCompatibility, DefaultCandidateScorer);
    let mut recalls_by_k = REQUIRED_RECALL_AT
        .into_iter()
        .map(|k| (k, Vec::<usize>::new()))
        .collect::<BTreeMap<_, _>>();

    for query in sampled_queries {
        let exact_neighbors = exact_neighbors(&state.corpus_entries, query, max_k)?;
        let approximate_neighbors = approximate_neighbors(
            root_id,
            root_embedding_spec,
            query,
            max_k,
            traversal_width,
            store,
            &searcher,
        )?;
        for &k in &REQUIRED_RECALL_AT {
            let denominator = exact_neighbors.len().min(k);
            let approximate_ids = approximate_neighbors
                .iter()
                .take(k)
                .map(|entry| entry.neighbor_id.clone())
                .collect::<HashSet<_>>();
            let matched = exact_neighbors
                .iter()
                .take(k)
                .filter(|entry| approximate_ids.contains(&entry.neighbor_id))
                .count()
                .min(denominator);
            recalls_by_k.entry(k).or_default().push(matched);
        }
    }

    Ok(CorpusTnnRecallReport {
        query_source: "corpus-based".into(),
        corpus_size,
        requested_sample_size: config.sample_size,
        effective_sample_size,
        seed: config.seed,
        traversal_width,
        recall_at: REQUIRED_RECALL_AT
            .into_iter()
            .map(|k| {
                let counts = recalls_by_k.remove(&k).unwrap_or_default();
                tnn_recall_metrics(k, k.min(corpus_size.saturating_sub(1)), &counts)
            })
            .collect(),
    })
}

fn zeroed_corpus_tnn_recall_report(
    corpus_size: usize,
    requested_sample_size: usize,
    effective_sample_size: usize,
    seed: u64,
    traversal_width: usize,
) -> CorpusTnnRecallReport {
    CorpusTnnRecallReport {
        query_source: "corpus-based".into(),
        corpus_size,
        requested_sample_size,
        effective_sample_size,
        seed,
        traversal_width,
        recall_at: REQUIRED_RECALL_AT
            .into_iter()
            .map(|k| TnnRecallAtMetrics {
                k,
                mean_recall: 0.0,
                stdev_recall: 0.0,
                histogram: Vec::new(),
            })
            .collect(),
    }
}

fn has_embedding_spec_mismatch(state: &TraversalState) -> bool {
    state
        .findings
        .iter()
        .any(|finding| finding.kind == FindingKind::EmbeddingSpecMismatch)
}

fn tnn_recall_metrics(k: usize, denominator: usize, counts: &[usize]) -> TnnRecallAtMetrics {
    let recalls = if denominator == 0 {
        vec![0.0; counts.len()]
    } else {
        counts
            .iter()
            .map(|count| *count as f32 / denominator as f32)
            .collect::<Vec<_>>()
    };
    let mut histogram_counts = BTreeMap::<usize, usize>::new();
    for count in counts {
        *histogram_counts.entry(*count).or_default() += 1;
    }
    TnnRecallAtMetrics {
        k,
        mean_recall: mean(&recalls),
        stdev_recall: stdev(&recalls),
        histogram: histogram_counts
            .into_iter()
            .map(
                |(matched_neighbor_count, sample_count)| TnnRecallHistogramBin {
                    matched_neighbor_count,
                    recall: if denominator == 0 {
                        0.0
                    } else {
                        matched_neighbor_count as f32 / denominator as f32
                    },
                    sample_count,
                },
            )
            .collect(),
    }
}

fn select_corpus_sample(
    entries: &[CorpusLeafEntry],
    config: TnnRecallConfig,
) -> Vec<&CorpusLeafEntry> {
    let mut ordered = entries
        .iter()
        .map(|entry| (sample_key(&entry.neighbor_id, config.seed), entry))
        .collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.neighbor_id.cmp(&right.1.neighbor_id))
    });
    ordered
        .into_iter()
        .take(config.sample_size.min(entries.len()))
        .map(|(_, entry)| entry)
        .collect()
}

fn sample_key(neighbor_id: &str, seed: u64) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(seed.to_le_bytes());
    digest.update(neighbor_id.as_bytes());
    digest.finalize().into()
}

fn exact_neighbors<'a>(
    corpus_entries: &'a [CorpusLeafEntry],
    query: &CorpusLeafEntry,
    max_k: usize,
) -> Result<Vec<&'a CorpusLeafEntry>, TreeQualityError> {
    let mut ranked = corpus_entries
        .iter()
        .filter(|entry| entry.neighbor_id != query.neighbor_id)
        .map(|entry| {
            cosine_similarity(&query.embedding, &entry.embedding).map(|score| (score, entry))
        })
        .collect::<Result<Vec<_>, _>>()?;
    ranked.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                left.1
                    .leaf_block_id
                    .as_bytes()
                    .cmp(right.1.leaf_block_id.as_bytes())
            })
            .then_with(|| left.1.neighbor_id.cmp(&right.1.neighbor_id))
    });
    Ok(ranked
        .into_iter()
        .take(max_k)
        .map(|(_, entry)| entry)
        .collect())
}

fn approximate_neighbors(
    root_id: &BlockHash,
    root_embedding_spec: &EmbeddingSpec,
    query: &CorpusLeafEntry,
    max_k: usize,
    traversal_width: usize,
    store: &dyn BlockStore,
    searcher: &Searcher<DefaultEmbeddingCompatibility, DefaultCandidateScorer>,
) -> Result<Vec<CorpusLeafEntry>, TreeQualityError> {
    if max_k == 0 {
        return Ok(Vec::new());
    }
    let target = EncodedTargetEmbedding::new(
        encode_embedding_values(&query.embedding, root_embedding_spec, &query.leaf_block_id)?,
        root_embedding_spec.clone(),
    );
    let result = search_with_partial_retry(
        searcher,
        root_id,
        &target,
        traversal_width,
        max_k.saturating_add(1),
        store,
    )
    .map_err(TreeQualityError::from_search_error)?;
    result
        .leaves
        .into_iter()
        .map(|leaf| {
            corpus_entry_from_leaf_result(leaf.leaf_block_id, &leaf.entry, root_embedding_spec)
        })
        .filter(|entry| match entry {
            Ok(entry) => entry.neighbor_id != query.neighbor_id,
            Err(_) => true,
        })
        .take(max_k)
        .collect()
}

#[derive(Clone, Debug)]
enum TraceSearchCandidate {
    Branch {
        child: BlockHash,
        depth: usize,
        level: u64,
        rank_score: CosineScore,
        display_score: f32,
    },
    Leaf {
        entry: CorpusLeafEntry,
        level: u64,
        rank_score: CosineScore,
    },
}

impl TraceSearchCandidate {
    fn is_terminal(&self) -> bool {
        self.level() == 0
    }

    fn level(&self) -> u64 {
        match self {
            Self::Branch { level, .. } | Self::Leaf { level, .. } => *level,
        }
    }

    fn identity(&self) -> &[u8; 32] {
        match self {
            Self::Branch { child, .. } => child.as_bytes(),
            Self::Leaf { entry, .. } => entry.leaf_block_id.as_bytes(),
        }
    }
}

fn compare_trace_candidates(left: &TraceSearchCandidate, right: &TraceSearchCandidate) -> Ordering {
    trace_candidate_rank_score(right)
        .cmp(trace_candidate_rank_score(left))
        .then_with(|| left.level().cmp(&right.level()))
        .then_with(|| left.identity().cmp(right.identity()))
}

fn trace_candidate_rank_score(candidate: &TraceSearchCandidate) -> &CosineScore {
    match candidate {
        TraceSearchCandidate::Branch { rank_score, .. }
        | TraceSearchCandidate::Leaf { rank_score, .. } => rank_score,
    }
}

fn ancestry_path_for_leaf(
    root_id: &BlockHash,
    leaf_block_id: BlockHash,
    state: &TraversalState,
) -> Result<Vec<BlockHash>, TreeQualityError> {
    let mut path = Vec::new();
    let mut cursor = Some(leaf_block_id);
    while let Some(block_id) = cursor {
        path.push(block_id);
        cursor = state.parent_by_id.get(&block_id).copied().flatten();
    }
    path.reverse();
    if path.first() == Some(root_id) {
        Ok(path)
    } else {
        Err(TreeQualityError::Search {
            message: format!(
                "leaf block {} did not resolve to root {} while building falsification ancestry",
                leaf_block_id, root_id
            ),
        })
    }
}

fn matched_neighbor_counts(
    exact_neighbors: &[&CorpusLeafEntry],
    approximate_neighbors: &[CorpusLeafEntry],
) -> Vec<TnnMatchedNeighborCount> {
    REQUIRED_RECALL_AT
        .into_iter()
        .map(|k| {
            let denominator = exact_neighbors.len().min(k);
            let approximate_ids = approximate_neighbors
                .iter()
                .take(k)
                .map(|entry| entry.neighbor_id.clone())
                .collect::<HashSet<_>>();
            let matched_neighbor_count = exact_neighbors
                .iter()
                .take(k)
                .filter(|entry| approximate_ids.contains(&entry.neighbor_id))
                .count()
                .min(denominator);
            TnnMatchedNeighborCount {
                k,
                matched_neighbor_count,
            }
        })
        .collect()
}

fn first_blocking_round<'a>(
    trace: &'a SearchTraceOutcome,
    oracle_path: &[BlockHash],
) -> Option<&'a OraclePathRoundTrace> {
    let blocked_child = oracle_path.get(trace.deepest_visited_oracle_index + 1)?;
    trace.oracle_path_rounds.iter().find(|round| {
        round.local_rank.child_block_id == *blocked_child && !round.selected_for_expansion
    })
}

struct TraceSearchRequest<'a> {
    root_id: &'a BlockHash,
    root_embedding_spec: &'a EmbeddingSpec,
    query: &'a CorpusLeafEntry,
    max_k: usize,
    traversal_width: usize,
    store: &'a dyn BlockStore,
    oracle_path: &'a [BlockHash],
    state: &'a TraversalState,
}

struct PendingOracleRoundState<'a> {
    frontier: &'a [TraceSearchCandidate],
    expanded_children: &'a HashSet<BlockHash>,
    oracle_path: &'a [BlockHash],
    local_ranks_by_child: &'a HashMap<BlockHash, OracleChildLocalRank>,
}

fn trace_approximate_neighbors(
    request: TraceSearchRequest<'_>,
) -> Result<SearchTraceOutcome, TreeQualityError> {
    let TraceSearchRequest {
        root_id,
        root_embedding_spec,
        query,
        max_k,
        traversal_width,
        store,
        oracle_path,
        state,
    } = request;
    if max_k == 0 {
        return Ok(SearchTraceOutcome {
            approximate_neighbors: Vec::new(),
            visited_leaf_neighbors: Vec::new(),
            deepest_visited_oracle_index: 0,
            oracle_leaf_visited: false,
            oracle_path_rounds: Vec::new(),
        });
    }
    let target = EncodedTargetEmbedding::new(
        encode_embedding_values(&query.embedding, root_embedding_spec, &query.leaf_block_id)?,
        root_embedding_spec.clone(),
    );
    let requested = max_k.saturating_add(1);
    let mut frontier = Vec::new();
    let mut local_ranks_by_child = HashMap::<BlockHash, OracleChildLocalRank>::new();
    let mut visited_leaf_neighbors = HashMap::<String, CorpusLeafEntry>::new();
    let mut deepest_visited_oracle_index = 0usize;
    let mut oracle_leaf_visited = false;
    let mut oracle_path_rounds = Vec::new();

    let initial = load_trace_block_candidates(
        root_id,
        &target,
        store,
        0,
        oracle_path,
        deepest_visited_oracle_index,
        state,
    )?;
    if let Some(local_rank) = initial.oracle_local_rank {
        local_ranks_by_child.insert(local_rank.child_block_id, local_rank);
    }
    frontier.extend(initial.candidates);

    let mut expanded_children = HashSet::new();
    let mut round_index = 0usize;

    loop {
        frontier.retain(|candidate| {
            !matches!(
                candidate,
                TraceSearchCandidate::Branch { child, .. } if expanded_children.contains(child)
            )
        });
        frontier.sort_by(compare_trace_candidates);

        if frontier.len() >= requested
            && frontier
                .iter()
                .take(requested)
                .all(TraceSearchCandidate::is_terminal)
        {
            let pending_oracle_round = PendingOracleRoundState {
                frontier: &frontier,
                expanded_children: &expanded_children,
                oracle_path,
                local_ranks_by_child: &local_ranks_by_child,
            };
            record_pending_oracle_round(
                pending_oracle_round,
                deepest_visited_oracle_index,
                round_index,
                false,
                &mut oracle_path_rounds,
            );
            let approximate_neighbors = frontier
                .iter()
                .filter_map(|candidate| match candidate {
                    TraceSearchCandidate::Leaf { entry, .. } => Some(entry.clone()),
                    TraceSearchCandidate::Branch { .. } => None,
                })
                .filter(|entry| entry.neighbor_id != query.neighbor_id)
                .take(max_k)
                .collect::<Vec<_>>();
            return Ok(SearchTraceOutcome {
                approximate_neighbors,
                visited_leaf_neighbors: rerank_visited_leaf_neighbors(
                    &visited_leaf_neighbors,
                    &query.neighbor_id,
                    &query.embedding,
                    max_k,
                )?,
                deepest_visited_oracle_index,
                oracle_leaf_visited,
                oracle_path_rounds,
            });
        }

        let current_round =
            select_trace_children_to_expand(&frontier, &expanded_children, traversal_width);
        if current_round.is_empty() {
            let pending_oracle_round = PendingOracleRoundState {
                frontier: &frontier,
                expanded_children: &expanded_children,
                oracle_path,
                local_ranks_by_child: &local_ranks_by_child,
            };
            record_pending_oracle_round(
                pending_oracle_round,
                deepest_visited_oracle_index,
                round_index,
                false,
                &mut oracle_path_rounds,
            );
            let approximate_neighbors = frontier
                .iter()
                .filter_map(|candidate| match candidate {
                    TraceSearchCandidate::Leaf { entry, .. } => Some(entry.clone()),
                    TraceSearchCandidate::Branch { .. } => None,
                })
                .filter(|entry| entry.neighbor_id != query.neighbor_id)
                .take(max_k)
                .collect::<Vec<_>>();
            return Ok(SearchTraceOutcome {
                approximate_neighbors,
                visited_leaf_neighbors: rerank_visited_leaf_neighbors(
                    &visited_leaf_neighbors,
                    &query.neighbor_id,
                    &query.embedding,
                    max_k,
                )?,
                deepest_visited_oracle_index,
                oracle_leaf_visited,
                oracle_path_rounds,
            });
        }

        let pending_oracle_round = PendingOracleRoundState {
            frontier: &frontier,
            expanded_children: &expanded_children,
            oracle_path,
            local_ranks_by_child: &local_ranks_by_child,
        };
        record_pending_oracle_round(
            pending_oracle_round,
            deepest_visited_oracle_index,
            round_index,
            current_round
                .iter()
                .any(|child_id| oracle_path.get(deepest_visited_oracle_index + 1) == Some(child_id)),
            &mut oracle_path_rounds,
        );

        let current_round_set: HashSet<_> = current_round.iter().copied().collect();
        let mut next_candidates = Vec::new();
        for child_id in &current_round {
            let next_deepest_visited_oracle_index =
                if oracle_path.get(deepest_visited_oracle_index + 1) == Some(child_id) {
                    deepest_visited_oracle_index + 1
                } else {
                    deepest_visited_oracle_index
                };
            let child_depth = frontier
                .iter()
                .find_map(|candidate| match candidate {
                    TraceSearchCandidate::Branch { child, depth, .. } if child == child_id => {
                        Some(*depth)
                    }
                    _ => None,
                })
                .unwrap_or(1);
            let loaded = load_trace_block_candidates(
                child_id,
                &target,
                store,
                child_depth,
                oracle_path,
                next_deepest_visited_oracle_index,
                state,
            )?;
            if let Some(local_rank) = loaded.oracle_local_rank {
                local_ranks_by_child.insert(local_rank.child_block_id, local_rank);
            }
            for candidate in loaded.candidates {
                if let TraceSearchCandidate::Leaf { entry, .. } = &candidate {
                    visited_leaf_neighbors
                        .entry(entry.neighbor_id.clone())
                        .or_insert_with(|| entry.clone());
                }
                next_candidates.push(candidate);
            }
            if oracle_path.get(deepest_visited_oracle_index + 1) == Some(child_id) {
                deepest_visited_oracle_index += 1;
                oracle_leaf_visited = deepest_visited_oracle_index + 1 == oracle_path.len();
            }
            expanded_children.insert(*child_id);
        }

        frontier.retain(|candidate| {
            !matches!(
                candidate,
                TraceSearchCandidate::Branch { child, .. } if current_round_set.contains(child)
            )
        });
        frontier.extend(next_candidates);
        round_index += 1;
    }
}

fn record_pending_oracle_round(
    state: PendingOracleRoundState<'_>,
    deepest_visited_oracle_index: usize,
    round_index: usize,
    selected_for_expansion: bool,
    oracle_path_rounds: &mut Vec<OraclePathRoundTrace>,
) {
    let Some(next_oracle_child) = state.oracle_path.get(deepest_visited_oracle_index + 1) else {
        return;
    };
    let Some(frontier_branch_rank) =
        frontier_branch_rank(state.frontier, state.expanded_children, *next_oracle_child)
    else {
        return;
    };
    let Some(local_rank) = state.local_ranks_by_child.get(next_oracle_child).cloned() else {
        return;
    };
    if oracle_path_rounds.last().is_some_and(|round| {
        round.round_index == round_index && round.local_rank.child_block_id == *next_oracle_child
    }) {
        return;
    }
    oracle_path_rounds.push(OraclePathRoundTrace {
        round_index,
        local_rank,
        frontier_branch_rank,
        selected_for_expansion,
    });
}

fn frontier_branch_rank(
    frontier: &[TraceSearchCandidate],
    expanded_children: &HashSet<BlockHash>,
    target_child: BlockHash,
) -> Option<usize> {
    let mut seen_children = HashSet::new();
    let mut rank = 0usize;
    for candidate in frontier {
        let TraceSearchCandidate::Branch { child, .. } = candidate else {
            continue;
        };
        if expanded_children.contains(child) || !seen_children.insert(*child) {
            continue;
        }
        rank += 1;
        if *child == target_child {
            return Some(rank);
        }
    }
    None
}

fn select_trace_children_to_expand(
    frontier: &[TraceSearchCandidate],
    expanded_children: &HashSet<BlockHash>,
    traversal_width: usize,
) -> Vec<BlockHash> {
    let mut selected = Vec::new();
    let mut seen_children = HashSet::new();
    for candidate in frontier {
        let TraceSearchCandidate::Branch { child, .. } = candidate else {
            continue;
        };
        if expanded_children.contains(child) || !seen_children.insert(*child) {
            continue;
        }
        selected.push(*child);
        if selected.len() == traversal_width {
            break;
        }
    }
    selected
}

struct LoadedTraceCandidates {
    candidates: Vec<TraceSearchCandidate>,
    oracle_local_rank: Option<OracleChildLocalRank>,
}

fn load_trace_block_candidates(
    block_id: &BlockHash,
    target: &EncodedTargetEmbedding,
    store: &dyn BlockStore,
    depth: usize,
    oracle_path: &[BlockHash],
    deepest_visited_oracle_index: usize,
    state: &TraversalState,
) -> Result<LoadedTraceCandidates, TreeQualityError> {
    let Some(validated) = store.get(block_id)? else {
        return Err(TreeQualityError::Search {
            message: format!("missing block {block_id} while tracing TNN falsification"),
        });
    };
    let scorer = DefaultCandidateScorer;
    match &validated.block {
        Block::Branch(branch) => {
            let next_oracle_child = oracle_path.get(deepest_visited_oracle_index + 1).copied();
            let mut candidates = branch
                .entries
                .iter()
                .map(|entry| {
                    let rank_score = scorer
                        .score(target, &entry.embedding, &branch.embedding_spec)
                        .map_err(|error| TreeQualityError::Search {
                            message: format!(
                                "failed to score branch child {} from block {}: {error}",
                                entry.child, validated.hash
                            ),
                        })?;
                    let embedding =
                        decode_embedding_values(&entry.embedding, &branch.embedding_spec)
                            .ok_or_else(|| TreeQualityError::InvalidEmbeddingLength {
                                block_id: validated.hash.to_string(),
                                encoding: branch.embedding_spec.encoding.clone(),
                                dims: branch.embedding_spec.dims,
                                expected_bytes: usize::try_from(branch.embedding_spec.dims)
                                    .unwrap_or(0)
                                    .saturating_mul(4),
                                actual_bytes: entry.embedding.len(),
                            })?;
                    let display_score = cosine_similarity_embedding_bytes(target, &embedding)?;
                    Ok(TraceSearchCandidate::Branch {
                        child: entry.child,
                        depth: depth + 1,
                        level: branch.level,
                        rank_score,
                        display_score,
                    })
                })
                .collect::<Result<Vec<_>, TreeQualityError>>()?;
            candidates.sort_by(compare_trace_candidates);

            let oracle_local_rank = next_oracle_child.and_then(|oracle_child| {
                candidates
                    .iter()
                    .position(|candidate| matches!(candidate, TraceSearchCandidate::Branch { child, .. } if *child == oracle_child))
                    .map(|index| {
                        let TraceSearchCandidate::Branch {
                            child,
                            level,
                            display_score,
                            ..
                        } = &candidates[index]
                        else {
                            unreachable!("position lookup must yield a branch candidate")
                        };
                        OracleChildLocalRank {
                            parent_block_id: validated.hash,
                            parent_level: branch.level,
                            child_block_id: *child,
                            child_level: state
                                .metrics_by_id
                                .get(child)
                                .map(|metrics| metrics.level)
                                .unwrap_or(level.saturating_sub(1)),
                            local_sibling_count: candidates.len(),
                            local_sibling_rank: index + 1,
                            local_routing_score: *display_score,
                        }
                    })
            });

            Ok(LoadedTraceCandidates {
                candidates,
                oracle_local_rank,
            })
        }
        Block::Leaf(leaf) => Ok(LoadedTraceCandidates {
            candidates: leaf
                .entries
                .iter()
                .map(|entry| {
                    let rank_score = scorer
                        .score(target, &entry.embedding, &leaf.embedding_spec)
                        .map_err(|error| TreeQualityError::Search {
                            message: format!(
                                "failed to score leaf entry in block {}: {error}",
                                validated.hash
                            ),
                        })?;
                    let corpus_entry =
                        corpus_entry_from_leaf_result(validated.hash, entry, &leaf.embedding_spec)?;
                    Ok(TraceSearchCandidate::Leaf {
                        entry: corpus_entry,
                        level: leaf.level,
                        rank_score,
                    })
                })
                .collect::<Result<Vec<_>, TreeQualityError>>()?,
            oracle_local_rank: None,
        }),
    }
}

fn cosine_similarity_embedding_bytes(
    target: &EncodedTargetEmbedding,
    candidate: &[f32],
) -> Result<f32, TreeQualityError> {
    let query =
        decode_embedding_values(&target.bytes, &target.embedding_spec).ok_or_else(|| {
            TreeQualityError::Search {
                message: "failed to decode target embedding while tracing TNN falsification".into(),
            }
        })?;
    cosine_similarity(&query, candidate).map(|score| score as f32)
}

fn rerank_visited_leaf_neighbors(
    visited_leaf_neighbors: &HashMap<String, CorpusLeafEntry>,
    query_neighbor_id: &str,
    query_embedding: &[f32],
    max_k: usize,
) -> Result<Vec<CorpusLeafEntry>, TreeQualityError> {
    let mut ranked = visited_leaf_neighbors
        .values()
        .filter(|entry| entry.neighbor_id != query_neighbor_id)
        .map(|entry| {
            cosine_similarity(query_embedding, &entry.embedding).map(|score| (score, entry))
        })
        .collect::<Result<Vec<_>, _>>()?;
    ranked.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                left.1
                    .leaf_block_id
                    .as_bytes()
                    .cmp(right.1.leaf_block_id.as_bytes())
            })
            .then_with(|| left.1.neighbor_id.cmp(&right.1.neighbor_id))
    });
    Ok(ranked
        .into_iter()
        .take(max_k)
        .map(|(_, entry)| entry.clone())
        .collect())
}

fn collect_corpus_entries(
    block_id: BlockHash,
    leaf: &LeafBlock,
    state: &mut TraversalState,
) -> Result<(), TreeQualityError> {
    for entry in &leaf.entries {
        match corpus_entry_from_leaf_result(block_id, entry, &leaf.embedding_spec) {
            Ok(entry) => state.corpus_entries.push(entry),
            Err(TreeQualityError::ZeroMagnitudeEmbedding { .. }) => {
                state.has_zero_magnitude_tnn_entry = true;
            }
            Err(other) => return Err(other),
        }
    }
    Ok(())
}

fn corpus_entry_from_leaf_result(
    leaf_block_id: BlockHash,
    entry: &LeafEntry,
    embedding_spec: &EmbeddingSpec,
) -> Result<CorpusLeafEntry, TreeQualityError> {
    let Some(embedding) = decode_embedding_values(&entry.embedding, embedding_spec) else {
        let block_id = leaf_block_id.to_string();
        let encoding = embedding_spec.encoding.clone();
        let dims = embedding_spec.dims;
        return match expected_embedding_byte_len(embedding_spec) {
            Some(expected_bytes) if entry.embedding.len() != expected_bytes => {
                Err(TreeQualityError::InvalidEmbeddingLength {
                    block_id,
                    encoding,
                    dims,
                    expected_bytes,
                    actual_bytes: entry.embedding.len(),
                })
            }
            _ => Err(TreeQualityError::UnsupportedEmbeddingSpec {
                block_id,
                encoding,
                dims,
            }),
        };
    };
    if embedding.iter().any(|value| !value.is_finite()) {
        return Err(TreeQualityError::NonFiniteEmbedding {
            block_id: leaf_block_id.to_string(),
        });
    }
    if l2_norm(&embedding) <= EPSILON {
        return Err(TreeQualityError::ZeroMagnitudeEmbedding {
            block_id: leaf_block_id.to_string(),
        });
    }

    Ok(CorpusLeafEntry {
        neighbor_id: corpus_neighbor_id(leaf_block_id, entry),
        leaf_block_id,
        embedding,
    })
}

fn encode_embedding_values(
    embedding: &[f32],
    embedding_spec: &EmbeddingSpec,
    block_id: &BlockHash,
) -> Result<Vec<u8>, TreeQualityError> {
    let expected_dims = usize::try_from(embedding_spec.dims).map_err(|_| {
        TreeQualityError::UnsupportedEmbeddingSpec {
            block_id: block_id.to_string(),
            encoding: embedding_spec.encoding.clone(),
            dims: embedding_spec.dims,
        }
    })?;
    if embedding.len() != expected_dims {
        return Err(TreeQualityError::InvalidEmbeddingLength {
            block_id: block_id.to_string(),
            encoding: embedding_spec.encoding.clone(),
            dims: embedding_spec.dims,
            expected_bytes: expected_embedding_byte_len(embedding_spec).unwrap_or_default(),
            actual_bytes: std::mem::size_of_val(embedding),
        });
    }

    match embedding_spec.encoding.as_str() {
        "f32le" => Ok(embedding
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()),
        "f16le" => Ok(embedding
            .iter()
            .flat_map(|value| f16::from_f32(*value).to_le_bytes())
            .collect()),
        _ => Err(TreeQualityError::UnsupportedEmbeddingSpec {
            block_id: block_id.to_string(),
            encoding: embedding_spec.encoding.clone(),
            dims: embedding_spec.dims,
        }),
    }
}

fn corpus_neighbor_id(leaf_block_id: BlockHash, entry: &LeafEntry) -> String {
    let mut digest = Sha256::new();
    digest.update(leaf_block_id.as_bytes());
    digest.update(&entry.embedding);
    digest.update(entry.content.media_type.as_bytes());
    digest.update(&entry.content.body);
    for (key, value) in metadata_values_to_text_map(&entry.metadata) {
        digest.update(key.as_bytes());
        digest.update([0]);
        digest.update(value.as_bytes());
        digest.update([0xff]);
    }
    hex_string(&digest.finalize())
}

fn hex_string(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut text, "{byte:02x}");
    }
    text
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> Result<f64, TreeQualityError> {
    let mut dot = 0.0f64;
    let mut left_norm_sq = 0.0f64;
    let mut right_norm_sq = 0.0f64;
    for (left, right) in left.iter().zip(right.iter()) {
        let left = *left as f64;
        let right = *right as f64;
        dot += left * right;
        left_norm_sq += left * left;
        right_norm_sq += right * right;
    }
    if left_norm_sq == 0.0 || right_norm_sq == 0.0 {
        return Err(TreeQualityError::ZeroMagnitudeEmbedding {
            block_id: "<corpus-entry>".into(),
        });
    }
    Ok(dot / (left_norm_sq.sqrt() * right_norm_sq.sqrt()))
}

impl TreeQualityError {
    fn from_search_error(error: lexongraph_search::SearchError) -> Self {
        match error {
            lexongraph_search::SearchError::ScoringFailure { block_id, message }
                if message.contains("zero magnitude") =>
            {
                Self::ZeroMagnitudeEmbedding {
                    block_id: block_id.to_string(),
                }
            }
            other => Self::Search {
                message: other.to_string(),
            },
        }
    }
}

fn block_metrics(
    block_id: BlockHash,
    block: &Block,
    parent_block_id: Option<BlockHash>,
    reachable_depth: usize,
) -> Result<BlockQualityMetrics, TreeQualityError> {
    let (kind, level, embedding_spec, entry_count, computed) = match block {
        Block::Branch(branch) => (
            "branch",
            branch.level,
            EmbeddingSpecReport {
                dims: branch.embedding_spec.dims,
                encoding: branch.embedding_spec.encoding.clone(),
            },
            branch.entries.len(),
            compute_block_metrics(
                block_id,
                &branch.embedding_spec,
                branch.entries.iter().map(|entry| &entry.embedding),
            )?,
        ),
        Block::Leaf(leaf) => (
            "leaf",
            leaf.level,
            EmbeddingSpecReport {
                dims: leaf.embedding_spec.dims,
                encoding: leaf.embedding_spec.encoding.clone(),
            },
            leaf.entries.len(),
            compute_block_metrics(
                block_id,
                &leaf.embedding_spec,
                leaf.entries.iter().map(|entry| &entry.embedding),
            )?,
        ),
    };

    Ok(BlockQualityMetrics {
        block_id: block_id.to_string(),
        kind: kind.into(),
        level,
        entry_count,
        parent_block_id: parent_block_id.map(|value| value.to_string()),
        reachable_depth,
        embedding_spec,
        spread: computed.spread,
        pca_first_component_variance_fraction: computed.pca_first_component_variance_fraction,
        quantile_occupancy: computed.quantile_occupancy,
    })
}

fn embedding_spec_for_block(block: &Block) -> EmbeddingSpec {
    match block {
        Block::Branch(branch) => branch.embedding_spec.clone(),
        Block::Leaf(leaf) => leaf.embedding_spec.clone(),
    }
}

fn compute_block_metrics<'a, I>(
    block_id: BlockHash,
    embedding_spec: &EmbeddingSpec,
    embeddings: I,
) -> Result<BlockComputedMetrics, TreeQualityError>
where
    I: Iterator<Item = &'a Vec<u8>>,
{
    let decoded = decode_embeddings(block_id, embedding_spec, embeddings)?;
    let spread = spread_metrics(&decoded, embedding_spec);
    let centered = centered_vectors(&decoded, &spread.centroid);
    let (principal_axis, pca_first_component_variance_fraction) =
        principal_axis_strength(&centered, embedding_spec.dims as usize);
    let quantile_occupancy = quantile_occupancy_metrics(&centered, &principal_axis);

    Ok(BlockComputedMetrics {
        spread,
        pca_first_component_variance_fraction,
        quantile_occupancy,
    })
}

fn decode_embeddings<'a, I>(
    block_id: BlockHash,
    embedding_spec: &EmbeddingSpec,
    embeddings: I,
) -> Result<Vec<Vec<f32>>, TreeQualityError>
where
    I: Iterator<Item = &'a Vec<u8>>,
{
    let mut decoded = Vec::new();
    for embedding in embeddings {
        let Some(values) = decode_embedding_values(embedding, embedding_spec) else {
            let block_id = block_id.to_string();
            let encoding = embedding_spec.encoding.clone();
            let dims = embedding_spec.dims;
            return match expected_embedding_byte_len(embedding_spec) {
                Some(expected_bytes) if embedding.len() != expected_bytes => {
                    Err(TreeQualityError::InvalidEmbeddingLength {
                        block_id,
                        encoding,
                        dims,
                        expected_bytes,
                        actual_bytes: embedding.len(),
                    })
                }
                _ => Err(TreeQualityError::UnsupportedEmbeddingSpec {
                    block_id,
                    encoding,
                    dims,
                }),
            };
        };
        if values.iter().any(|value| !value.is_finite()) {
            return Err(TreeQualityError::NonFiniteEmbedding {
                block_id: block_id.to_string(),
            });
        }
        decoded.push(values);
    }
    Ok(decoded)
}

fn expected_embedding_byte_len(embedding_spec: &EmbeddingSpec) -> Option<usize> {
    let dimension_count = usize::try_from(embedding_spec.dims).ok()?;
    let bytes_per_value = match embedding_spec.encoding.as_str() {
        "f32le" => 4usize,
        "f16le" => 2usize,
        _ => return None,
    };
    dimension_count.checked_mul(bytes_per_value)
}

fn spread_metrics(decoded: &[Vec<f32>], embedding_spec: &EmbeddingSpec) -> SpreadMetrics {
    let dimension_count = usize::try_from(embedding_spec.dims).unwrap_or(0);
    let mut centroid = vec![0.0f32; dimension_count];
    if decoded.is_empty() {
        return SpreadMetrics {
            centroid,
            mean_centroid_distance: 0.0,
            max_centroid_distance: 0.0,
        };
    }
    for vector in decoded {
        for (index, value) in vector.iter().enumerate() {
            centroid[index] += *value;
        }
    }
    for value in &mut centroid {
        *value /= decoded.len() as f32;
    }

    let distances = decoded
        .iter()
        .map(|vector| euclidean_distance(vector, &centroid))
        .collect::<Vec<_>>();

    SpreadMetrics {
        centroid,
        mean_centroid_distance: mean(&distances),
        max_centroid_distance: distances.iter().copied().fold(0.0f32, f32::max),
    }
}

fn centered_vectors(decoded: &[Vec<f32>], centroid: &[f32]) -> Vec<Vec<f32>> {
    decoded
        .iter()
        .map(|vector| {
            vector
                .iter()
                .zip(centroid.iter())
                .map(|(value, center)| *value - *center)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn principal_axis_strength(centered: &[Vec<f32>], dimension_count: usize) -> (Vec<f32>, f32) {
    if centered.len() <= 1 || dimension_count == 0 {
        return (vec![0.0; dimension_count], 0.0);
    }

    let total_variance = centered
        .iter()
        .flat_map(|vector| vector.iter())
        .map(|value| value * value)
        .sum::<f32>();
    if total_variance <= EPSILON {
        return (vec![0.0; dimension_count], 0.0);
    }

    let mut axis = centered
        .iter()
        .find(|vector| l2_norm(vector) > EPSILON)
        .cloned()
        .unwrap_or_else(|| vec![1.0; dimension_count]);
    normalize(&mut axis);
    for _ in 0..POWER_ITERATION_STEPS {
        let mut next = covariance_apply(centered, &axis);
        if l2_norm(&next) <= EPSILON {
            break;
        }
        normalize(&mut next);
        axis = next;
    }

    let covariance_times_axis = covariance_apply(centered, &axis);
    let leading_variance = dot(&axis, &covariance_times_axis).max(0.0);
    let strength = (leading_variance / total_variance).clamp(0.0, 1.0);
    (axis, strength)
}

fn covariance_apply(centered: &[Vec<f32>], axis: &[f32]) -> Vec<f32> {
    let mut output = vec![0.0; axis.len()];
    for vector in centered {
        let projection = dot(vector, axis);
        for (index, value) in vector.iter().enumerate() {
            output[index] += projection * *value;
        }
    }
    output
}

fn quantile_occupancy_metrics(
    centered: &[Vec<f32>],
    principal_axis: &[f32],
) -> QuantileOccupancyMetrics {
    let sample_count = centered.len();
    let bin_count = DEFAULT_QUANTILE_BIN_COUNT;
    if sample_count == 0 {
        return QuantileOccupancyMetrics {
            bin_count,
            occupancies: vec![0; bin_count],
            occupancy_variance: 0.0,
            empty_bin_count: bin_count,
            overfull_bin_count: 0,
        };
    }

    let projections = centered
        .iter()
        .map(|vector| dot(vector, principal_axis))
        .collect::<Vec<_>>();
    let mut sorted = projections.clone();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap());
    let thresholds = (1..bin_count)
        .map(|index| {
            let rank = (index * sample_count).div_ceil(bin_count);
            sorted[rank.saturating_sub(1)]
        })
        .collect::<Vec<_>>();

    let mut occupancies = vec![0usize; bin_count];
    for projection in projections {
        let bin = thresholds
            .iter()
            .position(|threshold| projection <= *threshold)
            .unwrap_or(bin_count - 1);
        occupancies[bin] += 1;
    }
    let expected = sample_count as f32 / bin_count as f32;
    let occupancy_variance = occupancies
        .iter()
        .map(|count| {
            let delta = *count as f32 - expected;
            delta * delta
        })
        .sum::<f32>()
        / occupancies.len() as f32;

    QuantileOccupancyMetrics {
        bin_count,
        empty_bin_count: occupancies.iter().filter(|count| **count == 0).count(),
        overfull_bin_count: occupancies
            .iter()
            .filter(|count| (**count as f32) > 2.0 * expected + EPSILON)
            .count(),
        occupancy_variance,
        occupancies,
    }
}

fn euclidean_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| {
            let delta = *left - *right;
            delta * delta
        })
        .sum::<f32>()
        .sqrt()
}

fn mean(values: &[f32]) -> f32 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f32>() / values.len() as f32
    }
}

fn stdev(values: &[f32]) -> f32 {
    if values.len() <= 1 {
        0.0
    } else {
        let average = mean(values);
        (values
            .iter()
            .map(|value| {
                let delta = *value - average;
                delta * delta
            })
            .sum::<f32>()
            / values.len() as f32)
            .sqrt()
    }
}

fn dot(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| left * right)
        .sum()
}

fn l2_norm(values: &[f32]) -> f32 {
    dot(values, values).sqrt()
}

fn normalize(values: &mut [f32]) {
    let norm = l2_norm(values);
    if norm > EPSILON {
        for value in values {
            *value /= norm;
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use lexongraph_block::{Block, BranchBlock, Content, LeafBlock, LeafEntry, VERSION_1};
    use lexongraph_block_store_fs::FilesystemBlockStore;

    #[test]
    fn assessment_reports_structural_findings_and_quality_statistics() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let left_left = store.put(&leaf_block(0, &[1.0, 0.0])).unwrap();
        let left_right = store.put(&leaf_block(0, &[-1.0, 0.0])).unwrap();
        let right_left = store.put(&leaf_block(0, &[0.2, 0.0])).unwrap();
        let right_right = store.put(&leaf_block(0, &[-0.2, 0.0])).unwrap();

        let left_branch = store
            .put(&branch_block(
                1,
                vec![([1.0, 0.0], left_left), ([-1.0, 0.0], left_right)],
            ))
            .unwrap();
        let right_branch = store
            .put(&branch_block(
                2,
                vec![([0.2, 0.0], right_left), ([-0.2, 0.0], right_right)],
            ))
            .unwrap();
        let root = store
            .put(&branch_block(
                2,
                vec![([0.2, 0.0], left_branch), ([-0.2, 0.0], right_branch)],
            ))
            .unwrap();

        let report = assess_rooted_tree(&root, &store).unwrap();

        assert_eq!(report.summary.block_count, 7);
        assert_eq!(report.summary.structural_finding_count, 1);
        assert_eq!(report.summary.child_dispersion_inversion_count, 1);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.kind == FindingKind::ChildLevelNotLowerThanParent)
        );
        assert!(report.layers.iter().any(|layer| layer.level == 1));
        assert!(report.layers.iter().any(|layer| layer.level == 0));
        assert_eq!(report.splits.len(), 3);
        assert!(report.splits.iter().any(|split| {
            split.child_dispersion_exceeds_parent_count > 0
                && split.mean_dispersion_increase_for_exceeding_children > 0.0
        }));
        assert!(
            report.blocks.iter().all(|block| {
                (0.0..=1.0).contains(&block.pca_first_component_variance_fraction)
            })
        );
        let rendered = render_report_summary(&report);
        assert!(rendered.contains("Layer statistics:"));
        assert!(rendered.contains("Corpus TNN-recall [corpus-based]:"));
        assert!(rendered.contains("TNN Recall@1:"));
        assert!(rendered.contains("Per-parent split effectiveness:"));
        assert!(rendered.contains("Per-block statistics:"));
        assert!(rendered.contains("quantile occupancies ["));
    }

    #[test]
    fn assessment_reports_rooted_corpus_tnn_recall_metrics() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let alpha = store.put(&named_leaf_block("alpha", &[1.0, 0.0])).unwrap();
        let beta = store.put(&named_leaf_block("beta", &[0.0, 1.0])).unwrap();
        let root = store
            .put(&branch_block(
                1,
                vec![([1.0, 0.0], alpha), ([0.0, 1.0], beta)],
            ))
            .unwrap();

        let report = assess_rooted_tree_with_config(
            &root,
            &store,
            TnnRecallConfig {
                sample_size: 2,
                seed: 7,
                traversal_width: 7,
            },
        )
        .unwrap();

        assert_eq!(report.corpus_tnn_recall.query_source, "corpus-based");
        assert_eq!(report.corpus_tnn_recall.corpus_size, 2);
        assert_eq!(report.corpus_tnn_recall.effective_sample_size, 2);
        assert_eq!(report.corpus_tnn_recall.seed, 7);
        assert_eq!(report.corpus_tnn_recall.traversal_width, 7);
        assert_eq!(report.corpus_tnn_recall.recall_at.len(), 3);
        for metric in &report.corpus_tnn_recall.recall_at {
            assert_eq!(metric.mean_recall, 1.0);
            assert_eq!(metric.stdev_recall, 0.0);
            assert_eq!(metric.histogram.len(), 1);
            assert_eq!(metric.histogram[0].sample_count, 2);
            assert_eq!(metric.histogram[0].matched_neighbor_count, 1);
            assert_eq!(metric.histogram[0].recall, 1.0);
        }
    }

    #[test]
    fn assessment_zeroes_tnn_recall_when_embedding_specs_mismatch() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let matching = store
            .put(&named_leaf_block("matching", &[1.0, 0.0]))
            .unwrap();
        let mismatched = store
            .put(&named_leaf_block_with_dims("mismatched", &[0.0, 1.0, 0.0]))
            .unwrap();
        let root = store
            .put(&branch_block(
                1,
                vec![([1.0, 0.0], matching), ([0.0, 1.0], mismatched)],
            ))
            .unwrap();

        let report = assess_rooted_tree_with_config(
            &root,
            &store,
            TnnRecallConfig {
                sample_size: 2,
                seed: 7,
                traversal_width: 3,
            },
        )
        .unwrap();

        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.kind == FindingKind::EmbeddingSpecMismatch)
        );
        assert_eq!(report.corpus_tnn_recall.effective_sample_size, 0);
        assert!(
            report
                .corpus_tnn_recall
                .recall_at
                .iter()
                .all(|metric| metric.mean_recall == 0.0
                    && metric.stdev_recall == 0.0
                    && metric.histogram.is_empty())
        );
    }

    #[test]
    fn assessment_zeroes_tnn_recall_when_rooted_corpus_contains_zero_magnitude_entry() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let alpha = store.put(&named_leaf_block("alpha", &[1.0, 0.0])).unwrap();
        let beta = store.put(&named_leaf_block("beta", &[0.0, 1.0])).unwrap();
        let zero = store.put(&named_leaf_block("zero", &[0.0, 0.0])).unwrap();
        let root = store
            .put(&branch_block(
                1,
                vec![([1.0, 0.0], alpha), ([0.0, 1.0], beta), ([0.0, 0.0], zero)],
            ))
            .unwrap();

        let report = assess_rooted_tree_with_config(
            &root,
            &store,
            TnnRecallConfig {
                sample_size: 3,
                seed: 7,
                traversal_width: 3,
            },
        )
        .unwrap();

        assert_eq!(report.corpus_tnn_recall.corpus_size, 2);
        assert_eq!(report.corpus_tnn_recall.effective_sample_size, 0);
        assert!(
            report
                .corpus_tnn_recall
                .recall_at
                .iter()
                .all(|metric| metric.mean_recall == 0.0
                    && metric.stdev_recall == 0.0
                    && metric.histogram.is_empty())
        );
    }

    #[test]
    fn assessment_rejects_zero_tnn_recall_traversal_width() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();
        let root = store.put(&leaf_block(0, &[1.0, 0.0])).unwrap();

        let error = assess_rooted_tree_with_config(
            &root,
            &store,
            TnnRecallConfig {
                sample_size: 1,
                seed: 0,
                traversal_width: 0,
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TreeQualityError::InvalidTnnRecallTraversalWidth
        ));
    }

    #[test]
    fn falsification_reports_oracle_branch_not_visited_before_termination() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let query_leaf = store.put(&named_leaf_block("query", &[1.0, 0.0])).unwrap();
        let near_a = store.put(&named_leaf_block("near-a", &[0.8, 0.6])).unwrap();
        let near_b = store
            .put(&named_leaf_block("near-b", &[0.75, 0.66]))
            .unwrap();
        let near_c = store
            .put(&named_leaf_block("near-c", &[0.7, 0.71]))
            .unwrap();
        let oracle = store
            .put(&named_leaf_block("oracle", &[0.999, 0.001]))
            .unwrap();
        let favored = store
            .put(&branch_block(
                1,
                vec![
                    ([1.0, 0.0], query_leaf),
                    ([0.8, 0.6], near_a),
                    ([0.75, 0.66], near_b),
                    ([0.7, 0.71], near_c),
                ],
            ))
            .unwrap();
        let oracle_branch = store
            .put(&branch_block(1, vec![([0.999, 0.001], oracle)]))
            .unwrap();
        let root = store
            .put(&branch_block(
                2,
                vec![([1.0, 0.0], favored), ([0.6, 0.8], oracle_branch)],
            ))
            .unwrap();

        let query_block = store.get(&query_leaf).unwrap().unwrap();
        let query_entry = match &query_block.block {
            Block::Leaf(leaf) => {
                corpus_entry_from_leaf_result(query_leaf, &leaf.entries[0], &leaf.embedding_spec)
            }
            Block::Branch(_) => panic!("query leaf must remain a leaf block"),
        }
        .unwrap();
        let root_block = store.get(&root).unwrap().unwrap();
        let root_embedding_spec = embedding_spec_for_block(&root_block.block);
        let oracle_path = vec![root, oracle_branch, oracle];

        let trace = trace_approximate_neighbors(TraceSearchRequest {
            root_id: &root,
            root_embedding_spec: &root_embedding_spec,
            query: &query_entry,
            max_k: 1,
            traversal_width: 1,
            store: &store,
            oracle_path: &oracle_path,
            state: &TraversalState {
                blocks: Vec::new(),
                corpus_entries: Vec::new(),
                findings: Vec::new(),
                metrics_by_id: HashMap::new(),
                child_ids_by_parent: HashMap::new(),
                parent_by_id: HashMap::new(),
                visited: HashSet::new(),
                has_zero_magnitude_tnn_entry: false,
                structural_finding_count: 0,
                edge_count: 0,
                max_depth: 0,
            },
        })
        .unwrap();
        let blocking = first_blocking_round(&trace, &oracle_path).unwrap();

        assert!(!trace.oracle_leaf_visited);
        assert_eq!(trace.deepest_visited_oracle_index, 0);
        assert_eq!(trace.approximate_neighbors.len(), 1);
        assert_eq!(trace.approximate_neighbors[0].leaf_block_id, near_a);
        assert_eq!(blocking.local_rank.child_level, 1);
        assert_eq!(blocking.frontier_branch_rank, 2);
        assert!(!blocking.selected_for_expansion);
    }

    #[test]
    fn assessment_writes_json_artifact() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();
        let root = store.put(&leaf_block(0, &[1.0, 0.0])).unwrap();

        let report = assess_rooted_tree(&root, &store).unwrap();
        assert_eq!(report.corpus_tnn_recall.effective_sample_size, 0);
        assert!(
            report
                .corpus_tnn_recall
                .recall_at
                .iter()
                .all(|metric| metric.mean_recall == 0.0 && metric.stdev_recall == 0.0)
        );
        let path = dir.path().join("report.json");
        write_report(&path, &report).unwrap();

        let rendered = fs::read_to_string(path).unwrap();
        assert!(rendered.contains("\"root_id\""));
        assert!(rendered.contains("\"corpus_tnn_recall\""));
        assert!(rendered.contains("\"layers\""));
        assert!(rendered.contains("\"splits\""));
        assert!(rendered.contains("\"occupancies\""));
        assert!(!rendered.contains("\"centroid\""));
    }

    #[test]
    fn assessment_reports_invalid_embedding_length() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();
        let root = store
            .put(&Block::Leaf(LeafBlock {
                version: VERSION_1,
                level: 0,
                embedding_spec: EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                entries: vec![LeafEntry {
                    embedding: vec![0u8; 2],
                    metadata: Vec::new(),
                    content: Content {
                        media_type: "text/plain".into(),
                        body: b"body".to_vec(),
                    },
                }],
                ext: None,
            }))
            .unwrap();

        let error = assess_rooted_tree(&root, &store).unwrap_err();
        assert!(matches!(
            error,
            TreeQualityError::InvalidEmbeddingLength {
                expected_bytes: 8,
                actual_bytes: 2,
                ..
            }
        ));
    }

    #[test]
    fn quantile_occupancy_keeps_default_bin_count_for_degenerate_axis() {
        let metrics = quantile_occupancy_metrics(&[vec![0.0, 0.0], vec![0.0, 0.0]], &[0.0, 0.0]);

        assert_eq!(metrics.bin_count, DEFAULT_QUANTILE_BIN_COUNT);
        assert_eq!(metrics.occupancies, vec![2, 0, 0, 0]);
        assert_eq!(metrics.empty_bin_count, 3);
        assert_eq!(metrics.overfull_bin_count, 1);
    }

    fn branch_block(level: u64, entries: Vec<([f32; 2], BlockHash)>) -> Block {
        Block::Branch(BranchBlock {
            version: VERSION_1,
            level,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            entries: entries
                .into_iter()
                .map(|(embedding, child)| BranchEntry {
                    embedding: encode_f32(&embedding),
                    child,
                })
                .collect(),
            ext: None,
        })
    }

    fn leaf_block(level: u64, embedding: &[f32; 2]) -> Block {
        Block::Leaf(LeafBlock {
            version: VERSION_1,
            level,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            entries: vec![LeafEntry {
                embedding: encode_f32(embedding),
                metadata: Vec::new(),
                content: Content {
                    media_type: "text/plain".into(),
                    body: b"body".to_vec(),
                },
            }],
            ext: None,
        })
    }

    fn named_leaf_block(name: &str, embedding: &[f32; 2]) -> Block {
        Block::Leaf(LeafBlock {
            version: VERSION_1,
            level: 0,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            entries: vec![LeafEntry {
                embedding: encode_f32(embedding),
                metadata: vec![(
                    ciborium::Value::Text("source_name".into()),
                    ciborium::Value::Text(name.into()),
                )],
                content: Content {
                    media_type: "text/plain".into(),
                    body: name.as_bytes().to_vec(),
                },
            }],
            ext: None,
        })
    }

    fn named_leaf_block_with_dims(name: &str, embedding: &[f32; 3]) -> Block {
        Block::Leaf(LeafBlock {
            version: VERSION_1,
            level: 0,
            embedding_spec: EmbeddingSpec {
                dims: 3,
                encoding: "f32le".into(),
            },
            entries: vec![LeafEntry {
                embedding: encode_f32_3(embedding),
                metadata: vec![(
                    ciborium::Value::Text("source_name".into()),
                    ciborium::Value::Text(name.into()),
                )],
                content: Content {
                    media_type: "text/plain".into(),
                    body: name.as_bytes().to_vec(),
                },
            }],
            ext: None,
        })
    }

    fn encode_f32(values: &[f32; 2]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }

    fn encode_f32_3(values: &[f32; 3]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }
}
