// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use half::f16;
use lexongraph_block::{
    Block, BlockHash, BranchBlock, BranchEntry, EbcpDescriptor, EmbeddingSpec, LeafBlock,
    LeafEntry, ValidatedBlock, deserialize_block, parse_branch_ebcp_descriptor,
    reconstruct_logical_branch_embedding_f32,
};
use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};
use lexongraph_search::{
    DefaultCandidateScorer, DefaultEmbeddingCompatibility, EncodedTargetEmbedding, Searcher,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::block_store::block_on_block_store_future;
use crate::search::default_traversal_width as default_search_traversal_width;
use crate::tree_tools::{
    decode_embedding_values, metadata_values_to_text_map, search_with_partial_retry,
};

const DEFAULT_QUANTILE_BIN_COUNT: usize = 4;
const DEFAULT_TNN_RECALL_SAMPLE_SIZE: usize = 100;
const DEFAULT_TNN_RECALL_SEED: u64 = 0;
const FAST_RANDOM_WALK_QUERY_SOURCE: &str = "random-walk-sampled";
const REQUIRED_RECALL_AT: [usize; 3] = [1, 5, 10];
const POWER_ITERATION_STEPS: usize = 8;
const EPSILON: f32 = 1.0e-6;
const RTT_CWND_BYTES: usize = 65_536;

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
    #[error("block {block_id} embedding reconstruction failed: {message}")]
    EmbeddingReconstruction { block_id: String, message: String },
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
    #[serde(skip_serializing)]
    comparison_embedding_spec: EmbeddingSpecReport,
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
    pub fast_random_walk: bool,
}

impl Default for TnnRecallConfig {
    fn default() -> Self {
        Self {
            sample_size: DEFAULT_TNN_RECALL_SAMPLE_SIZE,
            seed: DEFAULT_TNN_RECALL_SEED,
            traversal_width: default_search_traversal_width(),
            fast_random_walk: false,
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
pub struct QueryAccessLevelMetrics {
    pub level: u64,
    pub touched_block_count: usize,
    pub bytes_read: usize,
    pub estimated_rtts: usize,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct QueryAccessMetrics {
    pub query_id: String,
    pub touched_block_count: usize,
    pub bytes_read: usize,
    pub estimated_rtts: usize,
    pub actual_query_elapsed_micros: u64,
    pub levels: Vec<QueryAccessLevelMetrics>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct QueryAccessSummary {
    pub query_count: usize,
    pub touched_block_count: usize,
    pub bytes_read: usize,
    pub estimated_rtts: usize,
    pub total_query_elapsed_micros: u64,
    pub mean_query_elapsed_micros: u64,
    pub max_query_elapsed_micros: u64,
    pub levels: Vec<QueryAccessLevelMetrics>,
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
    pub access_summary: QueryAccessSummary,
    pub query_accesses: Vec<QueryAccessMetrics>,
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

#[derive(Clone, Copy, Debug)]
struct QueryTouchedBlock {
    level: u64,
    bytes_read: usize,
}

#[derive(Clone, Debug)]
struct QueryAccessCapture {
    metrics: QueryAccessMetrics,
    touched_blocks: HashMap<BlockHash, QueryTouchedBlock>,
}

#[derive(Clone, Debug)]
struct CorpusLeafEntry {
    neighbor_id: String,
    leaf_block_id: BlockHash,
    embedding: Vec<f32>,
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

struct CountingBlockStore<'a> {
    inner: &'a dyn BlockStore,
    touched_blocks: Mutex<HashMap<BlockHash, QueryTouchedBlock>>,
}

impl<'a> CountingBlockStore<'a> {
    fn new(inner: &'a dyn BlockStore) -> Self {
        Self {
            inner,
            touched_blocks: Mutex::new(HashMap::new()),
        }
    }

    fn snapshot(&self) -> HashMap<BlockHash, QueryTouchedBlock> {
        self.touched_blocks
            .lock()
            .expect("counting block store mutex poisoned")
            .clone()
    }
}

#[async_trait]
impl BlockStore for CountingBlockStore<'_> {
    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        self.inner.put_block_bytes(block_id, block_bytes).await
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        self.inner.get_block_bytes(block_id).await
    }

    async fn get(&self, block_id: &BlockHash) -> Result<Option<ValidatedBlock>, BlockStoreError> {
        let Some(bytes) = self.inner.get_block_bytes(block_id).await? else {
            return Ok(None);
        };
        let validated =
            deserialize_block(&bytes, block_id).map_err(BlockStoreError::DecodeFailure)?;
        let level = match &validated.block {
            Block::Branch(branch) => branch.level,
            Block::Leaf(leaf) => leaf.level,
        };
        self.touched_blocks
            .lock()
            .expect("counting block store mutex poisoned")
            .entry(*block_id)
            .or_insert(QueryTouchedBlock {
                level,
                bytes_read: bytes.len(),
            });
        Ok(Some(validated))
    }

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
        self.inner.iter_block_ids()
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
    let Some(root) = block_on_block_store_future(store.get(root_id))? else {
        return Err(TreeQualityError::MissingRootBlock {
            root_id: root_id.to_string(),
        });
    };
    let root_query_embedding_spec =
        comparison_embedding_spec_for_block(Some(*root_id), &root.block)?;
    if tnn_recall.fast_random_walk {
        return assess_random_walk_queries(
            root_id,
            &root.block,
            &root_query_embedding_spec,
            store,
            tnn_recall,
        );
    }

    let mut state = TraversalState {
        blocks: Vec::new(),
        corpus_entries: Vec::new(),
        findings: Vec::new(),
        metrics_by_id: HashMap::new(),
        child_ids_by_parent: HashMap::new(),
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
        &root_query_embedding_spec,
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

fn assess_random_walk_queries(
    root_id: &BlockHash,
    root_block: &Block,
    root_query_embedding_spec: &EmbeddingSpec,
    store: &dyn BlockStore,
    config: TnnRecallConfig,
) -> Result<TreeQualityReport, TreeQualityError> {
    let sampled_queries = sample_random_walk_queries(*root_id, root_block, store, config)?;
    let query_accesses = build_random_walk_query_accesses(
        root_id,
        root_query_embedding_spec,
        &sampled_queries,
        store,
        config.traversal_width,
    )?;
    let effective_sample_size = sampled_queries.len();
    Ok(TreeQualityReport {
        root_id: root_id.to_string(),
        summary: TreeQualitySummary {
            block_count: 0,
            branch_count: 0,
            leaf_count: 0,
            edge_count: 0,
            max_depth: 0,
            structural_finding_count: 0,
            child_dispersion_inversion_count: 0,
            parent_split_count: 0,
            mean_block_mean_centroid_distance: 0.0,
            max_block_max_centroid_distance: 0.0,
        },
        corpus_tnn_recall: CorpusTnnRecallReport {
            query_source: FAST_RANDOM_WALK_QUERY_SOURCE.into(),
            corpus_size: 0,
            requested_sample_size: config.sample_size,
            effective_sample_size,
            seed: config.seed,
            traversal_width: config.traversal_width,
            recall_at: Vec::new(),
            access_summary: summarize_query_accesses(&query_accesses),
            query_accesses: query_accesses
                .into_iter()
                .map(|query_access| query_access.metrics)
                .collect(),
        },
        findings: Vec::new(),
        layers: Vec::new(),
        splits: Vec::new(),
        blocks: Vec::new(),
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

pub fn write_report(path: &Path, report: &TreeQualityReport) -> Result<(), TreeQualityError> {
    let rendered = serde_json::to_vec_pretty(report)?;
    fs::write(path, rendered).map_err(|source| TreeQualityError::WriteArtifact {
        path: path.display().to_string(),
        source,
    })
}

pub fn render_report_summary(report: &TreeQualityReport) -> String {
    let fast_random_walk = report.corpus_tnn_recall.query_source == FAST_RANDOM_WALK_QUERY_SOURCE;
    let mut lines = vec![format!("Block-tree quality report for {}", report.root_id)];
    if fast_random_walk {
        lines.push(
            "Fast random-walk mode: skipped full-tree structural analysis and exact recall baseline."
                .into(),
        );
        lines.push(format!(
            "Corpus TNN-recall [{}]: corpus size unknown, sample {}/{}, seed {}, traversal width {}",
            report.corpus_tnn_recall.query_source,
            report.corpus_tnn_recall.effective_sample_size,
            report.corpus_tnn_recall.requested_sample_size,
            report.corpus_tnn_recall.seed,
            report.corpus_tnn_recall.traversal_width
        ));
    } else {
        lines.extend([
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
        ]);
    }

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
    if fast_random_walk {
        lines.push("TNN recall metrics skipped in fast random-walk mode.".into());
    }

    lines.push(format!(
        "Corpus query access [{}]: queries {}, blocks {}, bytes {}, estimated RTTs {}, actual query time total/mean/max {} / {} / {} ms",
        report.corpus_tnn_recall.query_source,
        report.corpus_tnn_recall.access_summary.query_count,
        report.corpus_tnn_recall.access_summary.touched_block_count,
        report.corpus_tnn_recall.access_summary.bytes_read,
        report.corpus_tnn_recall.access_summary.estimated_rtts,
        format_elapsed_millis(report.corpus_tnn_recall.access_summary.total_query_elapsed_micros),
        format_elapsed_millis(report.corpus_tnn_recall.access_summary.mean_query_elapsed_micros),
        format_elapsed_millis(report.corpus_tnn_recall.access_summary.max_query_elapsed_micros)
    ));
    for level in &report.corpus_tnn_recall.access_summary.levels {
        lines.push(format!(
            "- access level {}: blocks {}, bytes {}, estimated RTTs {}",
            level.level, level.touched_block_count, level.bytes_read, level.estimated_rtts
        ));
    }
    lines.push("Per-query corpus access:".into());
    for query_access in &report.corpus_tnn_recall.query_accesses {
        lines.push(format!(
            "- {}: blocks {}, bytes {}, estimated RTTs {}, actual time {} ms",
            query_access.query_id,
            query_access.touched_block_count,
            query_access.bytes_read,
            query_access.estimated_rtts,
            format_elapsed_millis(query_access.actual_query_elapsed_micros)
        ));
        for level in &query_access.levels {
            lines.push(format!(
                "  - level {}: blocks {}, bytes {}, estimated RTTs {}",
                level.level, level.touched_block_count, level.bytes_read, level.estimated_rtts
            ));
        }
    }

    if !fast_random_walk {
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
        if metrics.comparison_embedding_spec != parent_metrics.comparison_embedding_spec {
            state.push_finding(TreeQualityFinding {
                severity: FindingSeverity::Error,
                kind: FindingKind::EmbeddingSpecMismatch,
                block_id: block_id.to_string(),
                parent_block_id: Some(parent_id.to_string()),
                message: format!(
                    "child {} logical/comparison embedding spec {}/{} does not match parent {} logical/comparison embedding spec {}/{}",
                    block_id,
                    metrics.comparison_embedding_spec.encoding,
                    metrics.comparison_embedding_spec.dims,
                    parent_id,
                    parent_metrics.comparison_embedding_spec.encoding,
                    parent_metrics.comparison_embedding_spec.dims
                ),
                parent_mean_centroid_distance: Some(parent_metrics.spread.mean_centroid_distance),
                child_mean_centroid_distance: Some(metrics.spread.mean_centroid_distance),
            });
        }
    }

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
    let Some(validated_child) = block_on_block_store_future(store.get(&entry.child))? else {
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
    root_query_embedding_spec: &EmbeddingSpec,
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
    let mut query_accesses = Vec::with_capacity(sampled_queries.len());

    for query in sampled_queries {
        let exact_neighbors = exact_neighbors(&state.corpus_entries, query, max_k)?;
        let (approximate_neighbors, query_access) = approximate_neighbors(
            root_id,
            root_query_embedding_spec,
            query,
            max_k,
            traversal_width,
            store,
            &searcher,
        )?;
        query_accesses.push(query_access);
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
        access_summary: summarize_query_accesses(&query_accesses),
        query_accesses: query_accesses
            .into_iter()
            .map(|query_access| query_access.metrics)
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
        access_summary: QueryAccessSummary {
            query_count: 0,
            touched_block_count: 0,
            bytes_read: 0,
            estimated_rtts: 0,
            total_query_elapsed_micros: 0,
            mean_query_elapsed_micros: 0,
            max_query_elapsed_micros: 0,
            levels: Vec::new(),
        },
        query_accesses: Vec::new(),
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

fn sample_random_walk_queries(
    root_id: BlockHash,
    root_block: &Block,
    store: &dyn BlockStore,
    config: TnnRecallConfig,
) -> Result<Vec<CorpusLeafEntry>, TreeQualityError> {
    let mut samples = Vec::new();
    let mut seen_neighbor_ids = HashSet::<String>::new();
    let max_attempts = config
        .sample_size
        .saturating_mul(32)
        .max(config.sample_size);
    for walk_index in 0..max_attempts {
        if samples.len() >= config.sample_size {
            break;
        }
        let Some(sample) =
            random_walk_query(root_id, root_block, store, config.seed, walk_index as u64)?
        else {
            continue;
        };
        if seen_neighbor_ids.insert(sample.neighbor_id.clone()) {
            samples.push(sample);
        }
    }
    Ok(samples)
}

fn random_walk_query(
    root_id: BlockHash,
    root_block: &Block,
    store: &dyn BlockStore,
    seed: u64,
    walk_index: u64,
) -> Result<Option<CorpusLeafEntry>, TreeQualityError> {
    let mut current_id = root_id;
    let mut current_block = root_block.clone();
    let mut depth = 0usize;
    loop {
        match current_block {
            Block::Leaf(ref leaf) => {
                if leaf.entries.is_empty() {
                    return Ok(None);
                }
                let entry_index = deterministic_choice_index(
                    seed,
                    walk_index,
                    depth,
                    &current_id,
                    leaf.entries.len(),
                    b"leaf-entry",
                );
                let entry = &leaf.entries[entry_index];
                return match corpus_entry_from_leaf_result(current_id, entry, &leaf.embedding_spec)
                {
                    Ok(entry) => Ok(Some(entry)),
                    Err(TreeQualityError::ZeroMagnitudeEmbedding { .. }) => Ok(None),
                    Err(other) => Err(other),
                };
            }
            Block::Branch(ref branch) => {
                if branch.entries.is_empty() {
                    return Ok(None);
                }
                let child_index = deterministic_choice_index(
                    seed,
                    walk_index,
                    depth,
                    &current_id,
                    branch.entries.len(),
                    b"branch-child",
                );
                let child_id = branch.entries[child_index].child;
                let Some(validated_child) = block_on_block_store_future(store.get(&child_id))?
                else {
                    return Err(TreeQualityError::Search {
                        message: format!(
                            "fast random-walk query sampling hit missing child block {} from parent {}",
                            child_id, current_id
                        ),
                    });
                };
                current_id = validated_child.hash;
                current_block = validated_child.block;
                depth += 1;
            }
        }
    }
}

fn deterministic_choice_index(
    seed: u64,
    walk_index: u64,
    depth: usize,
    block_id: &BlockHash,
    len: usize,
    salt: &[u8],
) -> usize {
    debug_assert!(len > 0);
    let mut digest = Sha256::new();
    digest.update(seed.to_le_bytes());
    digest.update(walk_index.to_le_bytes());
    digest.update((depth as u64).to_le_bytes());
    digest.update(block_id.as_bytes());
    digest.update(salt);
    let digest = digest.finalize();
    let bytes: [u8; 8] = digest[..8].try_into().expect("sha256 prefix must fit");
    (u64::from_le_bytes(bytes) % len as u64) as usize
}

fn build_random_walk_query_accesses(
    root_id: &BlockHash,
    root_query_embedding_spec: &EmbeddingSpec,
    sampled_queries: &[CorpusLeafEntry],
    store: &dyn BlockStore,
    traversal_width: usize,
) -> Result<Vec<QueryAccessCapture>, TreeQualityError> {
    let searcher = Searcher::new(DefaultEmbeddingCompatibility, DefaultCandidateScorer);
    let max_k = REQUIRED_RECALL_AT.iter().copied().max().unwrap_or(1);
    sampled_queries
        .iter()
        .map(|query| {
            approximate_neighbors(
                root_id,
                root_query_embedding_spec,
                query,
                max_k,
                traversal_width,
                store,
                &searcher,
            )
            .map(|(_, query_access)| query_access)
        })
        .collect()
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
    root_query_embedding_spec: &EmbeddingSpec,
    query: &CorpusLeafEntry,
    max_k: usize,
    traversal_width: usize,
    store: &dyn BlockStore,
    searcher: &Searcher<DefaultEmbeddingCompatibility, DefaultCandidateScorer>,
) -> Result<(Vec<CorpusLeafEntry>, QueryAccessCapture), TreeQualityError> {
    if max_k == 0 {
        return Ok((Vec::new(), QueryAccessCapture::zeroed(&query.neighbor_id)));
    }
    let target = EncodedTargetEmbedding::new(
        encode_embedding_values(
            &query.embedding,
            root_query_embedding_spec,
            &query.leaf_block_id,
        )?,
        root_query_embedding_spec.clone(),
    );
    let counting_store = Arc::new(CountingBlockStore::new(store));
    let search_store = Arc::clone(&counting_store);
    let started_at = Instant::now();
    let result = crate::block_store::block_on_future_factory(move || async move {
        search_with_partial_retry(
            searcher,
            root_id,
            &target,
            traversal_width,
            max_k.saturating_add(1),
            search_store.as_ref(),
        )
        .await
    })
    .map_err(TreeQualityError::from_search_error)?;
    let elapsed = started_at.elapsed();
    let neighbors = result
        .leaves
        .into_iter()
        .map(|leaf| {
            corpus_entry_from_leaf_result(
                leaf.leaf_block_id,
                &leaf.entry,
                root_query_embedding_spec,
            )
        })
        .filter(|entry| match entry {
            Ok(entry) => entry.neighbor_id != query.neighbor_id,
            Err(_) => true,
        })
        .take(max_k)
        .collect::<Result<Vec<_>, _>>()?;
    let query_access = build_query_access_capture(
        &query.neighbor_id,
        counting_store.snapshot(),
        duration_to_micros(elapsed),
    );
    Ok((neighbors, query_access))
}

impl QueryAccessCapture {
    fn zeroed(query_id: &str) -> Self {
        Self {
            metrics: QueryAccessMetrics {
                query_id: query_id.to_string(),
                touched_block_count: 0,
                bytes_read: 0,
                estimated_rtts: 0,
                actual_query_elapsed_micros: 0,
                levels: Vec::new(),
            },
            touched_blocks: HashMap::new(),
        }
    }
}

fn build_query_access_capture(
    query_id: &str,
    touched_blocks: HashMap<BlockHash, QueryTouchedBlock>,
    actual_query_elapsed_micros: u64,
) -> QueryAccessCapture {
    let levels = build_level_access_metrics(touched_blocks.values().copied());
    QueryAccessCapture {
        metrics: QueryAccessMetrics {
            query_id: query_id.to_string(),
            touched_block_count: touched_blocks.len(),
            bytes_read: touched_blocks.values().map(|touch| touch.bytes_read).sum(),
            estimated_rtts: levels.iter().map(|level| level.estimated_rtts).sum(),
            actual_query_elapsed_micros,
            levels,
        },
        touched_blocks,
    }
}

fn summarize_query_accesses(query_accesses: &[QueryAccessCapture]) -> QueryAccessSummary {
    let mut touched_blocks = HashMap::<BlockHash, QueryTouchedBlock>::new();
    let mut by_level = BTreeMap::<u64, HashMap<BlockHash, QueryTouchedBlock>>::new();
    let mut estimated_rtts_by_level = BTreeMap::<u64, usize>::new();
    for query_access in query_accesses {
        for (&block_id, &touch) in &query_access.touched_blocks {
            touched_blocks.entry(block_id).or_insert(touch);
            by_level
                .entry(touch.level)
                .or_default()
                .entry(block_id)
                .or_insert(touch);
        }
        for level in &query_access.metrics.levels {
            *estimated_rtts_by_level.entry(level.level).or_default() += level.estimated_rtts;
        }
    }
    let levels = by_level
        .into_iter()
        .map(|(level, touched_blocks)| QueryAccessLevelMetrics {
            level,
            touched_block_count: touched_blocks.len(),
            bytes_read: touched_blocks.values().map(|touch| touch.bytes_read).sum(),
            estimated_rtts: estimated_rtts_by_level.get(&level).copied().unwrap_or(0),
        })
        .collect::<Vec<_>>();
    QueryAccessSummary {
        query_count: query_accesses.len(),
        touched_block_count: touched_blocks.len(),
        bytes_read: touched_blocks.values().map(|touch| touch.bytes_read).sum(),
        estimated_rtts: query_accesses
            .iter()
            .map(|query| query.metrics.estimated_rtts)
            .sum(),
        total_query_elapsed_micros: query_accesses
            .iter()
            .map(|query| query.metrics.actual_query_elapsed_micros)
            .sum(),
        mean_query_elapsed_micros: if query_accesses.is_empty() {
            0
        } else {
            query_accesses
                .iter()
                .map(|query| query.metrics.actual_query_elapsed_micros)
                .sum::<u64>()
                / query_accesses.len() as u64
        },
        max_query_elapsed_micros: query_accesses
            .iter()
            .map(|query| query.metrics.actual_query_elapsed_micros)
            .max()
            .unwrap_or(0),
        levels,
    }
}

fn duration_to_micros(duration: std::time::Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

fn format_elapsed_millis(micros: u64) -> String {
    format!("{:.3}", micros as f64 / 1_000.0)
}

fn build_level_access_metrics(
    touched_blocks: impl IntoIterator<Item = QueryTouchedBlock>,
) -> Vec<QueryAccessLevelMetrics> {
    let mut by_level = BTreeMap::<u64, (usize, usize)>::new();
    for touch in touched_blocks {
        let entry = by_level.entry(touch.level).or_default();
        entry.0 += 1;
        entry.1 += touch.bytes_read;
    }
    by_level
        .into_iter()
        .map(
            |(level, (touched_block_count, bytes_read))| QueryAccessLevelMetrics {
                level,
                touched_block_count,
                bytes_read,
                estimated_rtts: estimate_rtts(bytes_read),
            },
        )
        .collect()
}

fn estimate_rtts(bytes_read: usize) -> usize {
    bytes_read.div_ceil(RTT_CWND_BYTES)
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
    let (kind, level, embedding_spec, entry_count, comparison_embedding_spec, computed) =
        match block {
            Block::Branch(branch) => {
                let (comparison_embedding_spec, descriptor) =
                    branch_embedding_decode_context(Some(block_id), branch)?;
                let computed = compute_branch_block_metrics(
                    block_id,
                    branch,
                    &comparison_embedding_spec,
                    descriptor.as_ref(),
                )?;
                (
                    "branch",
                    branch.level,
                    EmbeddingSpecReport {
                        dims: branch.embedding_spec.dims,
                        encoding: branch.embedding_spec.encoding.clone(),
                    },
                    branch.entries.len(),
                    comparison_embedding_spec,
                    computed,
                )
            }
            Block::Leaf(leaf) => {
                let comparison_embedding_spec = leaf.embedding_spec.clone();
                let computed = compute_leaf_block_metrics(
                    block_id,
                    &leaf.embedding_spec,
                    leaf.entries.iter().map(|entry| &entry.embedding),
                    &comparison_embedding_spec,
                )?;
                (
                    "leaf",
                    leaf.level,
                    EmbeddingSpecReport {
                        dims: leaf.embedding_spec.dims,
                        encoding: leaf.embedding_spec.encoding.clone(),
                    },
                    leaf.entries.len(),
                    comparison_embedding_spec,
                    computed,
                )
            }
        };

    Ok(BlockQualityMetrics {
        block_id: block_id.to_string(),
        kind: kind.into(),
        level,
        entry_count,
        parent_block_id: parent_block_id.map(|value| value.to_string()),
        reachable_depth,
        embedding_spec,
        comparison_embedding_spec: EmbeddingSpecReport {
            dims: comparison_embedding_spec.dims,
            encoding: comparison_embedding_spec.encoding,
        },
        spread: computed.spread,
        pca_first_component_variance_fraction: computed.pca_first_component_variance_fraction,
        quantile_occupancy: computed.quantile_occupancy,
    })
}

fn comparison_embedding_spec_for_block(
    block_id: Option<BlockHash>,
    block: &Block,
) -> Result<EmbeddingSpec, TreeQualityError> {
    match block {
        Block::Branch(branch) => branch_embedding_decode_context(block_id, branch)
            .map(|(comparison_embedding_spec, _)| comparison_embedding_spec),
        Block::Leaf(leaf) => Ok(leaf.embedding_spec.clone()),
    }
}

fn branch_embedding_decode_context(
    block_id: Option<BlockHash>,
    branch: &BranchBlock,
) -> Result<(EmbeddingSpec, Option<EbcpDescriptor>), TreeQualityError> {
    parse_branch_ebcp_descriptor(&branch.embedding_spec, branch.ext.as_ref())
        .map(|descriptor| {
            let comparison_embedding_spec = descriptor
                .as_ref()
                .map(|descriptor| descriptor.logical_embedding_spec.clone())
                .unwrap_or_else(|| branch.embedding_spec.clone());
            (comparison_embedding_spec, descriptor)
        })
        .map_err(|error| TreeQualityError::EmbeddingReconstruction {
            block_id: block_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<unknown>".into()),
            message: error.to_string(),
        })
}

fn compute_branch_block_metrics(
    block_id: BlockHash,
    branch: &BranchBlock,
    comparison_spec: &EmbeddingSpec,
    descriptor: Option<&EbcpDescriptor>,
) -> Result<BlockComputedMetrics, TreeQualityError> {
    let decoded = decode_branch_embeddings(block_id, branch, descriptor)?;
    let spread = spread_metrics(&decoded, comparison_spec);
    let centered = centered_vectors(&decoded, &spread.centroid);
    let (principal_axis, pca_first_component_variance_fraction) =
        principal_axis_strength(&centered, comparison_spec.dims as usize);
    let quantile_occupancy = quantile_occupancy_metrics(&centered, &principal_axis);

    Ok(BlockComputedMetrics {
        spread,
        pca_first_component_variance_fraction,
        quantile_occupancy,
    })
}

fn compute_leaf_block_metrics<'a, I>(
    block_id: BlockHash,
    embedding_spec: &EmbeddingSpec,
    embeddings: I,
    comparison_spec: &EmbeddingSpec,
) -> Result<BlockComputedMetrics, TreeQualityError>
where
    I: Iterator<Item = &'a Vec<u8>>,
{
    let decoded = decode_leaf_embeddings(block_id, embedding_spec, embeddings)?;
    let spread = spread_metrics(&decoded, comparison_spec);
    let centered = centered_vectors(&decoded, &spread.centroid);
    let (principal_axis, pca_first_component_variance_fraction) =
        principal_axis_strength(&centered, comparison_spec.dims as usize);
    let quantile_occupancy = quantile_occupancy_metrics(&centered, &principal_axis);

    Ok(BlockComputedMetrics {
        spread,
        pca_first_component_variance_fraction,
        quantile_occupancy,
    })
}

fn decode_branch_embeddings(
    block_id: BlockHash,
    branch: &BranchBlock,
    descriptor: Option<&EbcpDescriptor>,
) -> Result<Vec<Vec<f32>>, TreeQualityError> {
    let mut decoded = Vec::with_capacity(branch.entries.len());
    for entry in &branch.entries {
        let values = reconstruct_logical_branch_embedding_f32(
            &entry.embedding,
            &branch.embedding_spec,
            descriptor,
        )
        .map_err(|error| TreeQualityError::EmbeddingReconstruction {
            block_id: block_id.to_string(),
            message: error.to_string(),
        })?;
        if values.iter().any(|value| !value.is_finite()) {
            return Err(TreeQualityError::NonFiniteEmbedding {
                block_id: block_id.to_string(),
            });
        }
        decoded.push(values);
    }
    Ok(decoded)
}

fn decode_leaf_embeddings<'a, I>(
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
    use lexongraph_block::{
        Block, BranchBlock, Content, EbcpDescriptor, EbcpRotation, LeafBlock, LeafEntry, VERSION_1,
        ebcp_extension_map,
    };
    use lexongraph_block_store::BlockStore;
    use lexongraph_block_store_fs::FilesystemBlockStore;

    fn put_block(store: &impl BlockStore, block: &Block) -> BlockHash {
        crate::block_store::block_on_block_store_future(store.put(block)).unwrap()
    }

    #[test]
    fn assessment_reports_structural_findings_and_quality_statistics() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let left_left = put_block(&store, &leaf_block(0, &[1.0, 0.0]));
        let left_right = put_block(&store, &leaf_block(0, &[-1.0, 0.0]));
        let right_left = put_block(&store, &leaf_block(0, &[0.2, 0.0]));
        let right_right = put_block(&store, &leaf_block(0, &[-0.2, 0.0]));

        let left_branch = put_block(
            &store,
            &branch_block(1, vec![([1.0, 0.0], left_left), ([-1.0, 0.0], left_right)]),
        );
        let right_branch = put_block(
            &store,
            &branch_block(
                2,
                vec![([0.2, 0.0], right_left), ([-0.2, 0.0], right_right)],
            ),
        );
        let root = put_block(
            &store,
            &branch_block(
                2,
                vec![([0.2, 0.0], left_branch), ([-0.2, 0.0], right_branch)],
            ),
        );

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
        assert!(rendered.contains("Corpus query access [corpus-based]:"));
        assert!(rendered.contains("actual query time total/mean/max"));
        assert!(rendered.contains("Per-query corpus access:"));
        assert!(rendered.contains("Per-parent split effectiveness:"));
        assert!(rendered.contains("Per-block statistics:"));
        assert!(rendered.contains("quantile occupancies ["));
    }

    #[test]
    fn assessment_reports_rooted_corpus_tnn_recall_metrics() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let alpha = put_block(&store, &named_leaf_block("alpha", &[1.0, 0.0]));
        let beta = put_block(&store, &named_leaf_block("beta", &[0.0, 1.0]));
        let root = put_block(
            &store,
            &branch_block(1, vec![([1.0, 0.0], alpha), ([0.0, 1.0], beta)]),
        );

        let report = assess_rooted_tree_with_config(
            &root,
            &store,
            TnnRecallConfig {
                sample_size: 2,
                seed: 7,
                traversal_width: 7,
                fast_random_walk: false,
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
        assert_eq!(report.corpus_tnn_recall.access_summary.query_count, 2);
        assert_eq!(report.corpus_tnn_recall.query_accesses.len(), 2);
        let mut total_blocks = 0usize;
        let mut total_bytes = 0usize;
        let mut total_rtts = 0usize;
        let mut total_elapsed_micros = 0u64;
        for query_access in &report.corpus_tnn_recall.query_accesses {
            let level_blocks: usize = query_access
                .levels
                .iter()
                .map(|level| level.touched_block_count)
                .sum();
            let level_bytes: usize = query_access
                .levels
                .iter()
                .map(|level| level.bytes_read)
                .sum();
            let level_rtts: usize = query_access
                .levels
                .iter()
                .map(|level| level.estimated_rtts)
                .sum();
            assert_eq!(query_access.touched_block_count, level_blocks);
            assert_eq!(query_access.bytes_read, level_bytes);
            assert_eq!(query_access.estimated_rtts, level_rtts);
            assert!(
                query_access.actual_query_elapsed_micros
                    <= report
                        .corpus_tnn_recall
                        .access_summary
                        .total_query_elapsed_micros
            );
            assert!(query_access.levels.iter().any(|level| level.level == 1));
            assert!(query_access.levels.iter().any(|level| level.level == 0));
            total_blocks += query_access.touched_block_count;
            total_bytes += query_access.bytes_read;
            total_rtts += query_access.estimated_rtts;
            total_elapsed_micros += query_access.actual_query_elapsed_micros;
        }
        let summary_level_blocks: usize = report
            .corpus_tnn_recall
            .access_summary
            .levels
            .iter()
            .map(|level| level.touched_block_count)
            .sum();
        let summary_level_bytes: usize = report
            .corpus_tnn_recall
            .access_summary
            .levels
            .iter()
            .map(|level| level.bytes_read)
            .sum();
        let summary_level_rtts: usize = report
            .corpus_tnn_recall
            .access_summary
            .levels
            .iter()
            .map(|level| level.estimated_rtts)
            .sum();
        assert_eq!(
            report.corpus_tnn_recall.access_summary.touched_block_count,
            summary_level_blocks
        );
        assert_eq!(
            report.corpus_tnn_recall.access_summary.bytes_read,
            summary_level_bytes
        );
        assert_eq!(
            report.corpus_tnn_recall.access_summary.estimated_rtts,
            total_rtts
        );
        assert_eq!(
            report.corpus_tnn_recall.access_summary.estimated_rtts,
            summary_level_rtts
        );
        assert_eq!(
            report
                .corpus_tnn_recall
                .access_summary
                .total_query_elapsed_micros,
            total_elapsed_micros
        );
        assert!(
            report
                .corpus_tnn_recall
                .access_summary
                .max_query_elapsed_micros
                >= report
                    .corpus_tnn_recall
                    .access_summary
                    .mean_query_elapsed_micros
        );
        assert!(report.corpus_tnn_recall.access_summary.touched_block_count <= total_blocks);
        assert!(report.corpus_tnn_recall.access_summary.bytes_read <= total_bytes);
    }

    #[test]
    fn assessment_fast_random_walk_skips_full_tree_metrics() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let alpha = put_block(&store, &named_leaf_block("alpha", &[1.0, 0.0]));
        let beta = put_block(&store, &named_leaf_block("beta", &[0.0, 1.0]));
        let root = put_block(
            &store,
            &branch_block(1, vec![([1.0, 0.0], alpha), ([0.0, 1.0], beta)]),
        );

        let report = assess_rooted_tree_with_config(
            &root,
            &store,
            TnnRecallConfig {
                sample_size: 2,
                seed: 7,
                traversal_width: 7,
                fast_random_walk: true,
            },
        )
        .unwrap();

        assert_eq!(
            report.corpus_tnn_recall.query_source,
            FAST_RANDOM_WALK_QUERY_SOURCE
        );
        assert_eq!(report.corpus_tnn_recall.corpus_size, 0);
        assert_eq!(report.corpus_tnn_recall.effective_sample_size, 2);
        assert!(report.corpus_tnn_recall.recall_at.is_empty());
        assert_eq!(report.corpus_tnn_recall.access_summary.query_count, 2);
        assert!(report.layers.is_empty());
        assert!(report.splits.is_empty());
        assert!(report.blocks.is_empty());
        assert!(report.findings.is_empty());

        let rendered = render_report_summary(&report);
        assert!(rendered.contains("Fast random-walk mode: skipped full-tree structural analysis"));
        assert!(rendered.contains("TNN recall metrics skipped in fast random-walk mode."));
        assert!(rendered.contains("Corpus query access [random-walk-sampled]:"));
        assert!(!rendered.contains("Layer statistics:"));
        assert!(!rendered.contains("Per-parent split effectiveness:"));
        assert!(!rendered.contains("Per-block statistics:"));
    }

    #[test]
    fn assessment_summarizes_query_accesses_as_unique_blocks_and_per_query_rtts() {
        let query_accesses = vec![
            build_query_access_capture(
                "query-a",
                HashMap::from([
                    (
                        BlockHash::from_bytes([1u8; BlockHash::LEN]),
                        QueryTouchedBlock {
                            level: 1,
                            bytes_read: 64_000,
                        },
                    ),
                    (
                        BlockHash::from_bytes([2u8; BlockHash::LEN]),
                        QueryTouchedBlock {
                            level: 0,
                            bytes_read: 1_000,
                        },
                    ),
                ]),
                1_500,
            ),
            build_query_access_capture(
                "query-b",
                HashMap::from([
                    (
                        BlockHash::from_bytes([1u8; BlockHash::LEN]),
                        QueryTouchedBlock {
                            level: 1,
                            bytes_read: 64_000,
                        },
                    ),
                    (
                        BlockHash::from_bytes([3u8; BlockHash::LEN]),
                        QueryTouchedBlock {
                            level: 0,
                            bytes_read: 1_000,
                        },
                    ),
                ]),
                2_500,
            ),
        ];

        let summary = summarize_query_accesses(&query_accesses);

        assert_eq!(summary.query_count, 2);
        assert_eq!(summary.touched_block_count, 3);
        assert_eq!(summary.bytes_read, 66_000);
        assert_eq!(summary.estimated_rtts, 4);
        assert_eq!(summary.total_query_elapsed_micros, 4_000);
        assert_eq!(summary.mean_query_elapsed_micros, 2_000);
        assert_eq!(summary.max_query_elapsed_micros, 2_500);
        assert_eq!(summary.levels.len(), 2);

        let root_level = summary
            .levels
            .iter()
            .find(|level| level.level == 1)
            .unwrap();
        assert_eq!(root_level.touched_block_count, 1);
        assert_eq!(root_level.bytes_read, 64_000);
        assert_eq!(root_level.estimated_rtts, 2);

        let leaf_level = summary
            .levels
            .iter()
            .find(|level| level.level == 0)
            .unwrap();
        assert_eq!(leaf_level.touched_block_count, 2);
        assert_eq!(leaf_level.bytes_read, 2_000);
        assert_eq!(leaf_level.estimated_rtts, 2);
    }

    #[test]
    fn assessment_uses_upstream_reconstruction_for_ebcp_branch_embeddings() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let alpha = put_block(&store, &named_leaf_block("alpha", &[1.0, 0.0]));
        let beta = put_block(&store, &named_leaf_block("beta", &[0.0, 1.0]));
        let root = put_block(
            &store,
            &ebcp_branch_block(1, vec![([1.0, 0.0], alpha), ([0.0, 1.0], beta)]),
        );

        let report = assess_rooted_tree_with_config(
            &root,
            &store,
            TnnRecallConfig {
                sample_size: 2,
                seed: 7,
                traversal_width: 7,
                fast_random_walk: false,
            },
        )
        .unwrap();

        assert_eq!(report.summary.structural_finding_count, 0);
        assert_eq!(report.corpus_tnn_recall.effective_sample_size, 2);
        for metric in &report.corpus_tnn_recall.recall_at {
            assert_eq!(metric.mean_recall, 1.0);
            assert_eq!(metric.stdev_recall, 0.0);
        }
    }

    #[test]
    fn assessment_zeroes_tnn_recall_when_embedding_specs_mismatch() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let matching = put_block(&store, &named_leaf_block("matching", &[1.0, 0.0]));
        let mismatched = put_block(
            &store,
            &named_leaf_block_with_dims("mismatched", &[0.0, 1.0, 0.0]),
        );
        let root = put_block(
            &store,
            &branch_block(1, vec![([1.0, 0.0], matching), ([0.0, 1.0], mismatched)]),
        );

        let report = assess_rooted_tree_with_config(
            &root,
            &store,
            TnnRecallConfig {
                sample_size: 2,
                seed: 7,
                traversal_width: 3,
                fast_random_walk: false,
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

        let alpha = put_block(&store, &named_leaf_block("alpha", &[1.0, 0.0]));
        let beta = put_block(&store, &named_leaf_block("beta", &[0.0, 1.0]));
        let zero = put_block(&store, &named_leaf_block("zero", &[0.0, 0.0]));
        let root = put_block(
            &store,
            &branch_block(
                1,
                vec![([1.0, 0.0], alpha), ([0.0, 1.0], beta), ([0.0, 0.0], zero)],
            ),
        );

        let report = assess_rooted_tree_with_config(
            &root,
            &store,
            TnnRecallConfig {
                sample_size: 3,
                seed: 7,
                traversal_width: 3,
                fast_random_walk: false,
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
        let root = put_block(&store, &leaf_block(0, &[1.0, 0.0]));

        let error = assess_rooted_tree_with_config(
            &root,
            &store,
            TnnRecallConfig {
                sample_size: 1,
                seed: 0,
                traversal_width: 0,
                fast_random_walk: false,
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TreeQualityError::InvalidTnnRecallTraversalWidth
        ));
    }

    #[test]
    fn assessment_writes_json_artifact() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();
        let root = put_block(&store, &leaf_block(0, &[1.0, 0.0]));

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
        assert!(rendered.contains("\"access_summary\""));
        assert!(rendered.contains("\"query_accesses\""));
        assert!(rendered.contains("\"layers\""));
        assert!(rendered.contains("\"splits\""));
        assert!(rendered.contains("\"occupancies\""));
        assert!(!rendered.contains("\"centroid\""));
    }

    #[test]
    fn assessment_reports_invalid_embedding_length() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();
        let root = put_block(
            &store,
            &Block::Leaf(LeafBlock {
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
            }),
        );

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

    fn ebcp_branch_block(level: u64, entries: Vec<([f32; 2], BlockHash)>) -> Block {
        Block::Branch(BranchBlock {
            version: VERSION_1,
            level,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "pca-rot-f32le".into(),
            },
            entries: entries
                .into_iter()
                .map(|(embedding, child)| BranchEntry {
                    embedding: encode_f32(&embedding),
                    child,
                })
                .collect(),
            ext: Some(ebcp_extension_map(&EbcpDescriptor {
                version: 1,
                logical_embedding_spec: EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                base_centroid: None,
                rotation: Some(EbcpRotation {
                    matrix_format: "f32le-row-major".into(),
                    matrix: vec![1.0, 0.0, 0.0, 1.0],
                }),
                quantization: None,
            })),
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
