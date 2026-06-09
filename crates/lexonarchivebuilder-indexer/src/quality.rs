use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use lexongraph_block::{Block, BlockHash, BranchEntry, EmbeddingSpec};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use serde::Serialize;
use thiserror::Error;

use crate::tree_tools::decode_embedding_values;

const DEFAULT_QUANTILE_BIN_COUNT: usize = 4;
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

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TreeQualityReport {
    pub root_id: String,
    pub summary: TreeQualitySummary,
    pub findings: Vec<TreeQualityFinding>,
    pub layers: Vec<LayerQualityMetrics>,
    pub splits: Vec<SplitEffectivenessMetrics>,
    pub blocks: Vec<BlockQualityMetrics>,
}

#[derive(Clone, Debug)]
struct TraversalState {
    blocks: Vec<BlockQualityMetrics>,
    findings: Vec<TreeQualityFinding>,
    metrics_by_id: HashMap<BlockHash, BlockQualityMetrics>,
    child_ids_by_parent: HashMap<BlockHash, Vec<BlockHash>>,
    visited: HashSet<BlockHash>,
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
    let Some(root) = store.get(root_id)? else {
        return Err(TreeQualityError::MissingRootBlock {
            root_id: root_id.to_string(),
        });
    };

    let mut state = TraversalState {
        blocks: Vec::new(),
        findings: Vec::new(),
        metrics_by_id: HashMap::new(),
        child_ids_by_parent: HashMap::new(),
        visited: HashSet::new(),
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

pub fn write_report(path: &Path, report: &TreeQualityReport) -> Result<(), TreeQualityError> {
    let rendered = serde_json::to_vec_pretty(report)?;
    fs::write(path, rendered).map_err(|source| TreeQualityError::WriteArtifact {
        path: path.display().to_string(),
        source,
    })
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
        "Layer statistics:".into(),
    ];

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

    state.metrics_by_id.insert(block_id, metrics.clone());
    state.blocks.push(metrics.clone());

    if let Block::Branch(branch) = block {
        for entry in &branch.entries {
            state.edge_count += 1;
            handle_child_entry(block_id, &metrics, entry, depth + 1, store, ancestry, state)?;
        }
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
    let bin_count = sample_count.clamp(1, DEFAULT_QUANTILE_BIN_COUNT);
    if sample_count == 0 {
        return QuantileOccupancyMetrics {
            bin_count,
            occupancies: vec![0; bin_count],
            occupancy_variance: 0.0,
            empty_bin_count: bin_count,
            overfull_bin_count: 0,
        };
    }
    if bin_count == 1 || l2_norm(principal_axis) <= EPSILON {
        return QuantileOccupancyMetrics {
            bin_count: 1,
            occupancies: vec![sample_count],
            occupancy_variance: 0.0,
            empty_bin_count: 0,
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
        assert!(rendered.contains("Per-parent split effectiveness:"));
        assert!(rendered.contains("Per-block statistics:"));
        assert!(rendered.contains("quantile occupancies ["));
    }

    #[test]
    fn assessment_writes_json_artifact() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();
        let root = store.put(&leaf_block(0, &[1.0, 0.0])).unwrap();

        let report = assess_rooted_tree(&root, &store).unwrap();
        let path = dir.path().join("report.json");
        write_report(&path, &report).unwrap();

        let rendered = fs::read_to_string(path).unwrap();
        assert!(rendered.contains("\"root_id\""));
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

    fn encode_f32(values: &[f32; 2]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }
}
