// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::future::Future;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ciborium::Value;
use futures::stream::{FuturesUnordered, StreamExt};
use lexongraph_block::{BlockHash, DecodedBlock, deserialize_versioned_block, v2};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::custom_blocks::{
    REPLAY_JOURNAL_BLOCK_TYPE, REPLAY_JOURNAL_MEDIA_TYPE, custom_block_payload,
};
use crate::mailbox::{NORMALIZED_EMAIL_ARTIFACT_BLOCK_TYPE, NORMALIZED_EMAIL_MEDIA_TYPE};
use crate::tree_tools::parse_block_hash;

pub const DEFAULT_MAX_IN_FLIGHT_DESTINATION_WRITES: usize = 64;
const DEFAULT_MAX_IN_FLIGHT_DESTINATION_WRITE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CopyFailureOperation {
    ReadSourceBlock,
    DecodeSourceBlock,
    CheckDestinationBlock,
    WriteDestinationBlock,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct CopyFailure {
    pub root_id: String,
    pub block_id: String,
    pub operation: CopyFailureOperation,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct RootedBlockCopyReport {
    pub destination_mode: CopyDestinationMode,
    pub requested_root_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copied_block_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_already_present_block_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempted_write_block_count: Option<usize>,
    pub failed_block_count: usize,
    pub failures: Vec<CopyFailure>,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CopyDestinationMode {
    ReadBeforeWrite,
    BlindWrite,
}

#[derive(Clone, Debug)]
pub struct RootedBlockCopyProgress {
    destination_mode: CopyDestinationMode,
    state: Arc<CopyProgressState>,
}

#[derive(Debug)]
struct CopyProgressState {
    read_source_block_count: AtomicUsize,
    copied_block_count: AtomicUsize,
    skipped_already_present_block_count: AtomicUsize,
    attempted_write_block_count: AtomicUsize,
    failed_block_count: AtomicUsize,
}

struct CopyMetrics {
    copied_block_count: usize,
    skipped_already_present_block_count: usize,
    attempted_write_block_count: usize,
    progress: Option<RootedBlockCopyProgress>,
}

impl RootedBlockCopyProgress {
    pub fn new(destination_mode: CopyDestinationMode) -> Self {
        Self {
            destination_mode,
            state: Arc::new(CopyProgressState::default()),
        }
    }

    pub fn snapshot(&self) -> RootedBlockCopyProgressSnapshot {
        RootedBlockCopyProgressSnapshot {
            destination_mode: self.destination_mode,
            read_source_block_count: self.state.read_source_block_count.load(Ordering::Relaxed),
            copied_block_count: matches!(
                self.destination_mode,
                CopyDestinationMode::ReadBeforeWrite
            )
            .then(|| self.state.copied_block_count.load(Ordering::Relaxed)),
            skipped_already_present_block_count: matches!(
                self.destination_mode,
                CopyDestinationMode::ReadBeforeWrite
            )
            .then(|| {
                self.state
                    .skipped_already_present_block_count
                    .load(Ordering::Relaxed)
            }),
            attempted_write_block_count: matches!(
                self.destination_mode,
                CopyDestinationMode::BlindWrite
            )
            .then(|| {
                self.state
                    .attempted_write_block_count
                    .load(Ordering::Relaxed)
            }),
            failed_block_count: self.state.failed_block_count.load(Ordering::Relaxed),
        }
    }

    fn note_read_source_block(&self) {
        self.state
            .read_source_block_count
            .fetch_add(1, Ordering::Relaxed);
    }

    fn note_copied_block(&self) {
        self.state
            .copied_block_count
            .fetch_add(1, Ordering::Relaxed);
    }

    fn note_skipped_already_present_block(&self) {
        self.state
            .skipped_already_present_block_count
            .fetch_add(1, Ordering::Relaxed);
    }

    fn note_attempted_write_block(&self) {
        self.state
            .attempted_write_block_count
            .fetch_add(1, Ordering::Relaxed);
    }

    fn note_failed_block(&self) {
        self.state
            .failed_block_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for CopyProgressState {
    fn default() -> Self {
        Self {
            read_source_block_count: AtomicUsize::new(0),
            copied_block_count: AtomicUsize::new(0),
            skipped_already_present_block_count: AtomicUsize::new(0),
            attempted_write_block_count: AtomicUsize::new(0),
            failed_block_count: AtomicUsize::new(0),
        }
    }
}

impl CopyMetrics {
    fn new(progress: Option<RootedBlockCopyProgress>) -> Self {
        Self {
            copied_block_count: 0,
            skipped_already_present_block_count: 0,
            attempted_write_block_count: 0,
            progress,
        }
    }

    fn progress(&self) -> Option<&RootedBlockCopyProgress> {
        self.progress.as_ref()
    }

    fn note_copied_block(&mut self) {
        self.copied_block_count += 1;
        if let Some(progress) = self.progress() {
            progress.note_copied_block();
        }
    }

    fn note_skipped_already_present_block(&mut self) {
        self.skipped_already_present_block_count += 1;
        if let Some(progress) = self.progress() {
            progress.note_skipped_already_present_block();
        }
    }

    fn note_attempted_write_block(&mut self) {
        self.attempted_write_block_count += 1;
        if let Some(progress) = self.progress() {
            progress.note_attempted_write_block();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RootedBlockCopyProgressSnapshot {
    pub destination_mode: CopyDestinationMode,
    pub read_source_block_count: usize,
    pub copied_block_count: Option<usize>,
    pub skipped_already_present_block_count: Option<usize>,
    pub attempted_write_block_count: Option<usize>,
    pub failed_block_count: usize,
}

#[derive(Debug, Error)]
pub enum RootedBlockCopyError {
    #[error("failed to render rooted block-copy report: {message}")]
    Render { message: String },
    #[error("failed to write rooted block-copy report {path}: {source}")]
    WriteArtifact {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

pub async fn copy_rooted_blocks(
    source: &dyn BlockStore,
    destination: &dyn BlockStore,
    root_ids: &[BlockHash],
) -> RootedBlockCopyReport {
    copy_rooted_blocks_with_mode_and_limit(
        source,
        destination,
        root_ids,
        CopyDestinationMode::ReadBeforeWrite,
        DEFAULT_MAX_IN_FLIGHT_DESTINATION_WRITES,
    )
    .await
}

pub async fn copy_rooted_blocks_with_mode(
    source: &dyn BlockStore,
    destination: &dyn BlockStore,
    root_ids: &[BlockHash],
    destination_mode: CopyDestinationMode,
) -> RootedBlockCopyReport {
    copy_rooted_blocks_with_mode_and_limit(
        source,
        destination,
        root_ids,
        destination_mode,
        DEFAULT_MAX_IN_FLIGHT_DESTINATION_WRITES,
    )
    .await
}

pub async fn copy_rooted_blocks_with_mode_and_limit(
    source: &dyn BlockStore,
    destination: &dyn BlockStore,
    root_ids: &[BlockHash],
    destination_mode: CopyDestinationMode,
    max_in_flight_destination_writes: usize,
) -> RootedBlockCopyReport {
    copy_rooted_blocks_with_mode_and_limit_and_progress(
        source,
        destination,
        root_ids,
        destination_mode,
        max_in_flight_destination_writes,
        None,
    )
    .await
}

pub async fn copy_rooted_blocks_with_mode_and_limit_and_progress(
    source: &dyn BlockStore,
    destination: &dyn BlockStore,
    root_ids: &[BlockHash],
    destination_mode: CopyDestinationMode,
    max_in_flight_destination_writes: usize,
    progress: Option<RootedBlockCopyProgress>,
) -> RootedBlockCopyReport {
    copy_rooted_blocks_with_mode_and_limits(
        source,
        destination,
        root_ids,
        destination_mode,
        max_in_flight_destination_writes,
        DEFAULT_MAX_IN_FLIGHT_DESTINATION_WRITE_BYTES,
        progress,
    )
    .await
}

async fn copy_rooted_blocks_with_mode_and_limits(
    source: &dyn BlockStore,
    destination: &dyn BlockStore,
    root_ids: &[BlockHash],
    destination_mode: CopyDestinationMode,
    max_in_flight_destination_writes: usize,
    max_in_flight_destination_write_bytes: usize,
    progress: Option<RootedBlockCopyProgress>,
) -> RootedBlockCopyReport {
    let requested_root_ids = root_ids.iter().map(ToString::to_string).collect::<Vec<_>>();
    let mut queue = root_ids
        .iter()
        .copied()
        .map(|root_id| (root_id, root_id))
        .collect::<VecDeque<_>>();
    let mut visited = HashSet::new();
    let effective_write_limit = max_in_flight_destination_writes.max(1);
    let mut metrics = CopyMetrics::new(progress.clone());
    let mut failures = CopyFailureTracker::new(progress.clone());
    let mut pending_writes = FuturesUnordered::<PendingDestinationWrite<'_>>::new();
    let mut in_flight_destination_write_bytes = 0usize;

    while let Some((request_root_id, block_id)) = queue.pop_front() {
        failures.note_block_root(request_root_id, block_id);
        if !visited.insert(block_id) {
            continue;
        }

        let Some(block_bytes) =
            read_source_block(source, block_id, &mut failures, metrics.progress()).await
        else {
            continue;
        };
        let child_ids = decode_source_block(block_id, &block_bytes, &mut failures);
        failures.remember_children(block_id, &child_ids);

        match destination_mode {
            CopyDestinationMode::ReadBeforeWrite => {
                match destination.get_block_bytes(&block_id).await {
                    Ok(Some(_)) => {
                        metrics.note_skipped_already_present_block();
                    }
                    Ok(None) => {
                        let block_bytes_len = block_bytes.len();
                        wait_for_write_capacity(
                            &mut pending_writes,
                            effective_write_limit,
                            max_in_flight_destination_write_bytes,
                            block_bytes_len,
                            &mut in_flight_destination_write_bytes,
                            &mut metrics,
                            &mut failures,
                        )
                        .await;
                        enqueue_destination_write(
                            &mut pending_writes,
                            destination,
                            block_id,
                            block_bytes,
                            true,
                        );
                        in_flight_destination_write_bytes += block_bytes_len;
                    }
                    Err(error) => failures.record(
                        block_id,
                        CopyFailureOperation::CheckDestinationBlock,
                        error.to_string(),
                    ),
                }
            }
            CopyDestinationMode::BlindWrite => {
                metrics.note_attempted_write_block();
                let block_bytes_len = block_bytes.len();
                wait_for_write_capacity(
                    &mut pending_writes,
                    effective_write_limit,
                    max_in_flight_destination_write_bytes,
                    block_bytes_len,
                    &mut in_flight_destination_write_bytes,
                    &mut metrics,
                    &mut failures,
                )
                .await;
                enqueue_destination_write(
                    &mut pending_writes,
                    destination,
                    block_id,
                    block_bytes,
                    false,
                );
                in_flight_destination_write_bytes += block_bytes_len;
            }
        }

        enqueue_children(request_root_id, &child_ids, &mut queue);
    }

    while let Some(completion) = pending_writes.next().await {
        record_write_completion(
            completion,
            &mut in_flight_destination_write_bytes,
            &mut metrics,
            &mut failures,
        );
    }

    RootedBlockCopyReport {
        destination_mode,
        requested_root_ids,
        copied_block_count: matches!(destination_mode, CopyDestinationMode::ReadBeforeWrite)
            .then_some(metrics.copied_block_count),
        skipped_already_present_block_count: matches!(
            destination_mode,
            CopyDestinationMode::ReadBeforeWrite
        )
        .then_some(metrics.skipped_already_present_block_count),
        attempted_write_block_count: matches!(destination_mode, CopyDestinationMode::BlindWrite)
            .then_some(metrics.attempted_write_block_count),
        failed_block_count: count_failed_blocks(failures.failures()),
        failures: failures.into_failures(),
    }
}

#[derive(Clone)]
struct FailureTemplate {
    operation: CopyFailureOperation,
    message: String,
}

#[derive(Default)]
struct CopyFailureTracker {
    block_roots: HashMap<BlockHash, HashSet<BlockHash>>,
    discovered_children: HashMap<BlockHash, Vec<BlockHash>>,
    failure_templates: HashMap<BlockHash, Vec<FailureTemplate>>,
    failed_block_ids: HashSet<BlockHash>,
    failures: Vec<CopyFailure>,
    progress: Option<RootedBlockCopyProgress>,
}

impl CopyFailureTracker {
    fn new(progress: Option<RootedBlockCopyProgress>) -> Self {
        Self {
            block_roots: HashMap::new(),
            discovered_children: HashMap::new(),
            failure_templates: HashMap::new(),
            failed_block_ids: HashSet::new(),
            failures: Vec::new(),
            progress,
        }
    }

    fn note_block_root(&mut self, request_root_id: BlockHash, block_id: BlockHash) {
        self.associate_root_with_known_subgraph(request_root_id, block_id);
    }

    fn remember_children(&mut self, block_id: BlockHash, child_ids: &[BlockHash]) {
        self.discovered_children
            .insert(block_id, child_ids.to_vec());
        let Some(root_ids) = self.block_roots.get(&block_id).cloned() else {
            return;
        };
        for root_id in root_ids {
            for child_id in child_ids {
                self.associate_root_with_known_subgraph(root_id, *child_id);
            }
        }
    }

    fn record(
        &mut self,
        block_id: BlockHash,
        operation: CopyFailureOperation,
        message: impl Into<String>,
    ) {
        let message = message.into();
        if self.failed_block_ids.insert(block_id)
            && let Some(progress) = self.progress.as_ref()
        {
            progress.note_failed_block();
        }
        self.failure_templates
            .entry(block_id)
            .or_default()
            .push(FailureTemplate {
                operation,
                message: message.clone(),
            });
        if let Some(root_ids) = self.block_roots.get(&block_id) {
            for root_id in root_ids {
                self.failures.push(CopyFailure {
                    root_id: root_id.to_string(),
                    block_id: block_id.to_string(),
                    operation,
                    message: message.clone(),
                });
            }
        }
    }

    fn failures(&self) -> &[CopyFailure] {
        &self.failures
    }

    fn into_failures(self) -> Vec<CopyFailure> {
        self.failures
    }

    fn associate_root_with_known_subgraph(
        &mut self,
        request_root_id: BlockHash,
        block_id: BlockHash,
    ) {
        let mut queue = VecDeque::from([block_id]);
        while let Some(current_block_id) = queue.pop_front() {
            let associated_roots = self.block_roots.entry(current_block_id).or_default();
            if !associated_roots.insert(request_root_id) {
                continue;
            }
            if let Some(templates) = self.failure_templates.get(&current_block_id) {
                for template in templates {
                    self.failures.push(CopyFailure {
                        root_id: request_root_id.to_string(),
                        block_id: current_block_id.to_string(),
                        operation: template.operation,
                        message: template.message.clone(),
                    });
                }
            }
            if let Some(child_ids) = self.discovered_children.get(&current_block_id) {
                queue.extend(child_ids.iter().copied());
            }
        }
    }
}

type PendingDestinationWrite<'a> = Pin<Box<dyn Future<Output = DestinationWriteCompletion> + 'a>>;

struct DestinationWriteCompletion {
    block_id: BlockHash,
    block_bytes_len: usize,
    count_as_copied: bool,
    result: Result<(), BlockStoreError>,
}

fn enqueue_destination_write<'a>(
    pending_writes: &mut FuturesUnordered<PendingDestinationWrite<'a>>,
    destination: &'a dyn BlockStore,
    block_id: BlockHash,
    block_bytes: Vec<u8>,
    count_as_copied: bool,
) {
    pending_writes.push(Box::pin(async move {
        let block_bytes_len = block_bytes.len();
        let result = destination.put_block_bytes(&block_id, &block_bytes).await;
        DestinationWriteCompletion {
            block_id,
            block_bytes_len,
            count_as_copied,
            result,
        }
    }));
}

async fn wait_for_write_capacity(
    pending_writes: &mut FuturesUnordered<PendingDestinationWrite<'_>>,
    effective_write_limit: usize,
    max_in_flight_destination_write_bytes: usize,
    next_write_bytes: usize,
    in_flight_destination_write_bytes: &mut usize,
    metrics: &mut CopyMetrics,
    failures: &mut CopyFailureTracker,
) {
    while pending_writes.len() >= effective_write_limit
        || (!pending_writes.is_empty()
            && max_in_flight_destination_write_bytes > 0
            && in_flight_destination_write_bytes.saturating_add(next_write_bytes)
                > max_in_flight_destination_write_bytes)
    {
        let completion = pending_writes
            .next()
            .await
            .expect("pending destination writes should complete");
        record_write_completion(
            completion,
            in_flight_destination_write_bytes,
            metrics,
            failures,
        );
    }
}

fn record_write_completion(
    completion: DestinationWriteCompletion,
    in_flight_destination_write_bytes: &mut usize,
    metrics: &mut CopyMetrics,
    failures: &mut CopyFailureTracker,
) {
    *in_flight_destination_write_bytes =
        in_flight_destination_write_bytes.saturating_sub(completion.block_bytes_len);
    match completion.result {
        Ok(()) => {
            if completion.count_as_copied {
                metrics.note_copied_block();
            }
        }
        Err(error) => failures.record(
            completion.block_id,
            CopyFailureOperation::WriteDestinationBlock,
            error.to_string(),
        ),
    }
}

fn count_failed_blocks(failures: &[CopyFailure]) -> usize {
    failures
        .iter()
        .map(|failure| failure.block_id.as_str())
        .collect::<HashSet<_>>()
        .len()
}

pub fn default_report_path(root_ids: &[BlockHash]) -> PathBuf {
    if root_ids.is_empty() {
        return PathBuf::from("rooted-copy-empty.json");
    }

    let joined_root_ids = root_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let digest = Sha256::digest(joined_root_ids.as_bytes());
    let first_root = root_ids[0].to_string();
    PathBuf::from(format!(
        "rooted-copy-{}-{:02x}{:02x}{:02x}{:02x}.json",
        &first_root[..8],
        digest[0],
        digest[1],
        digest[2],
        digest[3]
    ))
}

pub fn write_report(
    path: &Path,
    report: &RootedBlockCopyReport,
) -> Result<(), RootedBlockCopyError> {
    let rendered =
        serde_json::to_vec_pretty(report).map_err(|error| RootedBlockCopyError::Render {
            message: error.to_string(),
        })?;
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| RootedBlockCopyError::WriteArtifact {
            path: path.display().to_string(),
            source,
        })?;
    }
    fs::write(path, rendered).map_err(|source| RootedBlockCopyError::WriteArtifact {
        path: path.display().to_string(),
        source,
    })
}

pub fn render_report_summary(report: &RootedBlockCopyReport) -> String {
    let mut lines = vec![
        format!(
            "Rooted block copy results for {} requested root(s)",
            report.requested_root_ids.len()
        ),
        format!(
            "Destination mode: {}",
            match report.destination_mode {
                CopyDestinationMode::ReadBeforeWrite => "read-before-write",
                CopyDestinationMode::BlindWrite => "blind-write",
            }
        ),
        format!("Failed blocks: {}", report.failed_block_count),
    ];
    if let Some(copied_block_count) = report.copied_block_count {
        lines.push(format!("Copied blocks: {copied_block_count}"));
    }
    if let Some(skipped_already_present_block_count) = report.skipped_already_present_block_count {
        lines.push(format!(
            "Skipped already present: {skipped_already_present_block_count}"
        ));
    }
    if let Some(attempted_write_block_count) = report.attempted_write_block_count {
        lines.push(format!("Attempted writes: {attempted_write_block_count}"));
    }
    if !report.requested_root_ids.is_empty() {
        lines.push(format!("Roots: {}", report.requested_root_ids.join(", ")));
    }
    for failure in &report.failures {
        lines.push(format!(
            "FAIL [{}] root {} block {}: {}",
            failure_operation_label(failure.operation),
            failure.root_id,
            failure.block_id,
            failure.message
        ));
    }
    lines.join("\n")
}

async fn read_source_block(
    source: &dyn BlockStore,
    block_id: BlockHash,
    failures: &mut CopyFailureTracker,
    progress: Option<&RootedBlockCopyProgress>,
) -> Option<Vec<u8>> {
    match source.get_block_bytes(&block_id).await {
        Ok(Some(bytes)) => {
            if let Some(progress) = progress {
                progress.note_read_source_block();
            }
            Some(bytes)
        }
        Ok(None) => {
            failures.record(
                block_id,
                CopyFailureOperation::ReadSourceBlock,
                "source block was not found",
            );
            None
        }
        Err(error) => {
            failures.record(
                block_id,
                CopyFailureOperation::ReadSourceBlock,
                error.to_string(),
            );
            None
        }
    }
}

fn decode_source_block(
    block_id: BlockHash,
    block_bytes: &[u8],
    failures: &mut CopyFailureTracker,
) -> Vec<BlockHash> {
    match deserialize_versioned_block(block_bytes, &block_id) {
        Ok(DecodedBlock::V1(validated)) => match validated.block {
            lexongraph_block::Block::Branch(branch) => branch
                .entries
                .into_iter()
                .map(|entry| entry.child)
                .collect(),
            lexongraph_block::Block::Leaf(leaf) => match leaf_entry_child_ids(&leaf.entries) {
                Ok(child_ids) => child_ids,
                Err(message) => {
                    failures.record(block_id, CopyFailureOperation::DecodeSourceBlock, message);
                    Vec::new()
                }
            },
        },
        Ok(DecodedBlock::V2(validated)) => match v2::into_typed_block(validated) {
            Ok(v2::TypedBlock::Branch(branch)) => branch
                .entries
                .into_iter()
                .map(|entry| entry.child)
                .collect(),
            Ok(v2::TypedBlock::Leaf(leaf)) => match leaf_entry_child_ids(&leaf.entries) {
                Ok(child_ids) => child_ids,
                Err(message) => {
                    failures.record(block_id, CopyFailureOperation::DecodeSourceBlock, message);
                    Vec::new()
                }
            },
            Ok(v2::TypedBlock::Custom(custom)) => match custom_block_child_ids(&custom) {
                Ok(child_ids) => child_ids,
                Err(message) => {
                    failures.record(block_id, CopyFailureOperation::DecodeSourceBlock, message);
                    Vec::new()
                }
            },
            Err(error) => {
                failures.record(
                    block_id,
                    CopyFailureOperation::DecodeSourceBlock,
                    error.to_string(),
                );
                Vec::new()
            }
        },
        Err(error) => {
            failures.record(
                block_id,
                CopyFailureOperation::DecodeSourceBlock,
                error.to_string(),
            );
            Vec::new()
        }
    }
}

fn leaf_entry_child_ids(entries: &[lexongraph_block::LeafEntry]) -> Result<Vec<BlockHash>, String> {
    let mut child_ids = Vec::new();
    for entry in entries {
        for (key, value) in &entry.metadata {
            match (key, value) {
                (Value::Text(name), Value::Text(block_id))
                    if name == "email_artifact_ref" || name == "mailbox_artifact_ref" =>
                {
                    child_ids.push(parse_block_hash_text(block_id)?);
                }
                _ => {}
            }
        }
    }
    Ok(child_ids)
}

fn custom_block_child_ids(custom: &v2::CustomBlock) -> Result<Vec<BlockHash>, String> {
    if custom.type_name == REPLAY_JOURNAL_BLOCK_TYPE {
        return replay_journal_child_ids(custom);
    }
    if custom.type_name == NORMALIZED_EMAIL_ARTIFACT_BLOCK_TYPE {
        return normalized_email_artifact_child_ids(custom);
    }
    Ok(Vec::new())
}

fn replay_journal_child_ids(custom: &v2::CustomBlock) -> Result<Vec<BlockHash>, String> {
    let (media_type, body) = custom_block_payload(&custom.content)?;
    if media_type != REPLAY_JOURNAL_MEDIA_TYPE {
        return Err(format!("unexpected replay journal media_type {media_type}"));
    }

    let journal: ReplayJournalBlockBodyForCopy =
        ciborium::de::from_reader(Cursor::new(body)).map_err(|error| error.to_string())?;
    let mut child_ids = Vec::new();
    if let Some(previous_block_id) = journal.previous_block_id {
        child_ids.push(parse_block_hash_text(&previous_block_id)?);
    }
    for entry in journal.entries {
        match entry {
            ReplayJournalRecordForCopy::ReplayInput {
                block_id,
                content_ref,
            } => {
                child_ids.push(parse_block_hash_text(&block_id)?);
                if let ReplayJournalContentRefForCopy::EmailChunk {
                    email_artifact_ref, ..
                } = content_ref
                {
                    child_ids.push(parse_block_hash_text(&email_artifact_ref)?);
                }
            }
            ReplayJournalRecordForCopy::IndexingOutcome {
                input_block_ids,
                generated_block_ids,
                root_block_id,
            } => {
                for block_id in input_block_ids
                    .into_iter()
                    .chain(generated_block_ids)
                    .chain(std::iter::once(root_block_id))
                {
                    child_ids.push(parse_block_hash_text(&block_id)?);
                }
            }
        }
    }
    Ok(child_ids)
}

fn normalized_email_artifact_child_ids(custom: &v2::CustomBlock) -> Result<Vec<BlockHash>, String> {
    let (media_type, body) = custom_block_payload(&custom.content)?;
    if media_type != NORMALIZED_EMAIL_MEDIA_TYPE {
        return Err(format!(
            "unexpected normalized email media_type {media_type}"
        ));
    }
    let value: Value =
        ciborium::de::from_reader(Cursor::new(body)).map_err(|error| error.to_string())?;
    let Value::Map(fields) = value else {
        return Err("normalized email artifact body must decode to a CBOR map".into());
    };
    let mailbox_artifact_ref = fields
        .iter()
        .find_map(|(key, value)| match (key, value) {
            (Value::Text(name), Value::Text(mailbox_artifact_ref))
                if name == "mailbox_artifact_ref" =>
            {
                Some(mailbox_artifact_ref.clone())
            }
            _ => None,
        })
        .ok_or_else(|| {
            "normalized email artifact body is missing mailbox_artifact_ref".to_string()
        })?;
    Ok(vec![parse_block_hash_text(&mailbox_artifact_ref)?])
}

fn parse_block_hash_text(block_id: &str) -> Result<BlockHash, String> {
    parse_block_hash(block_id).map_err(|error| error.to_string())
}

fn enqueue_children(
    request_root_id: BlockHash,
    child_ids: &[BlockHash],
    queue: &mut VecDeque<(BlockHash, BlockHash)>,
) {
    for child_id in child_ids {
        queue.push_back((request_root_id, *child_id));
    }
}

fn failure_operation_label(operation: CopyFailureOperation) -> &'static str {
    match operation {
        CopyFailureOperation::ReadSourceBlock => "read-source-block",
        CopyFailureOperation::DecodeSourceBlock => "decode-source-block",
        CopyFailureOperation::CheckDestinationBlock => "check-destination-block",
        CopyFailureOperation::WriteDestinationBlock => "write-destination-block",
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ReplayJournalBlockBodyForCopy {
    previous_block_id: Option<String>,
    #[serde(default)]
    entries: Vec<ReplayJournalRecordForCopy>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum ReplayJournalRecordForCopy {
    ReplayInput {
        block_id: String,
        content_ref: ReplayJournalContentRefForCopy,
    },
    IndexingOutcome {
        input_block_ids: Vec<String>,
        generated_block_ids: Vec<String>,
        root_block_id: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum ReplayJournalContentRefForCopy {
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fmt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use ciborium::Value;
    use lexongraph_block::{
        Block, BranchBlock, BranchEntry, Content, EmbeddingSpec, LeafBlock, LeafEntry, VERSION_1,
        VersionedBlock, v2,
    };
    use lexongraph_block_store::{BlockIdStream, BlockStoreError, BlockStoreExt};
    use lexongraph_block_store_memory::MemoryBlockStore;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn rooted_block_copy_copies_only_reachable_blocks_and_skips_existing() {
        let source = MemoryBlockStore::new(16).unwrap();
        let destination = MemoryBlockStore::new(16).unwrap();

        let alpha = source.put(&leaf_block("alpha")).await.unwrap();
        let beta = source.put(&leaf_block("beta")).await.unwrap();
        let unreachable = source.put(&leaf_block("unreachable")).await.unwrap();
        let root = source.put(&branch_block(&[alpha, beta])).await.unwrap();

        let alpha_bytes = source.get_block_bytes(&alpha).await.unwrap().unwrap();
        destination
            .put_block_bytes(&alpha, &alpha_bytes)
            .await
            .unwrap();

        let report = copy_rooted_blocks(&source, &destination, &[root]).await;

        assert_eq!(
            report.destination_mode,
            CopyDestinationMode::ReadBeforeWrite
        );
        assert_eq!(report.copied_block_count, Some(2));
        assert_eq!(report.skipped_already_present_block_count, Some(1));
        assert_eq!(report.attempted_write_block_count, None);
        assert_eq!(report.failed_block_count, 0);
        assert!(report.failures.is_empty());
        assert!(destination.get_block_bytes(&root).await.unwrap().is_some());
        assert!(destination.get_block_bytes(&beta).await.unwrap().is_some());
        assert!(destination.get_block_bytes(&alpha).await.unwrap().is_some());
        assert!(
            destination
                .get_block_bytes(&unreachable)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn rooted_block_copy_reports_write_failures_and_continues() {
        let source = MemoryBlockStore::new(16).unwrap();
        let destination_inner = Arc::new(MemoryBlockStore::new(16).unwrap());

        let good_leaf = source.put(&leaf_block("good")).await.unwrap();
        let bad_leaf = source.put(&leaf_block("bad")).await.unwrap();
        let root = source
            .put(&branch_block(&[good_leaf, bad_leaf]))
            .await
            .unwrap();
        let destination = FailingPutStore {
            inner: destination_inner.clone(),
            blocked_puts: HashSet::from([bad_leaf]),
        };

        let report = copy_rooted_blocks(&source, &destination, &[root]).await;

        assert_eq!(
            report.destination_mode,
            CopyDestinationMode::ReadBeforeWrite
        );
        assert_eq!(report.copied_block_count, Some(2));
        assert_eq!(report.skipped_already_present_block_count, Some(0));
        assert_eq!(report.failed_block_count, 1);
        assert_eq!(report.failures[0].block_id, bad_leaf.to_string());
        assert_eq!(
            report.failures[0].operation,
            CopyFailureOperation::WriteDestinationBlock
        );
        assert!(
            destination_inner
                .get_block_bytes(&root)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            destination_inner
                .get_block_bytes(&good_leaf)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            destination_inner
                .get_block_bytes(&bad_leaf)
                .await
                .unwrap()
                .is_none()
        );
        assert!(render_report_summary(&report).contains("Failed blocks: 1"));
    }

    #[tokio::test]
    async fn rooted_block_copy_accepts_v2_custom_blocks() {
        let source = MemoryBlockStore::new(16).unwrap();
        let destination = MemoryBlockStore::new(16).unwrap();
        let root = source
            .put_versioned(&VersionedBlock::V2(
                v2::build_custom_block(
                    "lexonarchivebuilder.test-artifact",
                    Value::Bytes(vec![1, 2, 3]),
                )
                .unwrap(),
            ))
            .await
            .unwrap();

        let report = copy_rooted_blocks(&source, &destination, &[root]).await;

        assert_eq!(report.copied_block_count, Some(1));
        assert_eq!(report.skipped_already_present_block_count, Some(0));
        assert_eq!(report.failed_block_count, 0);
        assert!(destination.get_block_bytes(&root).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn rooted_block_copy_follows_replay_journal_previous_block_links() {
        let source = MemoryBlockStore::new(16).unwrap();
        let destination = MemoryBlockStore::new(16).unwrap();

        let previous = source
            .put_versioned(&replay_journal_block(None, vec![]))
            .await
            .unwrap();
        let head = source
            .put_versioned(&replay_journal_block(Some(previous.to_string()), vec![]))
            .await
            .unwrap();

        let report = copy_rooted_blocks(&source, &destination, &[head]).await;

        assert_eq!(report.copied_block_count, Some(2));
        assert_eq!(report.failed_block_count, 0);
        assert!(destination.get_block_bytes(&head).await.unwrap().is_some());
        assert!(
            destination
                .get_block_bytes(&previous)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn rooted_block_copy_follows_normalized_email_mailbox_artifact_refs() {
        let source = MemoryBlockStore::new(16).unwrap();
        let destination = MemoryBlockStore::new(16).unwrap();

        let mailbox_artifact = source
            .put_versioned(&VersionedBlock::V2(
                v2::build_custom_block(
                    "lexonarchivebuilder/mailbox-artifact",
                    Value::Map(vec![
                        (
                            Value::Text("media_type".into()),
                            Value::Text("message/rfc822".into()),
                        ),
                        (Value::Text("body".into()), Value::Bytes(vec![1, 2, 3])),
                    ]),
                )
                .unwrap(),
            ))
            .await
            .unwrap();
        let normalized_email = source
            .put_versioned(&normalized_email_artifact_block(
                &mailbox_artifact.to_string(),
            ))
            .await
            .unwrap();

        let report = copy_rooted_blocks(&source, &destination, &[normalized_email]).await;

        assert_eq!(report.copied_block_count, Some(2));
        assert_eq!(report.failed_block_count, 0);
        assert!(
            destination
                .get_block_bytes(&normalized_email)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            destination
                .get_block_bytes(&mailbox_artifact)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn rooted_block_copy_follows_leaf_email_artifact_refs() {
        let source = MemoryBlockStore::new(16).unwrap();
        let destination = MemoryBlockStore::new(16).unwrap();

        let mailbox_artifact = source
            .put_versioned(&VersionedBlock::V2(
                v2::build_custom_block(
                    "lexonarchivebuilder/mailbox-artifact",
                    Value::Map(vec![
                        (
                            Value::Text("media_type".into()),
                            Value::Text("message/rfc822".into()),
                        ),
                        (Value::Text("body".into()), Value::Bytes(vec![1, 2, 3])),
                    ]),
                )
                .unwrap(),
            ))
            .await
            .unwrap();
        let normalized_email = source
            .put_versioned(&normalized_email_artifact_block(
                &mailbox_artifact.to_string(),
            ))
            .await
            .unwrap();
        let leaf = source
            .put(&leaf_block_with_refs(
                "email",
                &[("email_artifact_ref", normalized_email.to_string())],
            ))
            .await
            .unwrap();
        let root = source.put(&branch_block(&[leaf])).await.unwrap();

        let report = copy_rooted_blocks(&source, &destination, &[root]).await;

        assert_eq!(report.copied_block_count, Some(4));
        assert_eq!(report.failed_block_count, 0);
        assert!(destination.get_block_bytes(&root).await.unwrap().is_some());
        assert!(destination.get_block_bytes(&leaf).await.unwrap().is_some());
        assert!(
            destination
                .get_block_bytes(&normalized_email)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            destination
                .get_block_bytes(&mailbox_artifact)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn rooted_block_copy_report_path_is_stable() {
        let root = BlockHash::from_bytes([3u8; BlockHash::LEN]);

        let path = default_report_path(&[root]);

        assert!(
            path.display()
                .to_string()
                .starts_with("rooted-copy-03030303-")
        );
    }

    #[test]
    fn rooted_block_copy_writes_json_artifact() {
        let dir = tempdir().unwrap();
        let report = RootedBlockCopyReport {
            destination_mode: CopyDestinationMode::ReadBeforeWrite,
            requested_root_ids: vec!["abc".into()],
            copied_block_count: Some(2),
            skipped_already_present_block_count: Some(1),
            attempted_write_block_count: None,
            failed_block_count: 1,
            failures: vec![CopyFailure {
                root_id: "abc".into(),
                block_id: "def".into(),
                operation: CopyFailureOperation::WriteDestinationBlock,
                message: "backend failure".into(),
            }],
        };

        let path = dir.path().join("rooted-copy.json");
        write_report(&path, &report).unwrap();

        let rendered = fs::read_to_string(path).unwrap();
        assert!(rendered.contains("\"requested_root_ids\""));
        assert!(rendered.contains("\"copied_block_count\": 2"));
        assert!(rendered.contains("\"failed_block_count\": 1"));
        assert!(rendered.contains("\"write-destination-block\""));
    }

    #[test]
    fn rooted_block_copy_write_report_creates_parent_directories() {
        let dir = tempdir().unwrap();
        let report = RootedBlockCopyReport {
            destination_mode: CopyDestinationMode::ReadBeforeWrite,
            requested_root_ids: vec!["abc".into()],
            copied_block_count: Some(0),
            skipped_already_present_block_count: Some(0),
            attempted_write_block_count: None,
            failed_block_count: 0,
            failures: Vec::new(),
        };

        let path = dir.path().join("nested").join("rooted-copy.json");
        write_report(&path, &report).unwrap();

        assert!(path.exists());
    }

    #[tokio::test]
    async fn rooted_block_copy_blind_write_skips_destination_reads() {
        let source = MemoryBlockStore::new(16).unwrap();
        let destination_inner = Arc::new(MemoryBlockStore::new(16).unwrap());
        let alpha = source.put(&leaf_block("alpha")).await.unwrap();
        let beta = source.put(&leaf_block("beta")).await.unwrap();
        let root = source.put(&branch_block(&[alpha, beta])).await.unwrap();
        let destination = RejectingReadStore {
            inner: Arc::clone(&destination_inner),
        };

        let report = copy_rooted_blocks_with_mode(
            &source,
            &destination,
            &[root],
            CopyDestinationMode::BlindWrite,
        )
        .await;

        assert_eq!(report.destination_mode, CopyDestinationMode::BlindWrite);
        assert_eq!(report.copied_block_count, None);
        assert_eq!(report.skipped_already_present_block_count, None);
        assert_eq!(report.attempted_write_block_count, Some(3));
        assert_eq!(report.failed_block_count, 0);
        assert!(render_report_summary(&report).contains("Attempted writes: 3"));
        assert!(!render_report_summary(&report).contains("Skipped already present:"));
        assert!(
            destination_inner
                .get_block_bytes(&root)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            destination_inner
                .get_block_bytes(&alpha)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            destination_inner
                .get_block_bytes(&beta)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn rooted_block_copy_progress_tracks_read_before_write_counts() {
        let source = MemoryBlockStore::new(16).unwrap();
        let destination = MemoryBlockStore::new(16).unwrap();

        let alpha = source.put(&leaf_block("alpha")).await.unwrap();
        let beta = source.put(&leaf_block("beta")).await.unwrap();
        let root = source.put(&branch_block(&[alpha, beta])).await.unwrap();

        let alpha_bytes = source.get_block_bytes(&alpha).await.unwrap().unwrap();
        destination
            .put_block_bytes(&alpha, &alpha_bytes)
            .await
            .unwrap();

        let progress = RootedBlockCopyProgress::new(CopyDestinationMode::ReadBeforeWrite);
        let report = copy_rooted_blocks_with_mode_and_limit_and_progress(
            &source,
            &destination,
            &[root],
            CopyDestinationMode::ReadBeforeWrite,
            DEFAULT_MAX_IN_FLIGHT_DESTINATION_WRITES,
            Some(progress.clone()),
        )
        .await;
        let snapshot = progress.snapshot();

        assert_eq!(report.copied_block_count, Some(2));
        assert_eq!(report.skipped_already_present_block_count, Some(1));
        assert_eq!(snapshot.read_source_block_count, 3);
        assert_eq!(snapshot.copied_block_count, Some(2));
        assert_eq!(snapshot.skipped_already_present_block_count, Some(1));
        assert_eq!(snapshot.attempted_write_block_count, None);
        assert_eq!(snapshot.failed_block_count, 0);
    }

    #[tokio::test]
    async fn rooted_block_copy_progress_tracks_blind_write_counts() {
        let source = MemoryBlockStore::new(16).unwrap();
        let destination_inner = Arc::new(MemoryBlockStore::new(16).unwrap());
        let alpha = source.put(&leaf_block("alpha")).await.unwrap();
        let beta = source.put(&leaf_block("beta")).await.unwrap();
        let root = source.put(&branch_block(&[alpha, beta])).await.unwrap();
        let destination = RejectingReadStore {
            inner: Arc::clone(&destination_inner),
        };

        let progress = RootedBlockCopyProgress::new(CopyDestinationMode::BlindWrite);
        let report = copy_rooted_blocks_with_mode_and_limit_and_progress(
            &source,
            &destination,
            &[root],
            CopyDestinationMode::BlindWrite,
            DEFAULT_MAX_IN_FLIGHT_DESTINATION_WRITES,
            Some(progress.clone()),
        )
        .await;
        let snapshot = progress.snapshot();

        assert_eq!(report.attempted_write_block_count, Some(3));
        assert_eq!(snapshot.read_source_block_count, 3);
        assert_eq!(snapshot.copied_block_count, None);
        assert_eq!(snapshot.skipped_already_present_block_count, None);
        assert_eq!(snapshot.attempted_write_block_count, Some(3));
        assert_eq!(snapshot.failed_block_count, 0);
    }

    #[tokio::test]
    async fn rooted_block_copy_blind_write_tolerates_preexisting_destination_blocks() {
        let source = MemoryBlockStore::new(16).unwrap();
        let destination_inner = Arc::new(MemoryBlockStore::new(16).unwrap());
        let alpha = source.put(&leaf_block("alpha")).await.unwrap();
        let beta = source.put(&leaf_block("beta")).await.unwrap();
        let root = source.put(&branch_block(&[alpha, beta])).await.unwrap();
        let existing_alpha_bytes = source.get_block_bytes(&alpha).await.unwrap().unwrap();
        destination_inner
            .put_block_bytes(&alpha, &existing_alpha_bytes)
            .await
            .unwrap();
        let destination = RejectingReadStore {
            inner: Arc::clone(&destination_inner),
        };

        let report = copy_rooted_blocks_with_mode(
            &source,
            &destination,
            &[root],
            CopyDestinationMode::BlindWrite,
        )
        .await;

        assert_eq!(report.destination_mode, CopyDestinationMode::BlindWrite);
        assert_eq!(report.attempted_write_block_count, Some(3));
        assert_eq!(report.failed_block_count, 0);
        assert!(report.failures.is_empty());
        assert!(
            destination_inner
                .get_block_bytes(&root)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            destination_inner
                .get_block_bytes(&beta)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn rooted_block_copy_honors_bounded_in_flight_destination_writes() {
        let source = Arc::new(MemoryBlockStore::new(16).unwrap());
        let alpha = source.put(&leaf_block("alpha")).await.unwrap();
        let beta = source.put(&leaf_block("beta")).await.unwrap();
        let gamma = source.put(&leaf_block("gamma")).await.unwrap();
        let root = source
            .put(&branch_block(&[alpha, beta, gamma]))
            .await
            .unwrap();
        let destination = Arc::new(BlockingPutStore::new(32, 2));

        let observer_destination = Arc::clone(&destination);
        let copy_future = async {
            copy_rooted_blocks_with_mode_and_limit(
                source.as_ref(),
                destination.as_ref(),
                &[root],
                CopyDestinationMode::ReadBeforeWrite,
                2,
            )
            .await
        };
        let observer = async move {
            tokio::time::timeout(
                Duration::from_secs(1),
                observer_destination.wait_until_max_observed(),
            )
            .await
            .unwrap();
            assert_eq!(observer_destination.max_in_flight(), 2);
            observer_destination.release_writes();
        };

        let (report, ()) = tokio::join!(copy_future, observer);

        assert_eq!(report.copied_block_count, Some(4));
        assert_eq!(report.skipped_already_present_block_count, Some(0));
        assert_eq!(report.failed_block_count, 0);
    }

    #[tokio::test]
    async fn rooted_block_copy_honors_bounded_in_flight_destination_write_bytes() {
        let source = Arc::new(MemoryBlockStore::new(16).unwrap());
        let alpha = source.put(&leaf_block("alpha")).await.unwrap();
        let beta = source.put(&leaf_block("beta")).await.unwrap();
        let root = source.put(&branch_block(&[alpha, beta])).await.unwrap();
        let root_bytes = source.get_block_bytes(&root).await.unwrap().unwrap();
        let alpha_bytes = source.get_block_bytes(&alpha).await.unwrap().unwrap();
        let max_in_flight_destination_write_bytes = root_bytes.len() + alpha_bytes.len();
        let destination = Arc::new(SlowByteTrackingPutStore::new(
            32,
            max_in_flight_destination_write_bytes,
            Duration::from_millis(50),
        ));

        let report = copy_rooted_blocks_with_mode_and_limits(
            source.as_ref(),
            destination.as_ref(),
            &[root],
            CopyDestinationMode::ReadBeforeWrite,
            8,
            max_in_flight_destination_write_bytes,
            None,
        )
        .await;

        assert_eq!(report.copied_block_count, Some(3));
        assert_eq!(report.skipped_already_present_block_count, Some(0));
        assert_eq!(report.failed_block_count, 0);
        assert!(
            destination.max_in_flight_bytes() <= max_in_flight_destination_write_bytes,
            "observed {} in-flight bytes with cap {}",
            destination.max_in_flight_bytes(),
            max_in_flight_destination_write_bytes
        );
    }

    #[tokio::test]
    async fn rooted_block_copy_writes_decode_failures_and_reports_all_reaching_roots() {
        let source = MemoryBlockStore::new(16).unwrap();
        let destination = MemoryBlockStore::new(16).unwrap();

        let shared_bad_leaf = source.put(&leaf_block("shared-bad")).await.unwrap();
        source
            .put_block_bytes(&shared_bad_leaf, b"not-a-valid-block")
            .await
            .unwrap();
        let shared_branch = source.put(&branch_block(&[shared_bad_leaf])).await.unwrap();
        let unique_a = source.put(&leaf_block("unique-a")).await.unwrap();
        let unique_b = source.put(&leaf_block("unique-b")).await.unwrap();
        let root_a = source
            .put(&branch_block(&[shared_branch, unique_a]))
            .await
            .unwrap();
        let root_b = source
            .put(&branch_block(&[shared_branch, unique_b]))
            .await
            .unwrap();

        let report = copy_rooted_blocks(&source, &destination, &[root_a, root_b]).await;

        assert_eq!(report.failed_block_count, 1);
        assert_eq!(report.failures.len(), 2);
        assert_eq!(
            report
                .failures
                .iter()
                .filter(|failure| failure.block_id == shared_bad_leaf.to_string())
                .count(),
            2
        );
        assert_eq!(
            report
                .failures
                .iter()
                .map(|failure| failure.root_id.clone())
                .collect::<HashSet<_>>(),
            HashSet::from([root_a.to_string(), root_b.to_string()])
        );
        assert_eq!(
            destination
                .get_block_bytes(&shared_bad_leaf)
                .await
                .unwrap()
                .unwrap(),
            b"not-a-valid-block"
        );
    }

    struct FailingPutStore {
        inner: Arc<MemoryBlockStore>,
        blocked_puts: HashSet<BlockHash>,
    }

    struct RejectingReadStore {
        inner: Arc<MemoryBlockStore>,
    }

    struct BlockingPutStore {
        inner: Arc<MemoryBlockStore>,
        active_writes: AtomicUsize,
        max_in_flight: AtomicUsize,
        target_max_in_flight: usize,
        observed_target: AtomicBool,
        observed_target_notify: tokio::sync::Notify,
        release_writes_flag: AtomicBool,
        release_writes_notify: tokio::sync::Notify,
    }

    struct SlowByteTrackingPutStore {
        inner: Arc<MemoryBlockStore>,
        active_write_bytes: AtomicUsize,
        max_in_flight_bytes: AtomicUsize,
        write_delay: Duration,
    }

    impl BlockingPutStore {
        fn new(capacity: usize, target_max_in_flight: usize) -> Self {
            Self {
                inner: Arc::new(MemoryBlockStore::new(capacity).unwrap()),
                active_writes: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
                target_max_in_flight,
                observed_target: AtomicBool::new(false),
                observed_target_notify: tokio::sync::Notify::new(),
                release_writes_flag: AtomicBool::new(false),
                release_writes_notify: tokio::sync::Notify::new(),
            }
        }

        async fn wait_until_max_observed(&self) {
            if self.observed_target.load(Ordering::SeqCst) {
                return;
            }
            self.observed_target_notify.notified().await;
        }

        fn release_writes(&self) {
            self.release_writes_flag.store(true, Ordering::SeqCst);
            self.release_writes_notify.notify_waiters();
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(Ordering::SeqCst)
        }
    }

    impl SlowByteTrackingPutStore {
        fn new(
            capacity: usize,
            _max_in_flight_destination_write_bytes: usize,
            write_delay: Duration,
        ) -> Self {
            Self {
                inner: Arc::new(MemoryBlockStore::new(capacity).unwrap()),
                active_write_bytes: AtomicUsize::new(0),
                max_in_flight_bytes: AtomicUsize::new(0),
                write_delay,
            }
        }

        fn max_in_flight_bytes(&self) -> usize {
            self.max_in_flight_bytes.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl BlockStore for FailingPutStore {
        async fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            if self.blocked_puts.contains(block_id) {
                return Err(BlockStoreError::BackendFailure(
                    TestStoreError("simulated destination write failure").to_string(),
                ));
            }
            self.inner.put_block_bytes(block_id, block_bytes).await
        }

        async fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            self.inner.get_block_bytes(block_id).await
        }

        fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
            self.inner.iter_block_ids()
        }
    }

    #[async_trait]
    impl BlockStore for RejectingReadStore {
        async fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            self.inner.put_block_bytes(block_id, block_bytes).await
        }

        async fn get_block_bytes(
            &self,
            _block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            Err(BlockStoreError::BackendFailure(
                TestStoreError("destination read should not be used").to_string(),
            ))
        }

        fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
            self.inner.iter_block_ids()
        }
    }

    #[async_trait]
    impl BlockStore for BlockingPutStore {
        async fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            let active = self.active_writes.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(active, Ordering::SeqCst);
            if active >= self.target_max_in_flight
                && !self.observed_target.swap(true, Ordering::SeqCst)
            {
                self.observed_target_notify.notify_waiters();
            }
            while !self.release_writes_flag.load(Ordering::SeqCst) {
                self.release_writes_notify.notified().await;
            }
            let result = self.inner.put_block_bytes(block_id, block_bytes).await;
            self.active_writes.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            self.inner.get_block_bytes(block_id).await
        }

        fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
            self.inner.iter_block_ids()
        }
    }

    #[async_trait]
    impl BlockStore for SlowByteTrackingPutStore {
        async fn put_block_bytes(
            &self,
            block_id: &BlockHash,
            block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            let active_bytes = self
                .active_write_bytes
                .fetch_add(block_bytes.len(), Ordering::SeqCst)
                + block_bytes.len();
            self.max_in_flight_bytes
                .fetch_max(active_bytes, Ordering::SeqCst);
            tokio::time::sleep(self.write_delay).await;
            let result = self.inner.put_block_bytes(block_id, block_bytes).await;
            self.active_write_bytes
                .fetch_sub(block_bytes.len(), Ordering::SeqCst);
            result
        }

        async fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            self.inner.get_block_bytes(block_id).await
        }

        fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
            self.inner.iter_block_ids()
        }
    }

    #[derive(Debug)]
    struct TestStoreError(&'static str);

    impl fmt::Display for TestStoreError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for TestStoreError {}

    fn leaf_block(name: &str) -> Block {
        leaf_block_with_refs(name, &[])
    }

    fn leaf_block_with_refs(name: &str, refs: &[(&str, String)]) -> Block {
        let mut metadata = vec![(Value::Text("source_name".into()), Value::Text(name.into()))];
        metadata.extend(
            refs.iter()
                .map(|(key, value)| (Value::Text((*key).into()), Value::Text(value.clone()))),
        );
        Block::Leaf(LeafBlock {
            version: VERSION_1,
            level: 0,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            entries: vec![LeafEntry {
                embedding: encode_f32(&[1.0, 0.0]),
                metadata,
                content: Content {
                    media_type: "text/plain".into(),
                    body: name.as_bytes().to_vec(),
                },
            }],
            ext: None,
        })
    }

    fn branch_block(children: &[BlockHash]) -> Block {
        Block::Branch(BranchBlock {
            version: VERSION_1,
            level: 1,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            entries: children
                .iter()
                .map(|child| BranchEntry {
                    embedding: encode_f32(&[1.0, 0.0]),
                    child: *child,
                })
                .collect(),
            ext: None,
        })
    }

    fn encode_f32(values: &[f32; 2]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }

    fn replay_journal_block(
        previous_block_id: Option<String>,
        entries: Vec<ReplayJournalRecordForCopy>,
    ) -> VersionedBlock {
        let body = ReplayJournalBlockBodyForCopy {
            previous_block_id,
            entries,
        };
        let mut encoded = Vec::new();
        ciborium::ser::into_writer(&body, &mut encoded).unwrap();
        VersionedBlock::V2(
            v2::build_custom_block(
                REPLAY_JOURNAL_BLOCK_TYPE,
                Value::Map(vec![
                    (
                        Value::Text("media_type".into()),
                        Value::Text(REPLAY_JOURNAL_MEDIA_TYPE.into()),
                    ),
                    (Value::Text("body".into()), Value::Bytes(encoded)),
                ]),
            )
            .unwrap(),
        )
    }

    fn normalized_email_artifact_block(mailbox_artifact_ref: &str) -> VersionedBlock {
        let body = Value::Map(vec![(
            Value::Text("mailbox_artifact_ref".into()),
            Value::Text(mailbox_artifact_ref.into()),
        )]);
        let mut encoded = Vec::new();
        ciborium::ser::into_writer(&body, &mut encoded).unwrap();
        VersionedBlock::V2(
            v2::build_custom_block(
                NORMALIZED_EMAIL_ARTIFACT_BLOCK_TYPE,
                Value::Map(vec![
                    (
                        Value::Text("media_type".into()),
                        Value::Text(NORMALIZED_EMAIL_MEDIA_TYPE.into()),
                    ),
                    (Value::Text("body".into()), Value::Bytes(encoded)),
                ]),
            )
            .unwrap(),
        )
    }
}
