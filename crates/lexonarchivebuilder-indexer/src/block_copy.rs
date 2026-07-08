// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::collections::{HashSet, VecDeque};
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use ciborium::Value;
use lexongraph_block::{BlockHash, DecodedBlock, deserialize_versioned_block, v2};
use lexongraph_block_store::BlockStore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::mailbox::NORMALIZED_EMAIL_ARTIFACT_BLOCK_TYPE;
use crate::tree_tools::parse_block_hash;

const REPLAY_JOURNAL_BLOCK_TYPE: &str = "lexonarchivebuilder.replay-journal";
const REPLAY_JOURNAL_MEDIA_TYPE: &str = "application/vnd.lexonarchivebuilder.replay-journal+cbor";
const NORMALIZED_EMAIL_MEDIA_TYPE: &str =
    "application/vnd.lexonarchivebuilder.normalized-email+cbor";

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
    pub requested_root_ids: Vec<String>,
    pub copied_block_count: usize,
    pub skipped_already_present_block_count: usize,
    pub failed_block_count: usize,
    pub failures: Vec<CopyFailure>,
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
    let requested_root_ids = root_ids.iter().map(ToString::to_string).collect::<Vec<_>>();
    let mut queue = root_ids
        .iter()
        .copied()
        .map(|root_id| (root_id, root_id))
        .collect::<VecDeque<_>>();
    let mut visited = HashSet::new();
    let mut copied_block_count = 0usize;
    let mut skipped_already_present_block_count = 0usize;
    let mut failures = Vec::new();

    while let Some((request_root_id, block_id)) = queue.pop_front() {
        if !visited.insert(block_id) {
            continue;
        }

        let Some(block_bytes) =
            read_source_block(source, request_root_id, block_id, &mut failures).await
        else {
            continue;
        };
        let Some(child_ids) =
            decode_source_block(request_root_id, block_id, &block_bytes, &mut failures)
        else {
            continue;
        };

        match destination.get_block_bytes(&block_id).await {
            Ok(Some(_)) => {
                skipped_already_present_block_count += 1;
            }
            Ok(None) => {
                if let Err(error) = destination.put_block_bytes(&block_id, &block_bytes).await {
                    failures.push(CopyFailure {
                        root_id: request_root_id.to_string(),
                        block_id: block_id.to_string(),
                        operation: CopyFailureOperation::WriteDestinationBlock,
                        message: error.to_string(),
                    });
                } else {
                    copied_block_count += 1;
                }
            }
            Err(error) => failures.push(CopyFailure {
                root_id: request_root_id.to_string(),
                block_id: block_id.to_string(),
                operation: CopyFailureOperation::CheckDestinationBlock,
                message: error.to_string(),
            }),
        }

        enqueue_children(request_root_id, &child_ids, &mut queue);
    }

    RootedBlockCopyReport {
        requested_root_ids,
        copied_block_count,
        skipped_already_present_block_count,
        failed_block_count: failures.len(),
        failures,
    }
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
        format!("Copied blocks: {}", report.copied_block_count),
        format!(
            "Skipped already present: {}",
            report.skipped_already_present_block_count
        ),
        format!("Failed blocks: {}", report.failed_block_count),
    ];
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
    request_root_id: BlockHash,
    block_id: BlockHash,
    failures: &mut Vec<CopyFailure>,
) -> Option<Vec<u8>> {
    match source.get_block_bytes(&block_id).await {
        Ok(Some(bytes)) => Some(bytes),
        Ok(None) => {
            failures.push(CopyFailure {
                root_id: request_root_id.to_string(),
                block_id: block_id.to_string(),
                operation: CopyFailureOperation::ReadSourceBlock,
                message: "source block was not found".into(),
            });
            None
        }
        Err(error) => {
            failures.push(CopyFailure {
                root_id: request_root_id.to_string(),
                block_id: block_id.to_string(),
                operation: CopyFailureOperation::ReadSourceBlock,
                message: error.to_string(),
            });
            None
        }
    }
}

fn decode_source_block(
    request_root_id: BlockHash,
    block_id: BlockHash,
    block_bytes: &[u8],
    failures: &mut Vec<CopyFailure>,
) -> Option<Vec<BlockHash>> {
    match deserialize_versioned_block(block_bytes, &block_id) {
        Ok(DecodedBlock::V1(validated)) => match validated.block {
            lexongraph_block::Block::Branch(branch) => Some(
                branch
                    .entries
                    .into_iter()
                    .map(|entry| entry.child)
                    .collect(),
            ),
            lexongraph_block::Block::Leaf(leaf) => match leaf_entry_child_ids(&leaf.entries) {
                Ok(child_ids) => Some(child_ids),
                Err(message) => {
                    failures.push(CopyFailure {
                        root_id: request_root_id.to_string(),
                        block_id: block_id.to_string(),
                        operation: CopyFailureOperation::DecodeSourceBlock,
                        message,
                    });
                    None
                }
            },
        },
        Ok(DecodedBlock::V2(validated)) => match v2::into_typed_block(validated) {
            Ok(v2::TypedBlock::Branch(branch)) => Some(
                branch
                    .entries
                    .into_iter()
                    .map(|entry| entry.child)
                    .collect(),
            ),
            Ok(v2::TypedBlock::Leaf(leaf)) => match leaf_entry_child_ids(&leaf.entries) {
                Ok(child_ids) => Some(child_ids),
                Err(message) => {
                    failures.push(CopyFailure {
                        root_id: request_root_id.to_string(),
                        block_id: block_id.to_string(),
                        operation: CopyFailureOperation::DecodeSourceBlock,
                        message,
                    });
                    None
                }
            },
            Ok(v2::TypedBlock::Custom(custom)) => match custom_block_child_ids(&custom) {
                Ok(child_ids) => Some(child_ids),
                Err(message) => {
                    failures.push(CopyFailure {
                        root_id: request_root_id.to_string(),
                        block_id: block_id.to_string(),
                        operation: CopyFailureOperation::DecodeSourceBlock,
                        message,
                    });
                    None
                }
            },
            Err(error) => {
                failures.push(CopyFailure {
                    root_id: request_root_id.to_string(),
                    block_id: block_id.to_string(),
                    operation: CopyFailureOperation::DecodeSourceBlock,
                    message: error.to_string(),
                });
                None
            }
        },
        Err(error) => {
            failures.push(CopyFailure {
                root_id: request_root_id.to_string(),
                block_id: block_id.to_string(),
                operation: CopyFailureOperation::DecodeSourceBlock,
                message: error.to_string(),
            });
            None
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

fn custom_block_payload(content: &Value) -> Result<(String, Vec<u8>), String> {
    let Value::Map(fields) = content else {
        return Err("custom block content must be a CBOR map".into());
    };
    let media_type = fields
        .iter()
        .find_map(|(key, value)| match (key, value) {
            (Value::Text(name), Value::Text(media_type)) if name == "media_type" => {
                Some(media_type.clone())
            }
            _ => None,
        })
        .ok_or_else(|| "custom block content is missing media_type".to_string())?;
    let body = fields
        .iter()
        .find_map(|(key, value)| match (key, value) {
            (Value::Text(name), Value::Bytes(body)) if name == "body" => Some(body.clone()),
            _ => None,
        })
        .ok_or_else(|| "custom block content is missing body".to_string())?;
    Ok((media_type, body))
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

        assert_eq!(report.copied_block_count, 2);
        assert_eq!(report.skipped_already_present_block_count, 1);
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

        assert_eq!(report.copied_block_count, 2);
        assert_eq!(report.skipped_already_present_block_count, 0);
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

        assert_eq!(report.copied_block_count, 1);
        assert_eq!(report.skipped_already_present_block_count, 0);
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

        assert_eq!(report.copied_block_count, 2);
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

        assert_eq!(report.copied_block_count, 2);
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

        assert_eq!(report.copied_block_count, 4);
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
            requested_root_ids: vec!["abc".into()],
            copied_block_count: 2,
            skipped_already_present_block_count: 1,
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

    struct FailingPutStore {
        inner: Arc<MemoryBlockStore>,
        blocked_puts: HashSet<BlockHash>,
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
