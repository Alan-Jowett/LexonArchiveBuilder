use std::fs;
use std::path::Path;
use std::sync::Arc;

use lexongraph_block::{
    Block, BlockError, BlockHash, EmbeddingSpec, SerializedBlock, deserialize_block,
    serialize_block,
};
use lexongraph_block_store::BlockStoreError;
use lexongraph_indexer::{
    ConstructedBlocks, IndexItem, Indexer, IndexerError, IndexingPhase, IndexingStatus,
    IndexingStatusObserver, IndexingStatusState,
};
use thiserror::Error;
use tokio::task::{JoinError, JoinSet};

use crate::block_store::ConfiguredBlockStore;
use crate::config::{BatchItemConfig, BatchRequest, BatchSummary, ConfigError, ExecutionStage};
use crate::embedding::{
    AzureOpenAiEmbeddingProviderStub, ConfiguredEmbeddingProvider, ConfiguredEmbeddingProviderError,
};
use crate::mailbox::{MailboxExpansionError, expand_mailbox_item_with_stats};
use crate::paths::resolve_path;
use crate::resolver::{ContentRef, LocalFilesystemContentResolver};

type RuntimeIndexer = Indexer<LocalFilesystemContentResolver, ConfiguredEmbeddingProvider>;
type ProgressReporter = Arc<dyn Fn(String) + Send + Sync + 'static>;

pub const INGESTION_ONLY_ROOT_ID_PLACEHOLDER: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, Default)]
struct StagedBlocks {
    block_ids: Vec<BlockHash>,
    blocks: Vec<SerializedBlock>,
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
    #[error("staged block hash mismatch: expected {expected}, store returned {actual}")]
    StagedBlockHashMismatch { expected: String, actual: String },
    #[error(transparent)]
    Indexer(#[from] IndexerError),
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
}

pub async fn run_request_file(request_path: &Path) -> Result<BatchSummary, RuntimeError> {
    run_request_file_with_stage(request_path, None).await
}

pub async fn run_request_file_with_stage(
    request_path: &Path,
    stage_override: Option<ExecutionStage>,
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

    run_request(request_dir, request).await
}

pub async fn run_request(
    request_dir: &Path,
    request: BatchRequest,
) -> Result<BatchSummary, RuntimeError> {
    run_request_with_progress(request_dir, request, |message| eprintln!("{message}")).await
}

async fn run_request_with_progress<F>(
    request_dir: &Path,
    request: BatchRequest,
    progress: F,
) -> Result<BatchSummary, RuntimeError>
where
    F: Fn(String) + Send + Sync + 'static,
{
    let progress: ProgressReporter = Arc::new(progress);
    request.validate()?;
    let stage = request.stage;
    let embedding_provider = configured_embedding_provider_for_stage(stage, &request)?;
    let block_store = ConfiguredBlockStore::from_environment(request_dir, &request.environment)?;
    let indexer = Indexer::with_defaults(LocalFilesystemContentResolver, embedding_provider);
    let embedding_spec = request.to_embedding_spec();
    let max_concurrency = request.effective_max_concurrency();
    let staged = if stage.includes_ingestion() {
        run_ingestion_stage(
            &indexer,
            request_dir,
            &request,
            &block_store,
            &embedding_spec,
            max_concurrency,
            &progress,
        )
        .await?
    } else {
        load_clusterable_blocks(&block_store, &embedding_spec, &progress)?
    };

    if !stage.includes_clustering() {
        report_progress(
            &progress,
            format!(
                "Skipped clustering and block assembly; returning placeholder root_id {}",
                placeholder_root_id()
            ),
        );
        return Ok(staged.into_summary(placeholder_root_id()));
    }

    run_clustering_stage(
        &indexer,
        staged,
        &block_store,
        &embedding_spec,
        request.block_size_target,
        &progress,
    )
}

async fn build_leaf_blocks_concurrently(
    indexer: &RuntimeIndexer,
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
    let batch_size = items.len().div_ceil(concurrency);
    let batch_count = items.len().div_ceil(batch_size);
    let mut join_set = JoinSet::new();
    for (batch_index, chunk) in items.chunks(batch_size).enumerate() {
        let indexer = indexer.clone();
        let embedding_spec = embedding_spec.clone();
        let batch_items = chunk.to_vec();
        join_set.spawn(async move {
            let constructed = indexer
                .build_leaf_blocks(&batch_items, embedding_spec)
                .await?;
            Ok::<(usize, ConstructedBlocks), IndexerError>((batch_index, constructed))
        });
    }

    let mut completed = vec![None; batch_count];
    while let Some(result) = join_set.join_next().await {
        let (batch_index, constructed) = result??;
        completed[batch_index] = Some(constructed);
    }

    let mut block_ids = Vec::with_capacity(items.len());
    let mut blocks = Vec::with_capacity(items.len());
    for constructed in completed.into_iter().flatten() {
        block_ids.extend(constructed.block_ids);
        blocks.extend(constructed.blocks);
    }

    Ok(ConstructedBlocks { block_ids, blocks })
}

fn configured_embedding_provider_for_stage(
    stage: ExecutionStage,
    request: &BatchRequest,
) -> Result<ConfiguredEmbeddingProvider, RuntimeError> {
    if stage.includes_ingestion() {
        request.environment.local_embedding()?;
        return ConfiguredEmbeddingProvider::from_environment(&request.environment)
            .map_err(RuntimeError::from);
    }

    Ok(ConfiguredEmbeddingProvider::AzureOpenAi(
        AzureOpenAiEmbeddingProviderStub,
    ))
}

async fn run_ingestion_stage(
    indexer: &RuntimeIndexer,
    request_dir: &Path,
    request: &BatchRequest,
    block_store: &dyn lexongraph_block_store::BlockStore,
    embedding_spec: &EmbeddingSpec,
    max_concurrency: usize,
    progress: &ProgressReporter,
) -> Result<StagedBlocks, RuntimeError> {
    let document_items = request.to_document_index_items(request_dir);
    let mut staged = StagedBlocks::default();

    if !document_items.is_empty() {
        report_progress(
            progress,
            format!(
                "Indexing {} document item(s) with up to {} concurrent leaf worker(s)",
                document_items.len(),
                max_concurrency
            ),
        );
        let constructed = build_leaf_blocks_concurrently(
            indexer,
            &document_items,
            embedding_spec,
            max_concurrency,
        )
        .await?;
        persist_staged_blocks(&constructed.blocks, block_store)?;
        report_progress(
            progress,
            format!(
                "Indexed {} document item(s) into {} leaf block(s)",
                document_items.len(),
                constructed.blocks.len()
            ),
        );
        staged.extend_constructed(&constructed);
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
            let constructed = build_leaf_blocks_concurrently(
                indexer,
                &expansion.items,
                embedding_spec,
                max_concurrency,
            )
            .await?;
            persist_staged_blocks(&constructed.blocks, block_store)?;
            report_progress(
                progress,
                format!(
                    "Indexed {} delegated item(s) from mailbox {} into {} leaf block(s)",
                    expansion.items.len(),
                    resolved.display(),
                    constructed.blocks.len()
                ),
            );
            staged.extend_constructed(&constructed);
        }
    }

    Ok(staged)
}

fn run_clustering_stage(
    indexer: &RuntimeIndexer,
    mut staged: StagedBlocks,
    block_store: &dyn lexongraph_block_store::BlockStore,
    embedding_spec: &EmbeddingSpec,
    block_size_target: usize,
    progress: &ProgressReporter,
) -> Result<BatchSummary, RuntimeError> {
    let observer = Some(make_status_observer(Arc::clone(progress)));
    let mut current_layer = unique_serialized_blocks_by_hash(std::mem::take(&mut staged.blocks));
    let mut layer_index = 0;

    if current_layer.is_empty() {
        return Err(RuntimeError::EmptyDelegatedOutput);
    }

    while current_layer.len() > 1 {
        let input_count = current_layer.len();
        let constructed = indexer.build_parent_blocks_with_observer(
            &current_layer,
            embedding_spec.clone(),
            block_size_target,
            layer_index,
            observer.clone(),
        )?;
        persist_staged_blocks(&constructed.blocks, block_store)?;
        let blocks_produced = constructed.blocks.len();
        let next_layer = unique_serialized_blocks_by_hash(constructed.blocks.clone());
        report_progress(
            progress,
            format!(
                "Indexed {} staged block(s) into {} parent block(s); {} staged block(s) remain",
                input_count,
                blocks_produced,
                next_layer.len()
            ),
        );
        staged.extend_constructed(&constructed);
        current_layer = next_layer;
        layer_index += 1;
    }

    let root = current_layer
        .first()
        .ok_or(RuntimeError::EmptyDelegatedOutput)?
        .hash;
    Ok(staged.into_summary(root.to_string()))
}

fn load_clusterable_blocks(
    store: &dyn lexongraph_block_store::BlockStore,
    embedding_spec: &EmbeddingSpec,
    progress: &ProgressReporter,
) -> Result<StagedBlocks, RuntimeError> {
    report_progress(
        progress,
        "Scanning the configured block store for clustering-eligible leaf blocks".to_string(),
    );

    let mut staged = StagedBlocks::default();
    for block_id in store.iter_block_ids()? {
        let block_id = block_id?;
        let Some(validated) = store.get(&block_id)? else {
            return Err(RuntimeError::MissingIteratedBlock {
                block_id: block_id.to_string(),
            });
        };
        let Some(serialized) = serialize_clusterable_block(&validated, embedding_spec)? else {
            continue;
        };
        staged.block_ids.push(validated.hash);
        staged.blocks.push(serialized);
    }

    if staged.blocks.is_empty() {
        return Err(RuntimeError::NoClusterableBlocks);
    }

    report_progress(
        progress,
        format!(
            "Loaded {} clustering-eligible leaf block(s) from the configured block store",
            staged.blocks.len()
        ),
    );
    Ok(staged)
}

fn serialize_clusterable_block(
    validated: &lexongraph_block::ValidatedBlock,
    embedding_spec: &EmbeddingSpec,
) -> Result<Option<SerializedBlock>, RuntimeError> {
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

    let serialized = serialize_block(&validated.block).map_err(|source| {
        RuntimeError::SerializeIteratedBlock {
            block_id: validated.hash.to_string(),
            source,
        }
    })?;
    if serialized.hash != validated.hash {
        return Err(RuntimeError::IteratedBlockHashMismatch {
            expected: validated.hash.to_string(),
            actual: serialized.hash.to_string(),
        });
    }
    Ok(Some(serialized))
}

fn make_status_observer(progress: ProgressReporter) -> IndexingStatusObserver {
    Arc::new(move |status| {
        report_progress(&progress, format_indexing_status(status));
    })
}

fn format_indexing_status(status: IndexingStatus) -> String {
    let elapsed_ms = status.elapsed.as_millis();
    match (status.phase, status.state) {
        (IndexingPhase::ParentLayerClustering, IndexingStatusState::Started) => format!(
            "Clustering layer {} started for {} child block(s)",
            status.layer_index, status.child_count
        ),
        (IndexingPhase::ParentLayerClustering, IndexingStatusState::InProgress) => format!(
            "Clustering layer {} still running after {} ms for {} child block(s)",
            status.layer_index, elapsed_ms, status.child_count
        ),
        (IndexingPhase::ParentLayerClustering, IndexingStatusState::Completed) => format!(
            "Clustering layer {} completed in {} ms: {} output group(s)",
            status.layer_index,
            elapsed_ms,
            status.output_count.unwrap_or_default()
        ),
        (IndexingPhase::ParentLayerClustering, IndexingStatusState::Failed) => format!(
            "Clustering layer {} failed after {} ms: {}",
            status.layer_index,
            elapsed_ms,
            status.error.unwrap_or_else(|| "unknown error".into())
        ),
        (IndexingPhase::ParentLayerMaterialization, IndexingStatusState::Completed) => format!(
            "Materialized parent layer {} in {} ms: {} block(s)",
            status.layer_index,
            elapsed_ms,
            status.output_count.unwrap_or_default()
        ),
        (IndexingPhase::ParentLayerMaterialization, IndexingStatusState::Failed) => format!(
            "Materializing parent layer {} failed after {} ms: {}",
            status.layer_index,
            elapsed_ms,
            status.error.unwrap_or_else(|| "unknown error".into())
        ),
        (IndexingPhase::ParentLayerMaterialization, IndexingStatusState::Started)
        | (IndexingPhase::ParentLayerMaterialization, IndexingStatusState::InProgress) => format!(
            "Materializing parent layer {} after {} ms",
            status.layer_index, elapsed_ms
        ),
    }
}

fn report_progress(progress: &ProgressReporter, message: String) {
    progress.as_ref()(message);
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

fn unique_serialized_blocks_by_hash(mut blocks: Vec<SerializedBlock>) -> Vec<SerializedBlock> {
    blocks.sort_by(|left, right| left.hash.as_bytes().cmp(right.hash.as_bytes()));
    blocks.dedup_by(|left, right| left.hash == right.hash);
    blocks
}

pub fn write_summary_file(path: &Path, summary: &BatchSummary) -> Result<(), RuntimeError> {
    if let Some(parent) = path.parent() {
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

    use serde_json::json;
    use tempfile::tempdir;

    use crate::config::{
        BatchItemConfig, EmbeddingSpecConfig, EnvironmentConfig, ExecutionStage,
        LocalEmbeddingConfig,
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

        let server = spawn_embedding_server(4);
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

        let first = run_request(temp.path(), request.clone()).await.unwrap();
        let stored_block_count_after_first = count_files_recursively(&temp.path().join("blocks"));
        let second = run_request(temp.path(), request).await.unwrap();
        let stored_block_count_after_second = count_files_recursively(&temp.path().join("blocks"));

        assert_eq!(first.root_id, second.root_id);
        assert_eq!(first.block_ids, second.block_ids);
        assert_eq!(
            stored_block_count_after_first,
            stored_block_count_after_second
        );
        assert!(stored_block_count_after_second > first.block_count);
        server.join();
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
        let summary = run_request_with_progress(temp.path(), request, move |message| {
            progress_capture.lock().unwrap().push(message);
        })
        .await
        .unwrap();
        let progress = progress.lock().unwrap();

        assert!(!summary.block_ids.is_empty());
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Indexing 1 document item(s)"))
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
                .any(|line| line.contains("Indexed 1 delegated item(s) from mailbox"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Clustering layer 0 started"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("Materialized parent layer 0"))
        );
        assert!(
            progress
                .iter()
                .any(|line| line.contains("parent block(s); 1 staged block(s) remain"))
        );
        server.join();
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

    fn spawn_embedding_server_with_delay(
        expected_requests: usize,
        response_delay: Duration,
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
                    let body = r#"{"data":[{"embedding":[0.25,0.75]}]}"#;
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
