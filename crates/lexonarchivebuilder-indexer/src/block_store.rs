// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lexonarchivebuilder_block_store_http3::Http3BlockStore;
use lexongraph_block::BlockHash;
use lexongraph_block_store::{
    BlockBytesBatchEntry, BlockIdStream, BlockStore, BlockStoreError, BlockStoreTelemetryCallback,
    BlockStoreTelemetryEvent,
};
use lexongraph_block_store_azure_sdk::AzureBlobBlockStore;
use lexongraph_block_store_azure_table_v2::AzureTableBlockStoreV2;
use lexongraph_block_store_fs::FilesystemBlockStore;
use lexongraph_block_store_memory::MemoryBlockStore;
use lexongraph_block_store_overlay::{OverlayBlockStore, OverlayStoreLayer, PassiveLayer};
use lexongraph_block_store_redb::{RedbBlockStore, RedbBlockStoreDurabilityMode};

use crate::config::{EnvironmentConfig, ProductionBlockStoreConfig};
use crate::paths::resolve_path;

const REDB_DATABASE_FILE_NAME: &str = "blocks.redb";
const REDB_PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

pub type OperatorProgressReporter = Arc<dyn Fn(String) + Send + Sync + 'static>;

#[derive(Clone, Debug)]
pub enum ConfiguredBlockStore {
    GatewayHttp3(Http3BlockStore),
    Local(FilesystemBlockStore),
    LocalRedb {
        store: RedbBlockStore,
        database_path: PathBuf,
    },
    Overlay(Arc<OverlayBlockStore>),
    AzureTable(AzureTableBlockStoreV2),
}

impl ConfiguredBlockStore {
    pub fn gateway_http3_store(gateway_dns_name: &str) -> Result<Self, BlockStoreError> {
        Http3BlockStore::new(gateway_dns_name).map(Self::GatewayHttp3)
    }

    pub fn from_environment(
        request_dir: &Path,
        environment: &EnvironmentConfig,
    ) -> Result<Self, BlockStoreError> {
        Self::from_environment_with_redb_durability_and_progress(
            request_dir,
            environment,
            RedbBlockStoreDurabilityMode::Durable,
            None,
        )
    }

    pub fn from_environment_with_redb_progress(
        request_dir: &Path,
        environment: &EnvironmentConfig,
        progress: Option<OperatorProgressReporter>,
    ) -> Result<Self, BlockStoreError> {
        Self::from_environment_with_redb_durability_and_progress(
            request_dir,
            environment,
            RedbBlockStoreDurabilityMode::Durable,
            progress,
        )
    }

    pub fn from_environment_with_redb_durability_and_progress(
        request_dir: &Path,
        environment: &EnvironmentConfig,
        redb_durability_mode: RedbBlockStoreDurabilityMode,
        progress: Option<OperatorProgressReporter>,
    ) -> Result<Self, BlockStoreError> {
        match environment {
            EnvironmentConfig::Local {
                block_store_root, ..
            } => FilesystemBlockStore::new(resolve_path(request_dir, block_store_root))
                .map(Self::Local),
            EnvironmentConfig::LocalRedb {
                block_store_root, ..
            } => Self::local_redb_store(
                request_dir,
                block_store_root,
                redb_durability_mode,
                progress,
            ),
            EnvironmentConfig::LocalOverlay { block_store, .. }
            | EnvironmentConfig::Production { block_store, .. } => {
                Self::production_overlay_store(request_dir, block_store)
            }
            EnvironmentConfig::ProductionV2 { block_store, .. } => {
                Self::production_v2_store(block_store)
            }
        }
    }

    pub fn from_environment_with_redb_durability(
        request_dir: &Path,
        environment: &EnvironmentConfig,
        redb_durability_mode: RedbBlockStoreDurabilityMode,
    ) -> Result<Self, BlockStoreError> {
        Self::from_environment_with_redb_durability_and_progress(
            request_dir,
            environment,
            redb_durability_mode,
            None,
        )
    }

    fn production_overlay_store(
        request_dir: &Path,
        config: &ProductionBlockStoreConfig,
    ) -> Result<Self, BlockStoreError> {
        config
            .validate_for_overlay()
            .map_err(|error| BlockStoreError::BackendFailure(error.to_string()))?;
        let azure_backing_store = AzureBlobBlockStore::new(&config.container_sas_url)?;
        let memory_cache = MemoryBlockStore::new(
            config
                .memory_cache_max_resident_blocks
                .expect("validated overlay caches always include a memory capacity"),
        )
        .map_err(|error| BlockStoreError::BackendFailure(error.to_string()))?;
        let filesystem_cache = FilesystemBlockStore::new(resolve_path(
            request_dir,
            config
                .filesystem_cache_root
                .as_ref()
                .expect("validated overlay caches always include a filesystem cache root"),
        ))?;
        let layers: Vec<Box<dyn OverlayStoreLayer>> = vec![
            Box::new(PassiveLayer::cache(memory_cache)),
            Box::new(PassiveLayer::cache(filesystem_cache)),
            Box::new(PassiveLayer::writable(azure_backing_store)),
        ];
        let overlay_store = OverlayBlockStore::new(layers)
            .map_err(|error| BlockStoreError::BackendFailure(error.to_string()))?;
        Ok(Self::Overlay(Arc::new(overlay_store)))
    }

    fn production_v2_store(config: &ProductionBlockStoreConfig) -> Result<Self, BlockStoreError> {
        config
            .validate_for_azure_table()
            .map_err(|error| BlockStoreError::BackendFailure(error.to_string()))?;
        AzureTableBlockStoreV2::new(&config.container_sas_url).map(Self::AzureTable)
    }

    fn local_redb_store(
        request_dir: &Path,
        block_store_root: &Path,
        redb_durability_mode: RedbBlockStoreDurabilityMode,
        progress: Option<OperatorProgressReporter>,
    ) -> Result<Self, BlockStoreError> {
        let store_root = resolve_path(request_dir, block_store_root);
        let database_path = store_root.join(REDB_DATABASE_FILE_NAME);
        report_operator_progress(
            progress.as_ref(),
            format!(
                "Opening local-redb block store {}.",
                database_path.display()
            ),
        );
        let callback_progress = progress.clone();
        let callback_database_path = database_path.clone();
        let heartbeat_database_path = database_path.clone();
        let opened_database_path = database_path.clone();
        let telemetry_callback = callback_progress
            .map(|progress| redb_telemetry_callback(progress, callback_database_path));
        run_with_operator_liveness(
            progress,
            move |elapsed| {
                format!(
                    "Still opening local-redb block store {} after {}s; waiting on upstream redb work.",
                    heartbeat_database_path.display(),
                    elapsed.as_secs()
                )
            },
            move || {
                RedbBlockStore::new_with_durability_and_telemetry(
                    store_root,
                    redb_durability_mode,
                    telemetry_callback,
                )
                .map(|store| Self::LocalRedb {
                    store,
                    database_path: opened_database_path,
                })
            },
        )
    }

    pub fn compact_now(&mut self) -> Result<(), BlockStoreError> {
        self.compact_now_with_progress(None)
    }

    pub fn compact_now_with_progress(
        &mut self,
        progress: Option<OperatorProgressReporter>,
    ) -> Result<(), BlockStoreError> {
        match self {
            Self::LocalRedb {
                store,
                database_path,
            } => {
                let heartbeat_database_path = database_path.clone();
                report_operator_progress(
                    progress.as_ref(),
                    format!(
                        "Compacting local-redb block store {}.",
                        database_path.display()
                    ),
                );
                run_with_operator_liveness(
                    progress,
                    move |elapsed| {
                        format!(
                            "Still compacting local-redb block store {} after {}s.",
                            heartbeat_database_path.display(),
                            elapsed.as_secs()
                        )
                    },
                    || store.compact_now(),
                )
            }
            Self::GatewayHttp3(_) | Self::Local(_) | Self::Overlay(_) | Self::AzureTable(_) => {
                Err(BlockStoreError::BackendFailure(
                    "maintenance compact is supported only for the local-redb block-store profile"
                        .to_owned(),
                ))
            }
        }
    }
}

fn report_operator_progress(progress: Option<&OperatorProgressReporter>, message: String) {
    if let Some(progress) = progress {
        progress(message);
    }
}

fn redb_telemetry_callback(
    progress: OperatorProgressReporter,
    fallback_database_path: PathBuf,
) -> BlockStoreTelemetryCallback {
    let last_reported_signature = Arc::new(Mutex::new(None::<String>));
    Arc::new(move |event| {
        if let Some(message) =
            project_redb_telemetry_event(&fallback_database_path, &event, &last_reported_signature)
        {
            progress(message);
        }
    })
}

fn project_redb_telemetry_event(
    fallback_database_path: &Path,
    event: &BlockStoreTelemetryEvent,
    last_reported_signature: &Mutex<Option<String>>,
) -> Option<String> {
    let database_path = event
        .attributes
        .get("database_path")
        .cloned()
        .unwrap_or_else(|| fallback_database_path.display().to_string());
    let message = match event.name.as_str() {
        "repair_status" => {
            let percent = event
                .attributes
                .get("progress")
                .and_then(|value| parse_repair_progress_percent(value))?;
            format!(
                "local-redb repair progress for {}: {}% (upstream coarse milestone).",
                database_path, percent
            )
        }
        _ => {
            let message = event.message.as_deref()?;
            format!("local-redb telemetry for {}: {}.", database_path, message)
        }
    };

    let signature = format!(
        "{}|{}|{}",
        event.name,
        database_path,
        event
            .attributes
            .get("progress")
            .map(String::as_str)
            .unwrap_or("")
    );
    let mut last_reported_signature = last_reported_signature
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if last_reported_signature.as_deref() == Some(signature.as_str()) {
        return None;
    }
    *last_reported_signature = Some(signature);
    Some(message)
}

fn parse_repair_progress_percent(value: &str) -> Option<u8> {
    let progress = value.parse::<f64>().ok()?;
    if !progress.is_finite() || !(0.0..=1.0).contains(&progress) {
        return None;
    }
    Some((progress * 100.0).round() as u8)
}

struct OperatorLivenessHeartbeat {
    keep_running: Arc<AtomicBool>,
    heartbeat_thread: Option<std::thread::JoinHandle<()>>,
}

impl OperatorLivenessHeartbeat {
    fn new<F>(
        progress: Option<OperatorProgressReporter>,
        heartbeat_interval: Duration,
        heartbeat_message: F,
    ) -> Self
    where
        F: Fn(Duration) -> String + Send + 'static,
    {
        let Some(progress) = progress else {
            return Self {
                keep_running: Arc::new(AtomicBool::new(false)),
                heartbeat_thread: None,
            };
        };
        if heartbeat_interval.is_zero() {
            return Self {
                keep_running: Arc::new(AtomicBool::new(false)),
                heartbeat_thread: None,
            };
        }

        let keep_running = Arc::new(AtomicBool::new(true));
        let heartbeat_keep_running = Arc::clone(&keep_running);
        let heartbeat_thread = Some(std::thread::spawn(move || {
            let start = Instant::now();
            while heartbeat_keep_running.load(Ordering::Acquire) {
                std::thread::park_timeout(heartbeat_interval);
                if !heartbeat_keep_running.load(Ordering::Acquire) {
                    break;
                }
                progress(heartbeat_message(start.elapsed()));
            }
        }));

        Self {
            keep_running,
            heartbeat_thread,
        }
    }

    fn stop(&mut self) {
        self.keep_running.store(false, Ordering::Release);
        if let Some(heartbeat_thread) = self.heartbeat_thread.take() {
            heartbeat_thread.thread().unpark();
            let _ = heartbeat_thread.join();
        }
    }
}

impl Drop for OperatorLivenessHeartbeat {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_with_operator_liveness<T, F, H>(
    progress: Option<OperatorProgressReporter>,
    heartbeat_message: H,
    action: F,
) -> Result<T, BlockStoreError>
where
    F: FnOnce() -> Result<T, BlockStoreError>,
    H: Fn(Duration) -> String + Send + 'static,
{
    let mut heartbeat = OperatorLivenessHeartbeat::new(
        progress,
        REDB_PROGRESS_HEARTBEAT_INTERVAL,
        heartbeat_message,
    );
    let result = action();
    heartbeat.stop();
    result
}

pub(crate) fn block_on_block_store_future<F>(future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(future))
            }
            tokio::runtime::RuntimeFlavor::CurrentThread => std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("failed to build tokio runtime for block-store bridge")
                            .block_on(future)
                    })
                    .join()
                    .expect("block-store bridge thread panicked")
            }),
            _ => unreachable!("unsupported tokio runtime flavor"),
        }
    } else {
        block_on_future(future)
    }
}

pub(crate) fn block_on_future<F>(future: F) -> F::Output
where
    F: Future,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(future))
            }
            tokio::runtime::RuntimeFlavor::CurrentThread => {
                panic!(
                    "block_on_future cannot run inside a current-thread Tokio runtime; \
                     use block_on_future_factory to construct the future inside a bridge thread"
                )
            }
            _ => unreachable!("unsupported tokio runtime flavor"),
        }
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for block-store bridge")
            .block_on(future)
    }
}

pub(crate) fn block_on_future_factory<F, Fut, T>(make_future: F) -> T
where
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = T>,
    T: Send,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(make_future()))
            }
            tokio::runtime::RuntimeFlavor::CurrentThread => std::thread::scope(|scope| {
                scope
                    .spawn(|| {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("failed to build tokio runtime for future bridge")
                            .block_on(make_future())
                    })
                    .join()
                    .expect("future bridge thread panicked")
            }),
            _ => unreachable!("unsupported tokio runtime flavor"),
        }
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for future bridge")
            .block_on(make_future())
    }
}

#[async_trait]
impl BlockStore for ConfiguredBlockStore {
    fn set_telemetry_callback(
        &self,
        telemetry_callback: Option<BlockStoreTelemetryCallback>,
    ) -> Result<(), BlockStoreError> {
        match self {
            Self::GatewayHttp3(store) => store.set_telemetry_callback(telemetry_callback),
            Self::Local(store) => store.set_telemetry_callback(telemetry_callback),
            Self::LocalRedb { store, .. } => store.set_telemetry_callback(telemetry_callback),
            Self::Overlay(store) => store.set_telemetry_callback(telemetry_callback),
            Self::AzureTable(store) => store.set_telemetry_callback(telemetry_callback),
        }
    }

    async fn put_block_bytes(
        &self,
        block_id: &BlockHash,
        block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        match self {
            Self::GatewayHttp3(store) => store.put_block_bytes(block_id, block_bytes).await,
            Self::Local(store) => store.put_block_bytes(block_id, block_bytes).await,
            Self::LocalRedb { store, .. } => store.put_block_bytes(block_id, block_bytes).await,
            Self::Overlay(store) => store.put_block_bytes(block_id, block_bytes).await,
            Self::AzureTable(store) => store.put_block_bytes(block_id, block_bytes).await,
        }
    }

    async fn put_block_bytes_batch(
        &self,
        entries: &[BlockBytesBatchEntry<'_>],
    ) -> Result<(), BlockStoreError> {
        match self {
            Self::GatewayHttp3(store) => store.put_block_bytes_batch(entries).await,
            Self::Local(store) => store.put_block_bytes_batch(entries).await,
            Self::LocalRedb { store, .. } => store.put_block_bytes_batch(entries).await,
            Self::Overlay(store) => store.put_block_bytes_batch(entries).await,
            Self::AzureTable(store) => store.put_block_bytes_batch(entries).await,
        }
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        match self {
            Self::GatewayHttp3(store) => store.get_block_bytes(block_id).await,
            Self::Local(store) => store.get_block_bytes(block_id).await,
            Self::LocalRedb { store, .. } => store.get_block_bytes(block_id).await,
            Self::Overlay(store) => store.get_block_bytes(block_id).await,
            Self::AzureTable(store) => store.get_block_bytes(block_id).await,
        }
    }

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
        match self {
            Self::GatewayHttp3(store) => store.iter_block_ids(),
            Self::Local(store) => store.iter_block_ids(),
            Self::LocalRedb { store, .. } => store.iter_block_ids(),
            Self::Overlay(store) => store.iter_block_ids(),
            Self::AzureTable(store) => store.iter_block_ids(),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use lexongraph_block::{
        Block, Content, EmbeddingSpec, LeafBlock, LeafEntry, VERSION_1, serialize_block,
    };
    use tempfile::tempdir;

    use super::*;
    use crate::config::{LocalEmbeddingConfig, ProductionEmbeddingConfig};

    fn put_block(store: &impl BlockStore, block: &Block) -> BlockHash {
        block_on_block_store_future(store.put(block)).unwrap()
    }

    fn local_redb_store_for_test(root: &Path) -> ConfiguredBlockStore {
        let store_root = root.join("blocks");
        ConfiguredBlockStore::LocalRedb {
            store: RedbBlockStore::new(&store_root).unwrap(),
            database_path: store_root.join(REDB_DATABASE_FILE_NAME),
        }
    }

    #[test]
    fn local_filesystem_store_uses_upstream_layout() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();
        let block = sample_block();
        let block_id = put_block(&store, &block);
        let block_id_text = block_id.to_string();
        let expected_path = dir
            .path()
            .join("blocks")
            .join(&block_id_text[..2])
            .join(&block_id_text[2..4])
            .join(format!("{block_id_text}.cbor"));

        assert!(expected_path.is_file());
    }

    #[test]
    fn configured_production_store_returns_explicit_backend_failure() {
        let error = ConfiguredBlockStore::from_environment(
            Path::new("."),
            &EnvironmentConfig::Production {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.blob.core.windows.net/archive-sync?sig=test".into(),
                    prefix: Some("archive-sync".into()),
                    filesystem_cache_root: None,
                    memory_cache_max_resident_blocks: None,
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://unused.production.example".into(),
                    deployment: "unused".into(),
                    api_version: "2024-02-01".into(),
                    api_key_env: None,
                },
            },
        )
        .unwrap_err();

        assert!(matches!(error, BlockStoreError::BackendFailure(_)));
        assert!(
            error
                .to_string()
                .contains("overlay block_store.prefix is not supported")
        );
    }

    #[test]
    fn configured_local_store_delegates_iter_block_ids() {
        let dir = tempdir().unwrap();
        let store = ConfiguredBlockStore::Local(
            FilesystemBlockStore::new(dir.path().join("blocks")).unwrap(),
        );
        let block = sample_block();
        let block_id = put_block(&store, &block);

        let block_ids = block_on_block_store_future(async {
            store.iter_block_ids()?.try_collect::<Vec<_>>().await
        })
        .unwrap();

        assert_eq!(block_ids, vec![block_id]);
    }

    #[test]
    fn configured_local_redb_store_delegates_iter_block_ids() {
        let dir = tempdir().unwrap();
        let store = local_redb_store_for_test(dir.path());
        let block = sample_block();
        let block_id = put_block(&store, &block);

        let block_ids = block_on_block_store_future(async {
            store.iter_block_ids()?.try_collect::<Vec<_>>().await
        })
        .unwrap();

        assert_eq!(block_ids, vec![block_id]);
    }

    #[test]
    fn configured_local_redb_store_supports_fast_durability() {
        let dir = tempdir().unwrap();
        let environment = EnvironmentConfig::LocalRedb {
            block_store_root: dir.path().join("blocks"),
            embedding: LocalEmbeddingConfig {
                base_url: "http://unused.local".into(),
                model: "all-MiniLM-L6-v2".into(),
                api_key_env: None,
                request_timeout_secs: 30,
                max_retries: 5,
                retry_delay_ms: 1_000,
            },
        };
        let store = ConfiguredBlockStore::from_environment_with_redb_durability(
            Path::new("."),
            &environment,
            RedbBlockStoreDurabilityMode::Fast,
        )
        .unwrap();

        match store {
            ConfiguredBlockStore::LocalRedb { store, .. } => {
                assert!(format!("{store:?}").contains("Fast"));
            }
            _ => panic!("expected local redb store"),
        }
    }

    #[test]
    fn configured_local_redb_store_compacts_without_losing_blocks() {
        let dir = tempdir().unwrap();
        let mut store = local_redb_store_for_test(dir.path());
        let block = sample_block();
        let block_id = put_block(&store, &block);

        store.compact_now().unwrap();

        let retrieved = block_on_block_store_future(store.get_block_bytes(&block_id))
            .unwrap()
            .unwrap();
        assert_eq!(retrieved, serialize_block(&block).unwrap().bytes);
    }

    #[test]
    fn configured_local_filesystem_store_rejects_compaction() {
        let dir = tempdir().unwrap();
        let mut store = ConfiguredBlockStore::Local(
            FilesystemBlockStore::new(dir.path().join("blocks")).unwrap(),
        );

        let error = store.compact_now().unwrap_err();

        assert!(matches!(error, BlockStoreError::BackendFailure(_)));
        assert!(error.to_string().contains("local-redb"));
    }

    #[test]
    fn repair_status_telemetry_projects_to_cli_progress_message() {
        let last_reported_signature = Mutex::new(None);
        let event = BlockStoreTelemetryEvent::new("repair_status")
            .with_attribute("database_path", r"C:\data\blocks.redb")
            .with_attribute("progress", "0.600");

        let message = project_redb_telemetry_event(
            Path::new(r"C:\fallback\blocks.redb"),
            &event,
            &last_reported_signature,
        );

        assert_eq!(
            message.as_deref(),
            Some(
                "local-redb repair progress for C:\\data\\blocks.redb: 60% (upstream coarse milestone)."
            )
        );
    }

    #[test]
    fn duplicate_repair_status_telemetry_is_suppressed() {
        let last_reported_signature = Mutex::new(None);
        let event = BlockStoreTelemetryEvent::new("repair_status")
            .with_attribute("database_path", r"C:\data\blocks.redb")
            .with_attribute("progress", "0.300");

        assert!(
            project_redb_telemetry_event(
                Path::new(r"C:\fallback\blocks.redb"),
                &event,
                &last_reported_signature,
            )
            .is_some()
        );
        assert!(
            project_redb_telemetry_event(
                Path::new(r"C:\fallback\blocks.redb"),
                &event,
                &last_reported_signature,
            )
            .is_none()
        );
    }

    #[test]
    fn repair_status_telemetry_preserves_terminal_completion_percentage() {
        let last_reported_signature = Mutex::new(None);
        let event = BlockStoreTelemetryEvent::new("repair_status")
            .with_attribute("database_path", r"C:\data\blocks.redb")
            .with_attribute("progress", "1.0");

        let message = project_redb_telemetry_event(
            Path::new(r"C:\fallback\blocks.redb"),
            &event,
            &last_reported_signature,
        );

        assert_eq!(
            message.as_deref(),
            Some(
                "local-redb repair progress for C:\\data\\blocks.redb: 100% (upstream coarse milestone)."
            )
        );
    }

    #[test]
    fn repair_status_telemetry_ignores_non_finite_progress_values() {
        let last_reported_signature = Mutex::new(None);
        let event = BlockStoreTelemetryEvent::new("repair_status")
            .with_attribute("database_path", r"C:\data\blocks.redb")
            .with_attribute("progress", "NaN");

        let message = project_redb_telemetry_event(
            Path::new(r"C:\fallback\blocks.redb"),
            &event,
            &last_reported_signature,
        );

        assert!(message.is_none());
    }

    #[test]
    fn configured_production_store_requires_overlay_layers() {
        let error = ConfiguredBlockStore::from_environment(
            Path::new("."),
            &EnvironmentConfig::Production {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.blob.core.windows.net/archive-sync?sig=test".into(),
                    prefix: None,
                    filesystem_cache_root: None,
                    memory_cache_max_resident_blocks: None,
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://unused.production.example".into(),
                    deployment: "unused".into(),
                    api_version: "2024-02-01".into(),
                    api_key_env: None,
                },
            },
        )
        .unwrap_err();

        assert!(matches!(error, BlockStoreError::BackendFailure(_)));
        assert!(
            error
                .to_string()
                .contains("overlay block_store.filesystem_cache_root is required")
        );
    }

    #[test]
    fn configured_production_store_accepts_overlay_cache_layers() {
        let store = ConfiguredBlockStore::from_environment(
            Path::new("."),
            &EnvironmentConfig::Production {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.blob.core.windows.net/archive-sync?sig=test".into(),
                    prefix: None,
                    filesystem_cache_root: Some("cache".into()),
                    memory_cache_max_resident_blocks: Some(64),
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://unused.production.example".into(),
                    deployment: "unused".into(),
                    api_version: "2024-02-01".into(),
                    api_key_env: None,
                },
            },
        )
        .unwrap();

        assert!(matches!(store, ConfiguredBlockStore::Overlay(_)));
    }

    #[test]
    fn configured_production_v2_store_accepts_direct_table_config() {
        let store = ConfiguredBlockStore::from_environment(
            Path::new("."),
            &EnvironmentConfig::ProductionV2 {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.table.core.windows.net/archive-sync?sig=test".into(),
                    prefix: None,
                    filesystem_cache_root: None,
                    memory_cache_max_resident_blocks: None,
                },
                embedding: ProductionEmbeddingConfig {
                    endpoint: "https://unused.production.example".into(),
                    deployment: "unused".into(),
                    api_version: "2024-02-01".into(),
                    api_key_env: None,
                },
            },
        )
        .unwrap();

        assert!(matches!(store, ConfiguredBlockStore::AzureTable(_)));
    }

    #[test]
    fn configured_local_overlay_store_accepts_overlay_cache_layers() {
        let store = ConfiguredBlockStore::from_environment(
            Path::new("."),
            &EnvironmentConfig::LocalOverlay {
                block_store: ProductionBlockStoreConfig {
                    container_sas_url:
                        "https://example.blob.core.windows.net/archive-sync?sig=test".into(),
                    prefix: None,
                    filesystem_cache_root: Some("cache".into()),
                    memory_cache_max_resident_blocks: Some(64),
                },
                embedding: crate::config::LocalEmbeddingConfig {
                    base_url: "http://localhost:8080".into(),
                    model: "all-MiniLM-L6-v2".into(),
                    api_key_env: None,
                    request_timeout_secs: 30,
                    max_retries: 5,
                    retry_delay_ms: 1_000,
                },
            },
        )
        .unwrap();

        assert!(matches!(store, ConfiguredBlockStore::Overlay(_)));
    }

    fn sample_block() -> Block {
        Block::Leaf(LeafBlock {
            version: VERSION_1,
            level: 0,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            entries: vec![LeafEntry {
                embedding: vec![0, 0, 0, 0, 0, 0, 0, 0],
                metadata: vec![],
                content: Content {
                    media_type: "text/plain".into(),
                    body: b"ignored".to_vec(),
                },
            }],
            ext: None,
        })
    }
}
