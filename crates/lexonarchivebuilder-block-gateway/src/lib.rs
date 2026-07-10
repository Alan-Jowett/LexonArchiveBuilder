// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{Context, anyhow};
use axum::Router;
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use bytes::Bytes;
use h3_axum::{BoxError, is_graceful_h3_close, serve_h3_with_axum};
use lexonarchivebuilder_indexer::tree_tools::parse_block_hash;
use lexongraph_block_store::BlockStore;
use lexongraph_block_store_azure_table_v2::AzureTableBlockStoreV2;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

pub const CACHE_CONTROL_VALUE: &str = "public, max-age=31536000, immutable";
pub const NO_STORE_CACHE_CONTROL_VALUE: &str = "no-store";
const MAX_CONCURRENT_CONNECTION_TASKS: usize = 256;
const MAX_CONCURRENT_REQUEST_TASKS_PER_CONNECTION: usize = 32;
static CACHE_CONTROL_HEADER: HeaderValue = HeaderValue::from_static(CACHE_CONTROL_VALUE);
static NO_STORE_CACHE_CONTROL_HEADER: HeaderValue =
    HeaderValue::from_static(NO_STORE_CACHE_CONTROL_VALUE);
static OCTET_STREAM_HEADER: HeaderValue = HeaderValue::from_static("application/octet-stream");
static RUSTLS_PROVIDER: OnceLock<()> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub listen_addr: SocketAddr,
    pub sas_url: String,
    pub certificate_path: std::path::PathBuf,
    pub private_key_path: std::path::PathBuf,
}

#[derive(Clone)]
pub struct GatewayState {
    store: Arc<dyn BlockStore + Send + Sync>,
}

impl GatewayState {
    pub fn new(store: Arc<dyn BlockStore + Send + Sync>) -> Self {
        Self { store }
    }
}

pub fn build_router(store: Arc<dyn BlockStore + Send + Sync>) -> Router {
    Router::new()
        .route("/block/{block_id}", get(get_block))
        .with_state(GatewayState::new(store))
}

pub async fn serve(config: GatewayConfig) -> anyhow::Result<()> {
    install_rustls_provider()?;

    let store = Arc::new(
        AzureTableBlockStoreV2::new(&config.sas_url).with_context(|| {
            format!(
                "failed to initialize Azure Table block store from SAS URL configured for {}",
                config.listen_addr
            )
        })?,
    );
    let app = build_router(store);
    let server_config =
        build_quic_server_config(&config.certificate_path, &config.private_key_path)?;
    let endpoint = quinn::Endpoint::server(server_config, config.listen_addr)
        .with_context(|| format!("failed to bind QUIC endpoint to {}", config.listen_addr))?;

    info!(
        "block gateway listening on https://{} over HTTP/3",
        config.listen_addr
    );

    let connection_task_slots = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTION_TASKS));
    while let Some(incoming) = endpoint.accept().await {
        let connection_task_slot = Arc::clone(&connection_task_slots)
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("connection task semaphore unexpectedly closed"))?;
        let app = app.clone();
        tokio::spawn(async move {
            let _connection_task_slot = connection_task_slot;
            if let Err(error) = handle_connection(incoming, app).await {
                error!(?error, "HTTP/3 connection failed");
            }
        });
    }

    Ok(())
}

fn install_rustls_provider() -> anyhow::Result<()> {
    if RUSTLS_PROVIDER.get().is_some() {
        return Ok(());
    }

    match rustls::crypto::aws_lc_rs::default_provider().install_default() {
        Ok(()) | Err(_) => {
            let _ = RUSTLS_PROVIDER.set(());
            Ok(())
        }
    }
}

fn build_quic_server_config(
    certificate_path: &Path,
    private_key_path: &Path,
) -> anyhow::Result<quinn::ServerConfig> {
    let certificates = load_certificates(certificate_path)?;
    let private_key = load_private_key(private_key_path)?;

    let mut tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .context("failed to build rustls server configuration")?;
    tls_config.alpn_protocols = vec![b"h3".to_vec()];

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)
            .context("failed to adapt rustls configuration for QUIC")?,
    ));
    let transport_config = Arc::get_mut(&mut server_config.transport)
        .ok_or_else(|| anyhow!("fresh QUIC transport config unexpectedly shared"))?;
    transport_config
        .max_concurrent_bidi_streams(100_u32.into())
        .max_concurrent_uni_streams(100_u32.into())
        .max_idle_timeout(Some(
            Duration::from_secs(60)
                .try_into()
                .context("failed to convert QUIC idle timeout")?,
        ));
    Ok(server_config)
}

fn load_certificates(path: &Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open certificate file {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("failed to parse certificate file {}", path.display()))?;
    if certificates.is_empty() {
        return Err(anyhow!(
            "certificate file {} did not contain any certificates",
            path.display()
        ));
    }
    Ok(certificates)
}

fn load_private_key(path: &Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open private key file {}", path.display()))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .with_context(|| format!("failed to parse private key file {}", path.display()))?
        .ok_or_else(|| {
            anyhow!(
                "private key file {} did not contain a supported key",
                path.display()
            )
        })
}

async fn get_block(
    AxumPath(block_id): AxumPath<String>,
    State(state): State<GatewayState>,
) -> Response<Body> {
    let Ok(block_hash) = parse_block_hash(&block_id) else {
        debug!(block_id, "rejecting malformed block ID as not found");
        return not_found_response();
    };

    match state.store.get_block_bytes(&block_hash).await {
        Ok(Some(block_bytes)) => {
            let mut response = Response::new(Body::from(block_bytes));
            *response.status_mut() = StatusCode::OK;
            response
                .headers_mut()
                .insert(CONTENT_TYPE, OCTET_STREAM_HEADER.clone());
            response
                .headers_mut()
                .insert(CACHE_CONTROL, CACHE_CONTROL_HEADER.clone());
            response
        }
        Ok(None) => {
            debug!(block_id, "block not found");
            not_found_response()
        }
        Err(error) => {
            warn!(block_id, ?error, "block lookup failed; projecting 404");
            not_found_response()
        }
    }
}

fn not_found_response() -> Response<Body> {
    let mut response = StatusCode::NOT_FOUND.into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, NO_STORE_CACHE_CONTROL_HEADER.clone());
    response
}

async fn handle_connection(incoming: quinn::Incoming, app: Router) -> anyhow::Result<()> {
    let connection = incoming.await.context("failed to accept QUIC connection")?;
    let remote_addr = connection.remote_address();
    info!(%remote_addr, "accepted QUIC connection");

    let h3_connection = h3::server::builder()
        .build(h3_quinn::Connection::new(connection))
        .await
        .context("failed to build HTTP/3 connection")?;
    tokio::pin!(h3_connection);
    let request_task_slots = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUEST_TASKS_PER_CONNECTION));

    loop {
        match h3_connection.accept().await {
            Ok(Some(resolver)) => {
                let request_task_slot = Arc::clone(&request_task_slots)
                    .acquire_owned()
                    .await
                    .map_err(|_| anyhow!("request task semaphore unexpectedly closed"))?;
                let app = app.clone();
                tokio::spawn(async move {
                    let _request_task_slot = request_task_slot;
                    if let Err(error) = handle_request(resolver, app).await {
                        error!(?error, "HTTP/3 request failed");
                    }
                });
            }
            Ok(None) => {
                info!(%remote_addr, "QUIC connection closed by peer");
                break;
            }
            Err(error) => {
                if is_graceful_h3_close(&error) {
                    debug!(%remote_addr, "HTTP/3 connection closed gracefully");
                } else {
                    error!(%remote_addr, ?error, "HTTP/3 connection error");
                }
                break;
            }
        }
    }

    Ok(())
}

async fn handle_request(
    resolver: h3::server::RequestResolver<h3_quinn::Connection, Bytes>,
    app: Router,
) -> Result<(), BoxError> {
    serve_h3_with_axum(app, resolver).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::body::to_bytes;
    use axum::http::Request;
    use lexongraph_block::BlockHash;
    use lexongraph_block_store::{BlockIdStream, BlockStoreError};
    use tower::util::ServiceExt;

    #[derive(Default)]
    struct MockStore {
        block: Option<(BlockHash, Vec<u8>)>,
        fail_reads: bool,
    }

    #[async_trait]
    impl BlockStore for MockStore {
        async fn put_block_bytes(
            &self,
            _block_id: &BlockHash,
            _block_bytes: &[u8],
        ) -> Result<(), BlockStoreError> {
            Ok(())
        }

        async fn get_block_bytes(
            &self,
            block_id: &BlockHash,
        ) -> Result<Option<Vec<u8>>, BlockStoreError> {
            if self.fail_reads {
                return Err(BlockStoreError::BackendFailure(
                    "forced read failure".into(),
                ));
            }
            Ok(self
                .block
                .as_ref()
                .filter(|(stored_id, _)| stored_id == block_id)
                .map(|(_, bytes)| bytes.clone()))
        }

        fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
            Err(BlockStoreError::BackendFailure(
                "iter not implemented in mock store".into(),
            ))
        }
    }

    #[tokio::test]
    async fn successful_lookup_returns_block_bytes_and_cache_headers() {
        let block_id = BlockHash::from_bytes([0x11; BlockHash::LEN]);
        let store = Arc::new(MockStore {
            block: Some((block_id, b"block-bytes".to_vec())),
            fail_reads: false,
        });
        let app = build_router(store);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/block/{block_id}"))
                    .body(Body::empty())
                    .expect("request build should succeed"),
            )
            .await
            .expect("router request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE),
            Some(&OCTET_STREAM_HEADER)
        );
        assert_eq!(
            response.headers().get(CACHE_CONTROL),
            Some(&CACHE_CONTROL_HEADER)
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        assert_eq!(body.as_ref(), b"block-bytes");
    }

    #[tokio::test]
    async fn malformed_block_id_returns_not_found() {
        let store = Arc::new(MockStore::default());
        let app = build_router(store);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/block/not-a-block-hash")
                    .body(Body::empty())
                    .expect("request build should succeed"),
            )
            .await
            .expect("router request should succeed");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(CACHE_CONTROL),
            Some(&NO_STORE_CACHE_CONTROL_HEADER)
        );
    }

    #[tokio::test]
    async fn missing_block_returns_not_found() {
        let store = Arc::new(MockStore::default());
        let app = build_router(store);
        let block_id = BlockHash::from_bytes([0x22; BlockHash::LEN]);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/block/{block_id}"))
                    .body(Body::empty())
                    .expect("request build should succeed"),
            )
            .await
            .expect("router request should succeed");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(CACHE_CONTROL),
            Some(&NO_STORE_CACHE_CONTROL_HEADER)
        );
    }

    #[tokio::test]
    async fn backend_read_failure_returns_not_found() {
        let block_id = BlockHash::from_bytes([0x33; BlockHash::LEN]);
        let store = Arc::new(MockStore {
            block: Some((block_id, b"ignored".to_vec())),
            fail_reads: true,
        });
        let app = build_router(store);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/block/{block_id}"))
                    .body(Body::empty())
                    .expect("request build should succeed"),
            )
            .await
            .expect("router request should succeed");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(CACHE_CONTROL),
            Some(&NO_STORE_CACHE_CONTROL_HEADER)
        );
    }
}
