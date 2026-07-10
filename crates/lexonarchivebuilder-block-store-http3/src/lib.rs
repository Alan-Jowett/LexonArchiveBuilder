// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::fmt;
use std::net::{Ipv6Addr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Buf;
use h3::error::{ConnectionError, StreamError};
use h3_quinn::quinn;
use http::{Request, Uri};
use lexongraph_block::BlockHash;
use lexongraph_block_store::{BlockIdStream, BlockStore, BlockStoreError};
use rustls::RootCertStore;
use tokio::sync::Mutex;
use tokio::time::timeout;

const ALPN_H3: &[u8] = b"h3";
const DEFAULT_GATEWAY_PORT: u16 = 443;
const DEFAULT_GATEWAY_TIMEOUT: Duration = Duration::from_secs(30);
static RUSTLS_PROVIDER: OnceLock<()> = OnceLock::new();

#[derive(Clone, Debug, PartialEq, Eq)]
struct GatewayResponse {
    status_code: u16,
    body: Vec<u8>,
}

#[async_trait]
trait GatewayTransport: Send + Sync {
    async fn fetch(&self, dns_name: &str, path: &str) -> Result<GatewayResponse, String>;
}

type Http3SendRequest = h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>;

#[derive(Default)]
struct Http3GatewayState {
    resolved_address: Option<SocketAddr>,
    send_request: Option<Http3SendRequest>,
}

impl fmt::Debug for Http3GatewayState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Http3GatewayState")
            .field("resolved_address", &self.resolved_address)
            .field("has_send_request", &self.send_request.is_some())
            .finish()
    }
}

#[derive(Debug)]
struct Http3GatewayTransport {
    endpoint: quinn::Endpoint,
    state: Mutex<Http3GatewayState>,
}

pub struct Http3BlockStore {
    dns_name: String,
    transport: Arc<dyn GatewayTransport>,
}

impl fmt::Debug for Http3BlockStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Http3BlockStore")
            .field("dns_name", &self.dns_name)
            .finish()
    }
}

impl Clone for Http3BlockStore {
    fn clone(&self) -> Self {
        Self {
            dns_name: self.dns_name.clone(),
            transport: Arc::clone(&self.transport),
        }
    }
}

impl Http3BlockStore {
    pub fn new(dns_name: &str) -> Result<Self, BlockStoreError> {
        Self::with_transport(
            dns_name,
            Arc::new(Http3GatewayTransport::new().map_err(BlockStoreError::BackendFailure)?),
        )
    }

    fn with_transport(
        dns_name: &str,
        transport: Arc<dyn GatewayTransport>,
    ) -> Result<Self, BlockStoreError> {
        let dns_name = validate_dns_name(dns_name)?;
        Ok(Self {
            dns_name,
            transport,
        })
    }

    fn build_block_path(block_id: &BlockHash) -> String {
        format!("/block/{block_id}")
    }

    fn build_block_uri(dns_name: &str, path: &str) -> Result<Uri, String> {
        format!("https://{dns_name}{path}")
            .parse()
            .map_err(|error| format!("failed to build gateway URI: {error}"))
    }
}

fn validate_dns_name(dns_name: &str) -> Result<String, BlockStoreError> {
    let trimmed = dns_name.trim();
    if trimmed.is_empty() {
        return Err(BlockStoreError::BackendFailure(
            "gateway dns name must not be empty".into(),
        ));
    }
    if trimmed.contains("://")
        || trimmed.contains('/')
        || trimmed.contains(':')
        || trimmed.chars().any(char::is_whitespace)
    {
        return Err(BlockStoreError::BackendFailure(
            "gateway dns name must be a bare host name without scheme, path, port, or whitespace"
                .into(),
        ));
    }
    Ok(trimmed.to_string())
}

fn load_native_roots() -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    let certificate_result = rustls_native_certs::load_native_certs();
    let parse_error_count = certificate_result.errors.len();
    for cert in certificate_result.certs {
        let _ = roots.add(cert);
    }

    if roots.is_empty() {
        return Err(if parse_error_count == 0 {
            "failed to load any native TLS root certificates".into()
        } else {
            format!(
                "failed to load any native TLS root certificates; {} certificate parse errors occurred",
                parse_error_count
            )
        });
    }

    Ok(roots)
}

fn install_rustls_provider() -> Result<(), String> {
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

fn build_client_config() -> Result<quinn::ClientConfig, String> {
    install_rustls_provider()?;
    let mut tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(load_native_roots()?)
        .with_no_client_auth();
    tls_config.alpn_protocols = vec![ALPN_H3.to_vec()];
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
        .map_err(|error| format!("failed to build QUIC TLS client config: {error}"))?;
    Ok(quinn::ClientConfig::new(Arc::new(crypto)))
}

fn build_client_endpoint() -> Result<quinn::Endpoint, String> {
    let bind_addr: SocketAddr = (Ipv6Addr::UNSPECIFIED, 0).into();
    let mut endpoint = quinn::Endpoint::client(bind_addr)
        .map_err(|error| format!("failed to create QUIC client endpoint: {error}"))?;
    endpoint.set_default_client_config(build_client_config()?);
    Ok(endpoint)
}

fn map_stream_error(error: StreamError) -> String {
    format!("gateway request failed: {error}")
}

fn map_connection_error(error: ConnectionError) -> Option<String> {
    if error.is_h3_no_error() {
        None
    } else {
        Some(format!("gateway connection closed with error: {error}"))
    }
}

impl Http3GatewayTransport {
    fn new() -> Result<Self, String> {
        Ok(Self {
            endpoint: build_client_endpoint()?,
            state: Mutex::new(Http3GatewayState::default()),
        })
    }

    async fn fetch_once(&self, dns_name: &str, path: &str) -> Result<GatewayResponse, String> {
        let uri = Http3BlockStore::build_block_uri(dns_name, path)?;
        let mut send_request = self.get_or_connect(dns_name).await?;

        let request = Request::builder()
            .method("GET")
            .uri(uri)
            .body(())
            .map_err(|error| format!("failed to build gateway request: {error}"))?;
        let mut request_stream = send_request
            .send_request(request)
            .await
            .map_err(map_stream_error)?;
        request_stream.finish().await.map_err(map_stream_error)?;

        let response = request_stream
            .recv_response()
            .await
            .map_err(map_stream_error)?;
        let status_code = response.status().as_u16();
        let mut body = Vec::new();
        while let Some(mut chunk) = request_stream.recv_data().await.map_err(map_stream_error)? {
            while chunk.has_remaining() {
                let bytes = chunk.chunk();
                body.extend_from_slice(bytes);
                let len = bytes.len();
                chunk.advance(len);
            }
        }

        Ok(GatewayResponse { status_code, body })
    }

    async fn get_or_connect(&self, dns_name: &str) -> Result<Http3SendRequest, String> {
        let resolved_address = {
            let state = self.state.lock().await;
            if let Some(send_request) = state.send_request.clone() {
                return Ok(send_request);
            }
            state.resolved_address
        };

        let address = if let Some(address) = resolved_address {
            address
        } else {
            tokio::net::lookup_host((dns_name, DEFAULT_GATEWAY_PORT))
                .await
                .map_err(|error| format!("failed to resolve gateway host {dns_name}: {error}"))?
                .next()
                .ok_or_else(|| format!("gateway host {dns_name} did not resolve to an address"))?
        };

        let connection = self
            .endpoint
            .connect(address, dns_name)
            .map_err(|error| format!("failed to start QUIC connection: {error}"))?
            .await
            .map_err(|error| format!("failed to establish QUIC connection: {error}"))?;

        let quinn_connection = h3_quinn::Connection::new(connection);
        let (mut driver, send_request) = h3::client::new(quinn_connection)
            .await
            .map_err(|error| format!("failed to initialize HTTP/3 client: {error}"))?;

        tokio::spawn(async move {
            let _ = map_connection_error(driver.wait_idle().await);
        });

        let mut state = self.state.lock().await;
        if let Some(existing_send_request) = state.send_request.clone() {
            return Ok(existing_send_request);
        }
        state.resolved_address = Some(address);
        state.send_request = Some(send_request.clone());
        Ok(send_request)
    }

    async fn reset_connection(&self) {
        let mut state = self.state.lock().await;
        state.resolved_address = None;
        state.send_request = None;
    }

    async fn fetch_with_timeout(
        &self,
        dns_name: &str,
        path: &str,
    ) -> Result<GatewayResponse, String> {
        timeout(DEFAULT_GATEWAY_TIMEOUT, self.fetch_once(dns_name, path))
            .await
            .map_err(|_| {
                format!(
                    "gateway request exceeded {}s timeout for host {dns_name}",
                    DEFAULT_GATEWAY_TIMEOUT.as_secs()
                )
            })?
    }
}

#[async_trait]
impl GatewayTransport for Http3GatewayTransport {
    async fn fetch(&self, dns_name: &str, path: &str) -> Result<GatewayResponse, String> {
        match self.fetch_with_timeout(dns_name, path).await {
            Ok(response) => Ok(response),
            Err(error) => {
                self.reset_connection().await;
                self.fetch_with_timeout(dns_name, path)
                    .await
                    .map_err(|retry_error| format!("{error}; retry failed: {retry_error}"))
            }
        }
    }
}

#[async_trait]
impl BlockStore for Http3BlockStore {
    async fn put_block_bytes(
        &self,
        _block_id: &BlockHash,
        _block_bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        Err(BlockStoreError::BackendFailure(
            "gateway-http3 block store is read-only and does not support writes".into(),
        ))
    }

    async fn get_block_bytes(
        &self,
        block_id: &BlockHash,
    ) -> Result<Option<Vec<u8>>, BlockStoreError> {
        let path = Self::build_block_path(block_id);
        let response = self
            .transport
            .fetch(&self.dns_name, &path)
            .await
            .map_err(BlockStoreError::BackendFailure)?;
        match response.status_code {
            200 => Ok(Some(response.body)),
            404 => Ok(None),
            status_code => Err(BlockStoreError::BackendFailure(format!(
                "gateway returned unexpected HTTP status {status_code} for block {block_id}"
            ))),
        }
    }

    fn iter_block_ids(&self) -> Result<BlockIdStream<'_>, BlockStoreError> {
        Err(BlockStoreError::BackendFailure(
            "gateway-http3 block store is read-only and does not support whole-store iteration"
                .into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct FakeTransport {
        response: Result<GatewayResponse, String>,
    }

    #[async_trait]
    impl GatewayTransport for FakeTransport {
        async fn fetch(&self, _dns_name: &str, _path: &str) -> Result<GatewayResponse, String> {
            self.response.clone()
        }
    }

    fn sample_block_id() -> BlockHash {
        BlockHash::from_bytes([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb,
            0xcc, 0xdd, 0xee, 0xff,
        ])
    }

    fn store_with_response(response: Result<GatewayResponse, String>) -> Http3BlockStore {
        Http3BlockStore::with_transport("gateway.example.com", Arc::new(FakeTransport { response }))
            .unwrap()
    }

    #[tokio::test]
    async fn get_block_bytes_returns_body_on_success() {
        let store = store_with_response(Ok(GatewayResponse {
            status_code: 200,
            body: vec![1, 2, 3],
        }));

        let body = store.get_block_bytes(&sample_block_id()).await.unwrap();

        assert_eq!(body, Some(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn get_block_bytes_maps_404_to_missing() {
        let store = store_with_response(Ok(GatewayResponse {
            status_code: 404,
            body: Vec::new(),
        }));

        let body = store.get_block_bytes(&sample_block_id()).await.unwrap();

        assert_eq!(body, None);
    }

    #[tokio::test]
    async fn get_block_bytes_surfaces_transport_failures() {
        let store = store_with_response(Err("transport failed".into()));

        let error = store.get_block_bytes(&sample_block_id()).await.unwrap_err();

        assert!(matches!(error, BlockStoreError::BackendFailure(_)));
        assert!(error.to_string().contains("transport failed"));
    }

    #[tokio::test]
    async fn get_block_bytes_rejects_unexpected_http_status() {
        let store = store_with_response(Ok(GatewayResponse {
            status_code: 500,
            body: Vec::new(),
        }));

        let error = store.get_block_bytes(&sample_block_id()).await.unwrap_err();

        assert!(matches!(error, BlockStoreError::BackendFailure(_)));
        assert!(error.to_string().contains("unexpected HTTP status 500"));
    }

    #[test]
    fn constructor_rejects_non_host_inputs() {
        for dns_name in [
            "",
            "   ",
            "https://gateway.example.com",
            "gateway.example.com:443",
            "gateway.example.com/path",
            "gateway example.com",
        ] {
            let error = Http3BlockStore::new(dns_name).unwrap_err();
            assert!(matches!(error, BlockStoreError::BackendFailure(_)));
        }
    }

    #[tokio::test]
    async fn write_operations_fail_explicitly() {
        let store = store_with_response(Ok(GatewayResponse {
            status_code: 200,
            body: Vec::new(),
        }));

        let error = store
            .put_block_bytes(&sample_block_id(), &[1, 2, 3])
            .await
            .unwrap_err();

        assert!(error.to_string().contains("read-only"));
    }

    #[tokio::test]
    async fn reset_connection_clears_cached_address() {
        let transport = Http3GatewayTransport::new().unwrap();
        {
            let mut state = transport.state.lock().await;
            state.resolved_address = Some((Ipv6Addr::LOCALHOST, DEFAULT_GATEWAY_PORT).into());
        }

        transport.reset_connection().await;

        let state = transport.state.lock().await;
        assert_eq!(state.resolved_address, None);
        assert!(state.send_request.is_none());
    }

    #[test]
    fn iter_block_ids_fails_explicitly() {
        let store = store_with_response(Ok(GatewayResponse {
            status_code: 200,
            body: Vec::new(),
        }));

        let error = match store.iter_block_ids() {
            Err(error) => error,
            Ok(_) => panic!("expected iter_block_ids to fail"),
        };

        assert!(error.to_string().contains("whole-store iteration"));
    }
}
