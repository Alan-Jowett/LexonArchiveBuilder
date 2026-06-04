use std::env;
use std::time::Duration;

use half::f16;
use lexongraph_block::EmbeddingSpec;
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::config::{EnvironmentConfig, LocalEmbeddingConfig};

#[derive(Clone, Debug)]
pub struct LocalOpenAiEmbeddingProvider {
    client: Client,
    base_url: String,
    model: String,
    api_key: Option<String>,
    max_retries: u32,
    retry_delay: Duration,
}

#[derive(Clone, Debug)]
pub struct AzureOpenAiEmbeddingProviderStub;

#[derive(Clone, Debug)]
pub enum ConfiguredEmbeddingProvider {
    Local(LocalOpenAiEmbeddingProvider),
    AzureOpenAi(AzureOpenAiEmbeddingProviderStub),
}

#[derive(Debug, Error)]
pub enum ConfiguredEmbeddingProviderError {
    #[error("failed to build HTTP client: {0}")]
    ClientBuild(reqwest::Error),
    #[error("missing environment variable {var} for the embedding provider API key")]
    MissingApiKey { var: String },
    #[error("embedding service returned no vectors")]
    MissingVector,
    #[error("embedding vector length {actual} does not match requested dims {expected}")]
    InvalidDimensions { expected: u64, actual: u64 },
    #[error("unsupported embedding encoding {0}; the first MVP supports f32le and f16le")]
    UnsupportedEncoding(String),
    #[error("embedding request failed after {attempts} attempts: {message}")]
    RequestFailed { attempts: u32, message: String },
    #[error("Azure OpenAI embedding provider is not implemented in the first MVP")]
    UnsupportedProduction,
}

#[derive(Debug, Serialize)]
struct EmbeddingRequestBody<'a> {
    input: &'a str,
    model: &'a str,
}

#[derive(Debug, Deserialize)]
struct EmbeddingResponseBody {
    data: Vec<EmbeddingResponseItem>,
}

#[derive(Debug, Deserialize)]
struct EmbeddingResponseItem {
    embedding: Vec<f32>,
}

impl ConfiguredEmbeddingProvider {
    pub fn from_environment(
        environment: &EnvironmentConfig,
    ) -> Result<Self, ConfiguredEmbeddingProviderError> {
        match environment {
            EnvironmentConfig::Local { embedding, .. } => Ok(Self::Local(
                LocalOpenAiEmbeddingProvider::from_config(embedding)?,
            )),
            EnvironmentConfig::Production { .. } => {
                Ok(Self::AzureOpenAi(AzureOpenAiEmbeddingProviderStub))
            }
        }
    }
}

impl LocalOpenAiEmbeddingProvider {
    pub fn from_config(
        config: &LocalEmbeddingConfig,
    ) -> Result<Self, ConfiguredEmbeddingProviderError> {
        let api_key = match &config.api_key_env {
            Some(var) => Some(env::var(var).map_err(|_| {
                ConfiguredEmbeddingProviderError::MissingApiKey { var: var.clone() }
            })?),
            None => None,
        };
        let client = Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .build()
            .map_err(ConfiguredEmbeddingProviderError::ClientBuild)?;

        Ok(Self {
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            model: config.model.clone(),
            api_key,
            max_retries: config.max_retries,
            retry_delay: Duration::from_millis(config.retry_delay_ms),
        })
    }

    async fn embed_impl(
        &self,
        input: &EmbeddingInput,
        spec: &EmbeddingSpec,
    ) -> Result<Vec<u8>, ConfiguredEmbeddingProviderError> {
        let text = String::from_utf8_lossy(&input.body);
        let endpoint = format!("{}/v1/embeddings", self.base_url);
        let request_body = EmbeddingRequestBody {
            input: &text,
            model: &self.model,
        };

        let max_attempts = self.max_retries + 1;
        let mut attempts_made = 0;
        let mut last_error = String::new();
        for attempt in 1..=max_attempts {
            attempts_made = attempt;
            let mut request = self.client.post(&endpoint).json(&request_body);
            if let Some(api_key) = &self.api_key {
                request = request.bearer_auth(api_key);
            }

            match request.send().await {
                Ok(response) => match response.error_for_status() {
                    Ok(success) => {
                        let parsed: EmbeddingResponseBody =
                            success.json().await.map_err(|error| {
                                ConfiguredEmbeddingProviderError::RequestFailed {
                                    attempts: attempt,
                                    message: error.to_string(),
                                }
                            })?;
                        let vector = parsed
                            .data
                            .into_iter()
                            .next()
                            .ok_or(ConfiguredEmbeddingProviderError::MissingVector)?
                            .embedding;
                        return encode_embedding(&vector, spec);
                    }
                    Err(error) => {
                        let status = error.status();
                        last_error = error.to_string();
                        if !should_retry(status, attempt, max_attempts) {
                            break;
                        }
                    }
                },
                Err(error) => {
                    last_error = error.to_string();
                    if attempt >= max_attempts {
                        break;
                    }
                }
            }

            tokio::time::sleep(self.retry_delay).await;
        }

        Err(ConfiguredEmbeddingProviderError::RequestFailed {
            attempts: attempts_made,
            message: last_error,
        })
    }
}

impl EmbeddingProvider for ConfiguredEmbeddingProvider {
    type Error = ConfiguredEmbeddingProviderError;

    async fn embed(
        &self,
        input: &EmbeddingInput,
        spec: &EmbeddingSpec,
    ) -> Result<Vec<u8>, Self::Error> {
        match self {
            Self::Local(provider) => provider.embed_impl(input, spec).await,
            Self::AzureOpenAi(_) => Err(ConfiguredEmbeddingProviderError::UnsupportedProduction),
        }
    }
}

fn should_retry(status: Option<StatusCode>, attempt: u32, max_attempts: u32) -> bool {
    if attempt >= max_attempts {
        return false;
    }

    matches!(
        status,
        Some(StatusCode::TOO_MANY_REQUESTS)
            | Some(StatusCode::BAD_GATEWAY)
            | Some(StatusCode::SERVICE_UNAVAILABLE)
            | Some(StatusCode::GATEWAY_TIMEOUT)
            | None
    )
}

fn encode_embedding(
    vector: &[f32],
    spec: &EmbeddingSpec,
) -> Result<Vec<u8>, ConfiguredEmbeddingProviderError> {
    let actual = vector.len() as u64;
    if actual != spec.dims {
        return Err(ConfiguredEmbeddingProviderError::InvalidDimensions {
            expected: spec.dims,
            actual,
        });
    }

    match spec.encoding.as_str() {
        "f32le" => Ok(vector
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()),
        "f16le" => Ok(vector
            .iter()
            .flat_map(|value| f16::from_f32(*value).to_le_bytes())
            .collect()),
        other => Err(ConfiguredEmbeddingProviderError::UnsupportedEncoding(
            other.to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Instant;

    use super::*;

    #[tokio::test]
    async fn local_provider_posts_openai_compatible_requests() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server = spawn_test_server(
            vec![http_response(200, r#"{"data":[{"embedding":[0.5,-1.0]}]}"#)],
            Arc::clone(&requests),
        );
        let provider = LocalOpenAiEmbeddingProvider::from_config(&LocalEmbeddingConfig {
            base_url: server.base_url.clone(),
            model: "all-MiniLM-L6-v2".into(),
            api_key_env: None,
            request_timeout_secs: 5,
            max_retries: 0,
            retry_delay_ms: 1,
        })
        .unwrap();

        let bytes = provider
            .embed_impl(
                &EmbeddingInput {
                    media_type: "text/plain".into(),
                    body: b"hello world".to_vec(),
                },
                &EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
            )
            .await
            .unwrap();

        assert_eq!(bytes.len(), 8);
        let request_log = requests.lock().unwrap();
        assert!(request_log[0].contains("POST /v1/embeddings HTTP/1.1"));
        assert!(request_log[0].contains("\"model\":\"all-MiniLM-L6-v2\""));
        server.join();
    }

    #[tokio::test]
    async fn local_provider_retries_transient_failures() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server = spawn_test_server(
            vec![
                http_response(503, r#"{"error":"warming up"}"#),
                http_response(200, r#"{"data":[{"embedding":[1.0,2.0]}]}"#),
            ],
            Arc::clone(&requests),
        );
        let provider = LocalOpenAiEmbeddingProvider::from_config(&LocalEmbeddingConfig {
            base_url: server.base_url.clone(),
            model: "all-MiniLM-L6-v2".into(),
            api_key_env: None,
            request_timeout_secs: 5,
            max_retries: 1,
            retry_delay_ms: 1,
        })
        .unwrap();

        let bytes = provider
            .embed_impl(
                &EmbeddingInput {
                    media_type: "text/plain".into(),
                    body: b"retry".to_vec(),
                },
                &EmbeddingSpec {
                    dims: 2,
                    encoding: "f16le".into(),
                },
            )
            .await
            .unwrap();

        assert_eq!(bytes.len(), 4);
        assert_eq!(requests.lock().unwrap().len(), 2);
        server.join();
    }

    #[tokio::test]
    async fn production_provider_stub_returns_explicit_error() {
        let provider = ConfiguredEmbeddingProvider::AzureOpenAi(AzureOpenAiEmbeddingProviderStub);
        let error = provider
            .embed(
                &EmbeddingInput {
                    media_type: "text/plain".into(),
                    body: b"ignored".to_vec(),
                },
                &EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ConfiguredEmbeddingProviderError::UnsupportedProduction
        ));
    }

    #[tokio::test]
    async fn local_provider_reports_actual_attempt_count_on_non_retryable_failure() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server = spawn_test_server(
            vec![http_response(400, r#"{"error":"bad request"}"#)],
            Arc::clone(&requests),
        );
        let provider = LocalOpenAiEmbeddingProvider::from_config(&LocalEmbeddingConfig {
            base_url: server.base_url.clone(),
            model: "all-MiniLM-L6-v2".into(),
            api_key_env: None,
            request_timeout_secs: 5,
            max_retries: 10,
            retry_delay_ms: 1,
        })
        .unwrap();

        let error = provider
            .embed_impl(
                &EmbeddingInput {
                    media_type: "text/plain".into(),
                    body: b"bad request".to_vec(),
                },
                &EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            ConfiguredEmbeddingProviderError::RequestFailed { attempts: 1, .. }
        ));
        assert_eq!(requests.lock().unwrap().len(), 1);
        server.join();
    }

    struct TestServer {
        base_url: String,
        handle: thread::JoinHandle<()>,
    }

    impl TestServer {
        fn join(self) {
            self.handle.join().unwrap();
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

    fn spawn_test_server(responses: Vec<String>, requests: Arc<Mutex<Vec<String>>>) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut response_iter = responses.into_iter();
            while Instant::now() < deadline {
                let Some(response) = response_iter.next() else {
                    break;
                };
                let (mut stream, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                        response_iter = std::iter::once(response)
                            .chain(response_iter)
                            .collect::<Vec<_>>()
                            .into_iter();
                        continue;
                    }
                    Err(error) => panic!("failed to accept embedding test connection: {error}"),
                };
                stream.set_nonblocking(true).unwrap();
                let mut request = Vec::new();
                let mut buffer = [0u8; 1024];
                let request_deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    if request_is_complete(&request) {
                        break;
                    }
                    if Instant::now() >= request_deadline {
                        panic!("timed out waiting for embedding test request body");
                    }
                    match stream.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(read) => {
                            request.extend_from_slice(&buffer[..read]);
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                            break;
                        }
                        Err(error) => panic!("failed to read test request: {error}"),
                    }
                }
                requests
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&request).to_string());
                stream.write_all(response.as_bytes()).unwrap();
                stream.flush().unwrap();
            }
        });
        ready_rx.recv().unwrap();

        TestServer {
            base_url: format!("http://{}", address),
            handle,
        }
    }

    fn http_response(status: u16, body: &str) -> String {
        format!(
            "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }
}
