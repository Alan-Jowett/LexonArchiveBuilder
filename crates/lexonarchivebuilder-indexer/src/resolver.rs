// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use ciborium::Value;
use lexongraph_block::{BlockHash, Content, DecodedBlock, v2};
use lexongraph_block_store::BlockStoreExt;
use lexongraph_streaming_indexer::ContentResolver;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::block_store::ConfiguredBlockStore;
use crate::mailbox::{
    CHUNK_MEDIA_TYPE, NORMALIZED_EMAIL_ARTIFACT_BLOCK_TYPE, NORMALIZED_EMAIL_SCHEMA_VERSION,
    chunk_email_core,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContentRef {
    Document {
        path: PathBuf,
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

#[derive(Clone, Debug)]
pub struct LocalFilesystemContentResolver {
    block_store: ConfiguredBlockStore,
    email_chunk_cache: Arc<Mutex<HashMap<String, Arc<Vec<String>>>>>,
}

type EmailChunkCache = Arc<Mutex<HashMap<String, Arc<Vec<String>>>>>;

impl LocalFilesystemContentResolver {
    pub fn new(block_store: ConfiguredBlockStore) -> Self {
        Self {
            block_store,
            email_chunk_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Debug, Error)]
pub enum LocalFilesystemContentResolverError {
    #[error("content source {path} does not exist")]
    Missing { path: PathBuf },
    #[error("content source {path} is not a file")]
    NotAFile { path: PathBuf },
    #[error("content source {path} must use the .{expected} extension")]
    WrongExtension {
        path: PathBuf,
        expected: &'static str,
    },
    #[error("failed to read content source {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("artifact block id {value} is not a valid hex block hash")]
    InvalidBlockHash { value: String },
    #[error(transparent)]
    BlockStore(#[from] lexongraph_block_store::BlockStoreError),
    #[error("artifact block {block_id} was not found")]
    MissingArtifact { block_id: String },
    #[error("artifact block {block_id} uses unsupported legacy block version {version}")]
    ArtifactLegacyVersion { block_id: String, version: u64 },
    #[error("artifact block {block_id} has unexpected v2 block type {type_name}")]
    ArtifactWrongType { block_id: String, type_name: String },
    #[error("artifact block {block_id} does not contain normalized email payload bytes")]
    ArtifactMissingContent { block_id: String },
    #[error("artifact block {block_id} has unexpected media type {media_type}")]
    ArtifactWrongMediaType {
        block_id: String,
        media_type: String,
    },
    #[error("artifact block {block_id} could not be decoded: {message}")]
    ArtifactDecode { block_id: String, message: String },
    #[error("artifact block {block_id} does not contain a text body field")]
    ArtifactMissingBody { block_id: String },
    #[error("artifact block {block_id} does not contain chunk index {chunk_index}")]
    MissingChunk {
        block_id: String,
        chunk_index: usize,
    },
}

impl ContentResolver<ContentRef> for LocalFilesystemContentResolver {
    type Error = LocalFilesystemContentResolverError;

    fn resolve(&self, content_ref: &ContentRef) -> Result<Content, Self::Error> {
        match content_ref {
            ContentRef::Document { path } => resolve_file(path, "txt", "text/plain"),
            ContentRef::Inline { media_type, body } => Ok(Content {
                media_type: media_type.clone(),
                body: body.clone(),
            }),
            ContentRef::EmailChunk {
                email_artifact_ref,
                chunk_index,
            } => resolve_email_chunk(
                &self.block_store,
                &self.email_chunk_cache,
                email_artifact_ref,
                *chunk_index,
            ),
        }
    }

    fn fingerprint(&self, content_ref: &ContentRef) -> Result<BlockHash, Self::Error> {
        let fingerprint = match content_ref {
            ContentRef::Document { path } => hash_bytes(
                format!("document:{}", path.to_string_lossy().replace('\\', "/")).as_bytes(),
            ),
            ContentRef::Inline { media_type, body } => {
                let mut bytes = media_type.as_bytes().to_vec();
                bytes.push(0);
                bytes.extend_from_slice(body);
                hash_bytes(&bytes)
            }
            ContentRef::EmailChunk {
                email_artifact_ref,
                chunk_index,
            } => hash_bytes(format!("email-chunk:{email_artifact_ref}:{chunk_index}").as_bytes()),
        };
        Ok(fingerprint)
    }
}

fn resolve_file(
    path: &PathBuf,
    expected_extension: &'static str,
    media_type: &str,
) -> Result<Content, LocalFilesystemContentResolverError> {
    if !path.exists() {
        return Err(LocalFilesystemContentResolverError::Missing { path: path.clone() });
    }
    if !path.is_file() {
        return Err(LocalFilesystemContentResolverError::NotAFile { path: path.clone() });
    }
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if !extension.eq_ignore_ascii_case(expected_extension) {
        return Err(LocalFilesystemContentResolverError::WrongExtension {
            path: path.clone(),
            expected: expected_extension,
        });
    }

    let raw_bytes = fs::read(path).map_err(|source| LocalFilesystemContentResolverError::Io {
        path: path.clone(),
        source,
    })?;
    let body = match String::from_utf8(raw_bytes) {
        Ok(text) => text.into_bytes(),
        Err(error) => String::from_utf8_lossy(&error.into_bytes())
            .into_owned()
            .into_bytes(),
    };

    Ok(Content {
        media_type: media_type.to_string(),
        body,
    })
}

fn resolve_email_chunk(
    store: &ConfiguredBlockStore,
    cache: &EmailChunkCache,
    email_artifact_ref: &str,
    chunk_index: usize,
) -> Result<Content, LocalFilesystemContentResolverError> {
    let chunks = {
        let cache_guard = cache.lock().unwrap();
        cache_guard.get(email_artifact_ref).cloned()
    };
    let chunks = match chunks {
        Some(chunks) => chunks,
        None => {
            let chunks = Arc::new(load_email_chunks(store, email_artifact_ref)?);
            let mut cache_guard = cache.lock().unwrap();
            Arc::clone(
                cache_guard
                    .entry(email_artifact_ref.to_string())
                    .or_insert_with(|| Arc::clone(&chunks)),
            )
        }
    };
    let Some(chunk) = chunks.get(chunk_index) else {
        return Err(LocalFilesystemContentResolverError::MissingChunk {
            block_id: email_artifact_ref.to_string(),
            chunk_index,
        });
    };

    Ok(Content {
        media_type: CHUNK_MEDIA_TYPE.to_string(),
        body: chunk.as_bytes().to_vec(),
    })
}

fn load_email_chunks(
    store: &ConfiguredBlockStore,
    email_artifact_ref: &str,
) -> Result<Vec<String>, LocalFilesystemContentResolverError> {
    let block_id = parse_block_hash(email_artifact_ref)?;
    let Some(decoded) = store.get_decoded(&block_id)? else {
        return Err(LocalFilesystemContentResolverError::MissingArtifact {
            block_id: email_artifact_ref.to_string(),
        });
    };
    let validated = match decoded {
        DecodedBlock::V1(_) => {
            return Err(LocalFilesystemContentResolverError::ArtifactLegacyVersion {
                block_id: email_artifact_ref.to_string(),
                version: 1,
            });
        }
        DecodedBlock::V2(validated) => validated,
    };
    let custom = match v2::into_typed_block(validated).map_err(|error| {
        LocalFilesystemContentResolverError::ArtifactDecode {
            block_id: email_artifact_ref.to_string(),
            message: error.to_string(),
        }
    })? {
        v2::TypedBlock::Custom(custom) => custom,
        other => {
            return Err(LocalFilesystemContentResolverError::ArtifactWrongType {
                block_id: email_artifact_ref.to_string(),
                type_name: typed_block_name(&other).to_string(),
            });
        }
    };
    if custom.type_name != NORMALIZED_EMAIL_ARTIFACT_BLOCK_TYPE {
        return Err(LocalFilesystemContentResolverError::ArtifactWrongType {
            block_id: email_artifact_ref.to_string(),
            type_name: custom.type_name,
        });
    }
    let (media_type, body) = custom_block_payload(&custom.content, email_artifact_ref)?;
    if media_type != "application/vnd.lexonarchivebuilder.normalized-email+cbor" {
        return Err(
            LocalFilesystemContentResolverError::ArtifactWrongMediaType {
                block_id: email_artifact_ref.to_string(),
                media_type,
            },
        );
    }
    let body = normalized_email_body(&body, email_artifact_ref)?;
    Ok(chunk_email_core(&body))
}

fn normalized_email_body(
    bytes: &[u8],
    block_id: &str,
) -> Result<String, LocalFilesystemContentResolverError> {
    let value: Value = ciborium::de::from_reader(Cursor::new(bytes)).map_err(|error| {
        LocalFilesystemContentResolverError::ArtifactDecode {
            block_id: block_id.to_string(),
            message: error.to_string(),
        }
    })?;
    let Value::Map(fields) = value else {
        return Err(LocalFilesystemContentResolverError::ArtifactDecode {
            block_id: block_id.to_string(),
            message: "normalized email artifact must decode to a CBOR map".into(),
        });
    };
    fields
        .iter()
        .find_map(|(key, value)| match (key, value) {
            (Value::Text(name), Value::Text(body)) if name == "body" => Some(body.clone()),
            _ => None,
        })
        .ok_or_else(
            || LocalFilesystemContentResolverError::ArtifactMissingBody {
                block_id: block_id.to_string(),
            },
        )
        .and_then(|body| {
            fields
                .iter()
                .find_map(|(key, value)| match (key, value) {
                    (Value::Text(name), Value::Integer(version)) if name == "schema_version" => {
                        u64::try_from(*version).ok()
                    }
                    _ => None,
                })
                .filter(|version| *version == NORMALIZED_EMAIL_SCHEMA_VERSION)
                .ok_or_else(|| LocalFilesystemContentResolverError::ArtifactDecode {
                    block_id: block_id.to_string(),
                    message: format!(
                        "normalized email artifact must use schema_version {}",
                        NORMALIZED_EMAIL_SCHEMA_VERSION
                    ),
                })?;
            Ok(body)
        })
}

fn custom_block_payload(
    content: &Value,
    block_id: &str,
) -> Result<(String, Vec<u8>), LocalFilesystemContentResolverError> {
    let Value::Map(fields) = content else {
        return Err(LocalFilesystemContentResolverError::ArtifactDecode {
            block_id: block_id.to_string(),
            message: "artifact custom block content must be a CBOR map".into(),
        });
    };
    let media_type = fields
        .iter()
        .find_map(|(key, value)| match (key, value) {
            (Value::Text(name), Value::Text(media_type)) if name == "media_type" => {
                Some(media_type.clone())
            }
            _ => None,
        })
        .ok_or_else(
            || LocalFilesystemContentResolverError::ArtifactWrongMediaType {
                block_id: block_id.to_string(),
                media_type: "<missing>".into(),
            },
        )?;
    let body = fields
        .iter()
        .find_map(|(key, value)| match (key, value) {
            (Value::Text(name), Value::Bytes(body)) if name == "body" => Some(body.clone()),
            _ => None,
        })
        .ok_or_else(
            || LocalFilesystemContentResolverError::ArtifactMissingContent {
                block_id: block_id.to_string(),
            },
        )?;
    Ok((media_type, body))
}

fn typed_block_name(block: &v2::TypedBlock) -> &str {
    match block {
        v2::TypedBlock::Branch(branch) => &branch.type_name,
        v2::TypedBlock::Leaf(leaf) => &leaf.type_name,
        v2::TypedBlock::Custom(custom) => &custom.type_name,
    }
}

fn parse_block_hash(value: &str) -> Result<BlockHash, LocalFilesystemContentResolverError> {
    if value.len() != BlockHash::LEN * 2 {
        return Err(LocalFilesystemContentResolverError::InvalidBlockHash {
            value: value.to_string(),
        });
    }

    let mut bytes = [0u8; BlockHash::LEN];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(chunk[0]).ok_or_else(|| {
            LocalFilesystemContentResolverError::InvalidBlockHash {
                value: value.to_string(),
            }
        })?;
        let low = decode_hex_nibble(chunk[1]).ok_or_else(|| {
            LocalFilesystemContentResolverError::InvalidBlockHash {
                value: value.to_string(),
            }
        })?;
        bytes[index] = (high << 4) | low;
    }

    Ok(BlockHash::from_bytes(bytes))
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn hash_bytes(bytes: &[u8]) -> BlockHash {
    BlockHash::from_bytes(Sha256::digest(bytes).into())
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::fs;

    use lexongraph_block::{BlockHash, Content, VersionedBlock, v2};
    use lexongraph_block_store::BlockStoreExt;
    use lexongraph_streaming_indexer::{IndexItem, Metadata, conformance};
    use tempfile::tempdir;

    use super::*;
    use crate::block_store::ConfiguredBlockStore;

    #[derive(Clone)]
    struct ResolverHarness {
        resolver: LocalFilesystemContentResolver,
        content_ref: ContentRef,
        expected_content: Content,
    }

    impl conformance::ContentResolverConformanceHarness for ResolverHarness {
        type Ref = ContentRef;
        type Resolver = ResolverMode;

        fn sample_item(&self) -> IndexItem<Self::Ref> {
            IndexItem {
                metadata: Metadata::default(),
                content_ref: self.content_ref.clone(),
            }
        }

        fn expected_content(&self) -> Content {
            self.expected_content.clone()
        }

        fn conforming_resolver(&self) -> Self::Resolver {
            ResolverMode::Working(Box::new(self.resolver.clone()))
        }

        fn failing_resolver(&self) -> Self::Resolver {
            ResolverMode::Failing
        }

        fn unusable_resolver(&self) -> Self::Resolver {
            ResolverMode::Unusable
        }
    }

    #[derive(Clone)]
    enum ResolverMode {
        Working(Box<LocalFilesystemContentResolver>),
        Failing,
        Unusable,
    }

    #[derive(Debug)]
    struct ResolverModeError(&'static str);

    impl fmt::Display for ResolverModeError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for ResolverModeError {}

    impl ContentResolver<ContentRef> for ResolverMode {
        type Error = ResolverModeError;

        fn resolve(&self, content_ref: &ContentRef) -> Result<Content, Self::Error> {
            match self {
                Self::Working(resolver) => resolver
                    .resolve(content_ref)
                    .map_err(|_| ResolverModeError("resolver failure")),
                Self::Failing => Err(ResolverModeError("expected failure")),
                Self::Unusable => Ok(Content {
                    media_type: String::new(),
                    body: b"".to_vec(),
                }),
            }
        }

        fn fingerprint(&self, content_ref: &ContentRef) -> Result<BlockHash, Self::Error> {
            match self {
                Self::Working(resolver) => resolver
                    .fingerprint(content_ref)
                    .map_err(|_| ResolverModeError("resolver failure")),
                Self::Failing => Err(ResolverModeError("expected failure")),
                Self::Unusable => Ok(BlockHash::from_bytes([0u8; BlockHash::LEN])),
            }
        }
    }

    #[test]
    fn inline_resolver_passes_conformance_suite() {
        let dir = tempdir().unwrap();
        let resolver = LocalFilesystemContentResolver::new(local_store(dir.path().join("blocks")));
        let harness = ResolverHarness {
            resolver,
            content_ref: ContentRef::Inline {
                media_type: "text/plain".into(),
                body: b"Inline email chunk".to_vec(),
            },
            expected_content: Content {
                media_type: "text/plain".into(),
                body: b"Inline email chunk".to_vec(),
            },
        };

        conformance::run_content_resolver_suite(&harness).unwrap();
    }

    #[test]
    fn document_resolver_passes_conformance_suite() {
        let dir = tempdir().unwrap();
        let document_path = dir.path().join("readme.txt");
        let body = b"LexonArchiveBuilder document body\n";
        fs::write(&document_path, body).unwrap();
        let resolver = LocalFilesystemContentResolver::new(local_store(dir.path().join("blocks")));
        let harness = ResolverHarness {
            resolver,
            content_ref: ContentRef::Document {
                path: document_path,
            },
            expected_content: Content {
                media_type: "text/plain".into(),
                body: body.to_vec(),
            },
        };

        conformance::run_content_resolver_suite(&harness).unwrap();
    }

    #[test]
    fn document_resolver_rejects_non_txt_documents() {
        let dir = tempdir().unwrap();
        let document_path = dir.path().join("readme.md");
        fs::write(&document_path, b"markdown").unwrap();
        let resolver = LocalFilesystemContentResolver::new(local_store(dir.path().join("blocks")));

        let error = resolver
            .resolve(&ContentRef::Document {
                path: document_path.clone(),
            })
            .unwrap_err();

        assert!(matches!(
            error,
            LocalFilesystemContentResolverError::WrongExtension { path, expected }
                if path == document_path && expected == "txt"
        ));
    }

    #[test]
    fn inline_resolver_preserves_provided_bytes() {
        let dir = tempdir().unwrap();
        let resolver = LocalFilesystemContentResolver::new(local_store(dir.path().join("blocks")));
        let content = resolver
            .resolve(&ContentRef::Inline {
                media_type: "text/plain".into(),
                body: vec![0x66, 0x6f, 0x80, 0x6f],
            })
            .unwrap();

        assert_eq!(content.body, vec![0x66, 0x6f, 0x80, 0x6f]);
    }

    #[test]
    fn email_chunk_resolution_caches_decoded_chunks_per_artifact() {
        let dir = tempdir().unwrap();
        let store = local_store(dir.path().join("blocks"));
        let email_artifact_ref = normalized_email_artifact_ref(
            &store,
            "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu",
        );
        let resolver = LocalFilesystemContentResolver::new(store);

        let first = resolver
            .resolve(&ContentRef::EmailChunk {
                email_artifact_ref: email_artifact_ref.clone(),
                chunk_index: 0,
            })
            .unwrap();
        let second = resolver
            .resolve(&ContentRef::EmailChunk {
                email_artifact_ref,
                chunk_index: 0,
            })
            .unwrap();

        assert_eq!(first, second);
        assert_eq!(resolver.email_chunk_cache.lock().unwrap().len(), 1);
    }

    fn local_store(path: PathBuf) -> ConfiguredBlockStore {
        ConfiguredBlockStore::Local(
            lexongraph_block_store_fs::FilesystemBlockStore::new(path).unwrap(),
        )
    }

    fn normalized_email_artifact_ref(store: &ConfiguredBlockStore, body: &str) -> String {
        let mut encoded = Vec::new();
        ciborium::ser::into_writer(
            &Value::Map(vec![
                (
                    Value::Text("schema_version".into()),
                    Value::Integer(NORMALIZED_EMAIL_SCHEMA_VERSION.into()),
                ),
                (Value::Text("body".into()), Value::Text(body.to_string())),
            ]),
            &mut encoded,
        )
        .unwrap();
        let block = v2::build_custom_block(
            NORMALIZED_EMAIL_ARTIFACT_BLOCK_TYPE,
            Value::Map(vec![
                (
                    Value::Text("media_type".into()),
                    Value::Text("application/vnd.lexonarchivebuilder.normalized-email+cbor".into()),
                ),
                (Value::Text("body".into()), Value::Bytes(encoded)),
            ]),
        )
        .unwrap();
        store
            .put_versioned(&VersionedBlock::V2(block))
            .unwrap()
            .to_string()
    }
}
