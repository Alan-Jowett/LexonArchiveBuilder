use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use ciborium::Value;
use lexongraph_block::{
    Block, BlockHash, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_leaf_block,
};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_indexer::{IndexItem, Metadata};
use mailparse::{MailHeaderMap, ParsedMail, parse_mail};
use thiserror::Error;

use crate::config::{BatchItemConfig, BatchRequest, metadata_to_lexongraph};
use crate::paths::resolve_path;
use crate::resolver::ContentRef;

const ARTIFACT_EMBEDDING_ENCODING: &str = "f32le";
const ARTIFACT_MEDIA_TYPE_MAILBOX: &str = "application/mbox";
const ARTIFACT_MEDIA_TYPE_NORMALIZED_EMAIL: &str =
    "application/vnd.lexonarchivebuilder.normalized-email+cbor";
const CHUNK_MEDIA_TYPE: &str = "text/plain";
const NORMALIZED_EMAIL_SCHEMA_VERSION: u64 = 1;
const MAX_CHUNK_CHARS: usize = 1_000;

#[derive(Debug, Error)]
pub enum MailboxExpansionError {
    #[error("mailbox source {path} does not exist")]
    Missing { path: PathBuf },
    #[error("mailbox source {path} is not a file")]
    NotAFile { path: PathBuf },
    #[error("mailbox source {path} must use the .mail or .mbox extension")]
    WrongExtension { path: PathBuf },
    #[error("failed to read mailbox source {path}: {source}")]
    ReadMailbox {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("mailbox source {path} did not contain any messages")]
    EmptyMailbox { path: PathBuf },
    #[error("failed to parse message {message_index} from mailbox {path}: {source}")]
    ParseMessage {
        path: PathBuf,
        message_index: usize,
        #[source]
        source: mailparse::MailParseError,
    },
    #[error("failed to encode normalized email artifact for mailbox {path}: {message}")]
    EncodeArtifact { path: PathBuf, message: String },
    #[error("failed to build artifact block for mailbox {path}: {source}")]
    BuildArtifact {
        path: PathBuf,
        #[source]
        source: lexongraph_block::BlockError,
    },
    #[error("failed to store artifact block for mailbox {path}: {source}")]
    StoreArtifact {
        path: PathBuf,
        #[source]
        source: BlockStoreError,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NormalizedHeader {
    name: String,
    value: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NormalizedEmailArtifact {
    schema_version: u64,
    mailbox_artifact_ref: String,
    mailbox_message_index: usize,
    headers: Vec<NormalizedHeader>,
    subject: Option<String>,
    from: Option<String>,
    recipient_context: Option<String>,
    date: Option<String>,
    message_id: Option<String>,
    body: String,
}

#[derive(Debug)]
pub(crate) struct MailboxExpansion {
    pub(crate) items: Vec<IndexItem<ContentRef>>,
    pub(crate) message_count: usize,
}

pub fn expand_batch_items(
    request_dir: &Path,
    request: &BatchRequest,
    store: &dyn BlockStore,
) -> Result<Vec<IndexItem<ContentRef>>, MailboxExpansionError> {
    let mut items = request.to_document_index_items(request_dir);
    for item in &request.items {
        if let BatchItemConfig::Mailbox { path, metadata } = item {
            let resolved = resolve_path(request_dir, path);
            items.extend(expand_mailbox_item(&resolved, metadata, store)?);
        }
    }
    Ok(items)
}

pub(crate) fn expand_mailbox_item_with_stats(
    path: &Path,
    metadata: &BTreeMap<String, String>,
    store: &dyn BlockStore,
) -> Result<MailboxExpansion, MailboxExpansionError> {
    validate_mailbox_path(path)?;
    let raw_bytes = fs::read(path).map_err(|source| MailboxExpansionError::ReadMailbox {
        path: path.to_path_buf(),
        source,
    })?;
    let messages = split_mbox_messages(&raw_bytes);
    if messages.is_empty() {
        return Err(MailboxExpansionError::EmptyMailbox {
            path: path.to_path_buf(),
        });
    }
    let mailbox_artifact_ref =
        store_artifact_block(store, path, ARTIFACT_MEDIA_TYPE_MAILBOX, raw_bytes)?;

    let mut items = Vec::new();
    for (message_index, message) in messages.iter().enumerate() {
        let parsed = parse_mail(message).map_err(|source| MailboxExpansionError::ParseMessage {
            path: path.to_path_buf(),
            message_index,
            source,
        })?;
        let normalized = normalize_email(&parsed, mailbox_artifact_ref.to_string(), message_index);
        let normalized_bytes = render_normalized_email(path, &normalized)?;
        let email_artifact_ref = store_artifact_block(
            store,
            path,
            ARTIFACT_MEDIA_TYPE_NORMALIZED_EMAIL,
            normalized_bytes,
        )?;
        let chunks = chunk_email_core(&normalized.body);
        for (chunk_index, chunk) in chunks.into_iter().enumerate() {
            let chunk_locator = format!("{email_artifact_ref}:{chunk_index}");
            let chunk_metadata = build_chunk_metadata(
                metadata,
                path,
                &mailbox_artifact_ref.to_string(),
                &email_artifact_ref.to_string(),
                &normalized,
                chunk_index,
                &chunk_locator,
            );
            items.push(IndexItem {
                metadata: chunk_metadata,
                content_ref: ContentRef::Inline {
                    media_type: CHUNK_MEDIA_TYPE.to_string(),
                    body: chunk.into_bytes(),
                },
            });
        }
    }

    Ok(MailboxExpansion {
        items,
        message_count: messages.len(),
    })
}

fn expand_mailbox_item(
    path: &Path,
    metadata: &BTreeMap<String, String>,
    store: &dyn BlockStore,
) -> Result<Vec<IndexItem<ContentRef>>, MailboxExpansionError> {
    Ok(expand_mailbox_item_with_stats(path, metadata, store)?.items)
}

fn validate_mailbox_path(path: &Path) -> Result<(), MailboxExpansionError> {
    if !path.exists() {
        return Err(MailboxExpansionError::Missing {
            path: path.to_path_buf(),
        });
    }
    if !path.is_file() {
        return Err(MailboxExpansionError::NotAFile {
            path: path.to_path_buf(),
        });
    }
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if !is_supported_mailbox_extension(extension) {
        return Err(MailboxExpansionError::WrongExtension {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn is_supported_mailbox_extension(extension: &str) -> bool {
    extension.eq_ignore_ascii_case("mail") || extension.eq_ignore_ascii_case("mbox")
}

fn normalize_email(
    parsed: &ParsedMail<'_>,
    mailbox_artifact_ref: String,
    mailbox_message_index: usize,
) -> NormalizedEmailArtifact {
    let headers = parsed
        .headers
        .iter()
        .map(|header| NormalizedHeader {
            name: header.get_key(),
            value: normalize_inline_text(&header.get_value()),
        })
        .collect::<Vec<_>>();
    let subject = parsed
        .headers
        .get_first_value("Subject")
        .map(|value| normalize_inline_text(&value))
        .filter(|value| !value.is_empty());
    let from = parsed
        .headers
        .get_first_value("From")
        .map(|value| normalize_inline_text(&value))
        .filter(|value| !value.is_empty());
    let recipient_context = ["To", "Cc", "List-Id"]
        .into_iter()
        .find_map(|key| parsed.headers.get_first_value(key))
        .map(|value| normalize_inline_text(&value))
        .filter(|value| !value.is_empty());
    let date = parsed
        .headers
        .get_first_value("Date")
        .map(|value| normalize_inline_text(&value))
        .filter(|value| !value.is_empty());
    let message_id = parsed
        .headers
        .get_first_value("Message-ID")
        .map(|value| normalize_inline_text(&value))
        .filter(|value| !value.is_empty());
    let body = derive_email_core(parsed)
        .or_else(|| subject.clone())
        .unwrap_or_else(|| "(empty email body)".to_string());

    NormalizedEmailArtifact {
        schema_version: NORMALIZED_EMAIL_SCHEMA_VERSION,
        mailbox_artifact_ref,
        mailbox_message_index,
        headers,
        subject,
        from,
        recipient_context,
        date,
        message_id,
        body,
    }
}

fn derive_email_core(parsed: &ParsedMail<'_>) -> Option<String> {
    let raw_body = first_preferred_body(parsed)?;
    let normalized = normalize_email_body(&raw_body);
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn first_preferred_body(parsed: &ParsedMail<'_>) -> Option<String> {
    if parsed.subparts.is_empty() {
        return decoded_body_lossy(parsed).map(|body| normalize_line_endings(&body));
    }

    let mut html_body = None;
    for part in &parsed.subparts {
        if let Some(body) = preferred_leaf_body(part, &mut html_body) {
            return Some(body);
        }
    }
    html_body
}

fn preferred_leaf_body(parsed: &ParsedMail<'_>, html_body: &mut Option<String>) -> Option<String> {
    if parsed.subparts.is_empty() {
        let mimetype = parsed.ctype.mimetype.to_ascii_lowercase();
        if mimetype == "text/plain" {
            return decoded_body_lossy(parsed).map(|body| normalize_line_endings(&body));
        }
        if mimetype == "text/html" && html_body.is_none() {
            *html_body = decoded_body_lossy(parsed)
                .map(|body| strip_html_tags(&normalize_line_endings(&body)));
        }
        return None;
    }

    for part in &parsed.subparts {
        if let Some(body) = preferred_leaf_body(part, html_body) {
            return Some(body);
        }
    }
    None
}

fn normalize_email_body(body: &str) -> String {
    let mut normalized_lines = Vec::new();
    let mut previous_blank = false;
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            if !previous_blank && !normalized_lines.is_empty() {
                normalized_lines.push(String::new());
            }
            previous_blank = true;
            continue;
        }
        if looks_like_reply_intro(trimmed) || trimmed == "--" || trimmed.starts_with("-- ") {
            break;
        }
        if trimmed.starts_with('>') {
            continue;
        }

        normalized_lines.push(trimmed.to_string());
        previous_blank = false;
    }

    normalized_lines.join("\n").trim().to_string()
}

fn looks_like_reply_intro(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    lower.starts_with("on ") && lower.ends_with(" wrote:")
}

fn chunk_email_core(body: &str) -> Vec<String> {
    let units = sentence_units(body);
    if units.is_empty() {
        return vec![body.trim().to_string()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    for unit in units {
        let candidate_len = if current.is_empty() {
            unit.chars().count()
        } else {
            current.chars().count() + 1 + unit.chars().count()
        };
        if !current.is_empty() && candidate_len > MAX_CHUNK_CHARS {
            chunks.push(current);
            current = unit;
            continue;
        }

        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(&unit);
    }
    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
}

fn sentence_units(body: &str) -> Vec<String> {
    let mut sentences = Vec::new();
    let mut current = String::new();
    let mut blank_run = 0usize;
    for ch in body.chars() {
        if ch == '\n' {
            blank_run += 1;
        } else if !ch.is_whitespace() {
            blank_run = 0;
        }
        current.push(ch);
        if matches!(ch, '.' | '!' | '?') || blank_run >= 2 {
            let sentence = normalize_inline_text(&current);
            if !sentence.is_empty() {
                sentences.push(sentence);
            }
            current.clear();
            blank_run = 0;
        }
    }
    let trailing = normalize_inline_text(&current);
    if !trailing.is_empty() {
        sentences.push(trailing);
    }
    sentences
}

fn build_chunk_metadata(
    base_metadata: &BTreeMap<String, String>,
    mailbox_path: &Path,
    mailbox_artifact_ref: &str,
    email_artifact_ref: &str,
    artifact: &NormalizedEmailArtifact,
    chunk_index: usize,
    chunk_locator: &str,
) -> Metadata {
    let mut metadata = base_metadata.clone();
    metadata.insert(
        "mailbox_artifact_ref".into(),
        mailbox_artifact_ref.to_string(),
    );
    metadata.insert("email_artifact_ref".into(), email_artifact_ref.to_string());
    metadata.insert(
        "mailbox_message_index".into(),
        artifact.mailbox_message_index.to_string(),
    );
    metadata.insert("chunk_index".into(), chunk_index.to_string());
    metadata.insert("chunk_locator".into(), chunk_locator.to_string());
    if let Some(subject) = &artifact.subject {
        metadata.insert("email_subject".into(), subject.clone());
        metadata.insert("email_name".into(), subject.clone());
    }
    if let Some(from) = &artifact.from {
        metadata.insert("email_from".into(), from.clone());
    }
    if let Some(recipient_context) = &artifact.recipient_context {
        metadata.insert("email_recipient_context".into(), recipient_context.clone());
    }
    if let Some(date) = &artifact.date {
        metadata.insert("email_date".into(), date.clone());
    }
    if let Some(message_id) = &artifact.message_id {
        metadata.insert("email_message_id".into(), message_id.clone());
    }

    metadata_to_lexongraph(&metadata, "email", mailbox_path)
}

fn store_artifact_block(
    store: &dyn BlockStore,
    path: &Path,
    media_type: &str,
    body: Vec<u8>,
) -> Result<BlockHash, MailboxExpansionError> {
    let block = build_leaf_block(
        VERSION_1,
        artifact_embedding_spec(),
        vec![LeafEntry {
            embedding: Vec::new(),
            metadata: Vec::new(),
            content: Content {
                media_type: media_type.to_string(),
                body,
            },
        }],
        None,
    )
    .map_err(|source| MailboxExpansionError::BuildArtifact {
        path: path.to_path_buf(),
        source,
    })?;

    store
        .put(&Block::Leaf(block))
        .map_err(|source| MailboxExpansionError::StoreArtifact {
            path: path.to_path_buf(),
            source,
        })
}

fn artifact_embedding_spec() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 0,
        encoding: ARTIFACT_EMBEDDING_ENCODING.to_string(),
    }
}

fn render_normalized_email(
    path: &Path,
    artifact: &NormalizedEmailArtifact,
) -> Result<Vec<u8>, MailboxExpansionError> {
    let value = canonicalize_value(Value::Map(vec![
        (
            Value::Text("schema_version".into()),
            Value::Integer(artifact.schema_version.into()),
        ),
        (
            Value::Text("mailbox_artifact_ref".into()),
            Value::Text(artifact.mailbox_artifact_ref.clone()),
        ),
        (
            Value::Text("mailbox_message_index".into()),
            Value::Integer((artifact.mailbox_message_index as u64).into()),
        ),
        (
            Value::Text("headers".into()),
            Value::Array(
                artifact
                    .headers
                    .iter()
                    .map(|header| {
                        Value::Map(vec![
                            (Value::Text("name".into()), Value::Text(header.name.clone())),
                            (
                                Value::Text("value".into()),
                                Value::Text(header.value.clone()),
                            ),
                        ])
                    })
                    .collect(),
            ),
        ),
        option_text_field("subject", artifact.subject.as_ref()),
        option_text_field("from", artifact.from.as_ref()),
        option_text_field("recipient_context", artifact.recipient_context.as_ref()),
        option_text_field("date", artifact.date.as_ref()),
        option_text_field("message_id", artifact.message_id.as_ref()),
        (
            Value::Text("body".into()),
            Value::Text(artifact.body.clone()),
        ),
    ]))
    .map_err(|message| MailboxExpansionError::EncodeArtifact {
        path: path.to_path_buf(),
        message,
    })?;
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(&value, &mut bytes).map_err(|error| {
        MailboxExpansionError::EncodeArtifact {
            path: path.to_path_buf(),
            message: error.to_string(),
        }
    })?;
    Ok(bytes)
}

fn option_text_field(key: &str, value: Option<&String>) -> (Value, Value) {
    (
        Value::Text(key.to_string()),
        value
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
    )
}

fn canonicalize_value(value: Value) -> Result<Value, String> {
    match value {
        Value::Integer(_)
        | Value::Bytes(_)
        | Value::Float(_)
        | Value::Text(_)
        | Value::Bool(_)
        | Value::Null => Ok(value),
        Value::Tag(tag, nested) => Ok(Value::Tag(tag, Box::new(canonicalize_value(*nested)?))),
        Value::Array(values) => Ok(Value::Array(
            values
                .into_iter()
                .map(canonicalize_value)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Value::Map(entries) => {
            let mut normalized = entries
                .into_iter()
                .map(|(key, value)| Ok((canonicalize_value(key)?, canonicalize_value(value)?)))
                .collect::<Result<Vec<_>, String>>()?;
            normalized
                .sort_by(|(left_key, _), (right_key, _)| canonical_value_cmp(left_key, right_key));
            for pair in normalized.windows(2) {
                if canonical_value_cmp(&pair[0].0, &pair[1].0) == Ordering::Equal {
                    return Err("duplicate keys are not permitted in canonicalized CBOR".into());
                }
            }
            Ok(Value::Map(normalized))
        }
        other => Err(format!(
            "unsupported CBOR value encountered during canonicalization: {other:?}"
        )),
    }
}

fn canonical_value_cmp(left: &Value, right: &Value) -> Ordering {
    let left_bytes = encoded_value_bytes(left);
    let right_bytes = encoded_value_bytes(right);
    left_bytes
        .len()
        .cmp(&right_bytes.len())
        .then_with(|| left_bytes.cmp(&right_bytes))
}

fn encoded_value_bytes(value: &Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(value, &mut bytes)
        .expect("serializing a Value to bytes must succeed");
    bytes
}

fn split_mbox_messages(raw_bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut messages = Vec::new();
    let mut current = Vec::new();
    let mut saw_separator = false;
    for line in raw_bytes.split_inclusive(|byte| *byte == b'\n') {
        if line.starts_with(b"From ") {
            if !current.is_empty() {
                messages.push(std::mem::take(&mut current));
            }
            saw_separator = true;
            continue;
        }
        current.extend_from_slice(line);
    }
    if !current.is_empty() {
        messages.push(current);
    }

    if saw_separator {
        messages
    } else if raw_bytes.is_empty() {
        Vec::new()
    } else {
        vec![raw_bytes.to_vec()]
    }
}

fn normalize_line_endings(value: &str) -> String {
    value.replace("\r\n", "\n").replace('\r', "\n")
}

fn normalize_inline_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn strip_html_tags(value: &str) -> String {
    let mut result = String::new();
    let mut inside_tag = false;
    for ch in value.chars() {
        match ch {
            '<' => inside_tag = true,
            '>' => inside_tag = false,
            _ if !inside_tag => result.push(ch),
            _ => {}
        }
    }

    result
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
}

fn decoded_body_lossy(parsed: &ParsedMail<'_>) -> Option<String> {
    match parsed.get_body() {
        Ok(body) => Some(body),
        Err(_) => parsed
            .get_body_raw()
            .ok()
            .map(|body| String::from_utf8_lossy(&body).into_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        EmbeddingSpecConfig, EnvironmentConfig, ExecutionStage, LocalEmbeddingConfig,
    };
    use lexongraph_block_store_fs::FilesystemBlockStore;

    #[test]
    fn mailbox_expansion_stores_artifacts_and_emits_chunk_items() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox_path = dir.path().join("2026-01.mbox");
        fs::write(
            &mailbox_path,
            concat!(
                "From alan@example.com Sat Jan 03 10:00:00 2026\n",
                "Subject: LexonArchiveBuilder MVP\n",
                "From: Alan Example <alan@example.com>\n",
                "To: team@example.com\n",
                "Date: Sat, 03 Jan 2026 10:00:00 +0000\n",
                "Message-ID: <m1@example.com>\n",
                "\n",
                "This is the first sentence. This is the second sentence.\n"
            ),
        )
        .unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let items = expand_mailbox_item(
            &mailbox_path,
            &BTreeMap::from([("month".into(), "2026-01".into())]),
            &store,
        )
        .unwrap();

        assert_eq!(items.len(), 1);
        let metadata = metadata_to_text_map(&items[0].metadata);
        assert_eq!(metadata.get("source_kind"), Some(&"email".to_string()));
        assert_eq!(
            metadata.get("email_subject"),
            Some(&"LexonArchiveBuilder MVP".to_string())
        );
        assert_eq!(
            metadata.get("email_recipient_context"),
            Some(&"team@example.com".to_string())
        );
        assert!(metadata.contains_key("mailbox_artifact_ref"));
        assert!(metadata.contains_key("email_artifact_ref"));
        assert!(metadata.contains_key("chunk_locator"));
        match &items[0].content_ref {
            ContentRef::Inline { media_type, body } => {
                assert_eq!(media_type, CHUNK_MEDIA_TYPE);
                assert_eq!(
                    String::from_utf8(body.clone()).unwrap(),
                    "This is the first sentence. This is the second sentence."
                );
            }
            other => panic!("expected inline email content, got {other:?}"),
        }
        assert_eq!(count_files_recursively(&dir.path().join("blocks")), 2);
    }

    #[test]
    fn mailbox_expansion_accepts_mail_extension() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox_path = dir.path().join("2026-01.mail");
        fs::write(
            &mailbox_path,
            concat!(
                "From alan@example.com Sat Jan 03 10:00:00 2026\n",
                "Subject: Mail Extension\n",
                "From: Alan Example <alan@example.com>\n",
                "To: team@example.com\n",
                "\n",
                "Mail extension body.\n"
            ),
        )
        .unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let items = expand_mailbox_item(&mailbox_path, &BTreeMap::new(), &store).unwrap();

        assert_eq!(items.len(), 1);
        let metadata = metadata_to_text_map(&items[0].metadata);
        assert_eq!(
            metadata.get("email_subject"),
            Some(&"Mail Extension".to_string())
        );
        assert!(metadata.contains_key("mailbox_artifact_ref"));
        assert!(metadata.contains_key("email_artifact_ref"));
    }

    #[test]
    fn mailbox_expansion_rejects_unsupported_extension() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox_path = dir.path().join("2026-01.eml");
        fs::write(
            &mailbox_path,
            concat!(
                "From alan@example.com Sat Jan 03 10:00:00 2026\n",
                "Subject: Wrong Extension\n",
                "\n",
                "Body.\n"
            ),
        )
        .unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let error = expand_mailbox_item(&mailbox_path, &BTreeMap::new(), &store).unwrap_err();

        assert!(matches!(
            error,
            MailboxExpansionError::WrongExtension { path } if path == mailbox_path
        ));
    }

    #[test]
    fn normalized_email_artifacts_are_stable_for_unchanged_mailboxes() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox_path = dir.path().join("2026-02.mbox");
        fs::write(
            &mailbox_path,
            concat!(
                "From alan@example.com Sat Jan 03 10:00:00 2026\n",
                "Subject: Stable\n",
                "From: alan@example.com\n",
                "\n",
                "Stable body.\n"
            ),
        )
        .unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let first = expand_mailbox_item(&mailbox_path, &BTreeMap::new(), &store).unwrap();
        let file_count_after_first = count_files_recursively(&dir.path().join("blocks"));
        let second = expand_mailbox_item(&mailbox_path, &BTreeMap::new(), &store).unwrap();
        let file_count_after_second = count_files_recursively(&dir.path().join("blocks"));

        assert_eq!(
            metadata_to_text_map(&first[0].metadata).get("email_artifact_ref"),
            metadata_to_text_map(&second[0].metadata).get("email_artifact_ref")
        );
        assert_eq!(
            metadata_to_text_map(&first[0].metadata).get("chunk_locator"),
            metadata_to_text_map(&second[0].metadata).get("chunk_locator")
        );
        assert_eq!(file_count_after_first, file_count_after_second);
    }

    #[test]
    fn batch_expansion_keeps_document_items_direct() {
        let dir = tempfile::tempdir().unwrap();
        let mailbox_path = dir.path().join("2026-03.mbox");
        let document_path = dir.path().join("overview.txt");
        fs::write(
            &mailbox_path,
            concat!(
                "From alan@example.com Sat Jan 03 10:00:00 2026\n",
                "Subject: Mixed\n",
                "\n",
                "Body.\n"
            ),
        )
        .unwrap();
        fs::write(&document_path, "LexonArchiveBuilder docs.\n").unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();
        let request = BatchRequest {
            environment: EnvironmentConfig::Local {
                block_store_root: PathBuf::from("blocks"),
                embedding: LocalEmbeddingConfig {
                    base_url: "http://localhost:8080".into(),
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
                    path: mailbox_path.strip_prefix(dir.path()).unwrap().to_path_buf(),
                    metadata: BTreeMap::new(),
                },
                BatchItemConfig::Document {
                    path: document_path
                        .strip_prefix(dir.path())
                        .unwrap()
                        .to_path_buf(),
                    metadata: BTreeMap::new(),
                },
            ],
        };

        let items = expand_batch_items(dir.path(), &request, &store).unwrap();

        assert_eq!(items.len(), 2);
        assert!(matches!(items[0].content_ref, ContentRef::Document { .. }));
        assert!(matches!(items[1].content_ref, ContentRef::Inline { .. }));
    }

    #[test]
    fn single_part_messages_fall_back_to_lossy_body_decoding() {
        let message = [
            b"Subject: Lossy\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n".as_slice(),
            &[0x66, 0x80, 0x6f],
        ]
        .concat();
        let parsed = parse_mail(&message).unwrap();

        let body = first_preferred_body(&parsed).unwrap();

        assert_eq!(body, "f\u{fffd}o");
    }

    #[test]
    fn multipart_leaf_bodies_fall_back_to_lossy_body_decoding() {
        let message = [
            concat!(
                "Subject: Multipart Lossy\r\n",
                "Content-Type: multipart/alternative; boundary=\"b\"\r\n",
                "\r\n",
                "--b\r\n",
                "Content-Type: text/plain; charset=utf-8\r\n",
                "\r\n"
            )
            .as_bytes(),
            &[0x66, 0x80, 0x6f, b'\r', b'\n'],
            b"--b--\r\n".as_slice(),
        ]
        .concat();
        let parsed = parse_mail(&message).unwrap();

        let body = first_preferred_body(&parsed).unwrap();

        assert_eq!(body, "f\u{fffd}o");
    }

    fn metadata_to_text_map(metadata: &Metadata) -> BTreeMap<String, String> {
        metadata
            .iter()
            .filter_map(|(key, value)| match (key, value) {
                (Value::Text(key), Value::Text(value)) => Some((key.clone(), value.clone())),
                _ => None,
            })
            .collect()
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
}
