// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::collections::BTreeMap;

use ciborium::Value;
use half::f16;
use lexongraph_block::{BlockHash, EmbeddingSpec};
use lexongraph_block_store::BlockStore;
use lexongraph_search::{
    DefaultCandidateScorer, DefaultEmbeddingCompatibility, EncodedTargetEmbedding, SearchError,
    SearchResult, Searcher,
};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("failed to parse root_id {value}")]
pub struct ParseBlockHashError {
    pub value: String,
}

pub fn parse_block_hash(value: &str) -> Result<BlockHash, ParseBlockHashError> {
    if value.len() != BlockHash::LEN * 2 {
        return Err(ParseBlockHashError {
            value: value.to_string(),
        });
    }

    let mut bytes = [0u8; BlockHash::LEN];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(chunk[0]).map_err(|()| ParseBlockHashError {
            value: value.to_string(),
        })?;
        let low = decode_hex_nibble(chunk[1]).map_err(|()| ParseBlockHashError {
            value: value.to_string(),
        })?;
        bytes[index] = (high << 4) | low;
    }

    Ok(BlockHash::from_bytes(bytes))
}

pub fn search_with_partial_retry(
    searcher: &Searcher<DefaultEmbeddingCompatibility, DefaultCandidateScorer>,
    root_id: &BlockHash,
    target: &EncodedTargetEmbedding,
    traversal_width: usize,
    top_k: usize,
    store: &dyn BlockStore,
) -> Result<SearchResult, SearchError> {
    match searcher.search(root_id, target, traversal_width, top_k, store) {
        Ok(result) => Ok(result),
        Err(SearchError::Exhausted {
            reachable_leaves, ..
        }) if reachable_leaves > 0 => {
            searcher.search(root_id, target, traversal_width, reachable_leaves, store)
        }
        Err(error) => Err(error),
    }
}

pub fn metadata_values_to_text_map(metadata: &[(Value, Value)]) -> BTreeMap<String, String> {
    metadata
        .iter()
        .filter_map(|(key, value)| match (key, value) {
            (Value::Text(key), Value::Text(value)) => Some((key.clone(), value.clone())),
            _ => None,
        })
        .collect()
}

pub fn source_name_from_metadata(metadata: &BTreeMap<String, String>) -> Option<String> {
    [
        "source_name",
        "document_name",
        "email_name",
        "thread_name",
        "name",
    ]
    .iter()
    .find_map(|key| metadata.get(*key).cloned())
}

pub fn decode_embedding_values(bytes: &[u8], embedding_spec: &EmbeddingSpec) -> Option<Vec<f32>> {
    let dimension_count = usize::try_from(embedding_spec.dims).ok()?;
    match embedding_spec.encoding.as_str() {
        "f32le" => {
            if bytes.len() != dimension_count.checked_mul(4)? {
                return None;
            }
            Some(
                bytes
                    .chunks_exact(4)
                    .map(|chunk| {
                        f32::from_le_bytes(chunk.try_into().expect("embedding chunk size is fixed"))
                    })
                    .collect(),
            )
        }
        "f16le" => {
            if bytes.len() != dimension_count.checked_mul(2)? {
                return None;
            }
            Some(
                bytes
                    .chunks_exact(2)
                    .map(|chunk| {
                        f16::from_le_bytes(chunk.try_into().expect("embedding chunk size is fixed"))
                            .to_f32()
                    })
                    .collect(),
            )
        }
        _ => None,
    }
}

fn decode_hex_nibble(byte: u8) -> Result<u8, ()> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_block_hash_rejects_invalid_hex() {
        let invalid_hex = "zz".repeat(BlockHash::LEN);
        let error = parse_block_hash(&invalid_hex).unwrap_err();
        assert_eq!(error.value, invalid_hex);
    }

    #[test]
    fn decode_embedding_values_decodes_f32le() {
        let spec = EmbeddingSpec {
            dims: 2,
            encoding: "f32le".into(),
        };
        let bytes = [1.0f32.to_le_bytes(), (-2.0f32).to_le_bytes()].concat();

        let decoded = decode_embedding_values(&bytes, &spec).unwrap();

        assert_eq!(decoded, vec![1.0, -2.0]);
    }
}
