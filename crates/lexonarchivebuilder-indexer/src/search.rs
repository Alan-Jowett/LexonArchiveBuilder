// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use lexongraph_block::{Block, BlockHash, EmbeddingSpec};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_embeddings_trait::{EmbeddingInput, EmbeddingProvider};
use lexongraph_search::{
    DefaultCandidateScorer, DefaultEmbeddingCompatibility, EncodedTargetEmbedding, SearchError,
    Searcher,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::tree_tools::{
    metadata_values_to_text_map, search_with_partial_retry, source_name_from_metadata,
};

const DEFAULT_TRAVERSAL_WIDTH: usize = 3;

#[derive(Debug, Error)]
pub enum RootedSearchError {
    #[error("root block {root_id} was not found")]
    MissingRootBlock { root_id: String },
    #[error("top_k must be at least 1")]
    InvalidTopK,
    #[error("traversal_width must be at least 1")]
    InvalidTraversalWidth,
    #[error(transparent)]
    BlockStore(BlockStoreError),
    #[error(transparent)]
    Search(SearchError),
    #[error("embedding provider failed: {message}")]
    Provider { message: String },
    #[error("failed to render rooted search report: {message}")]
    Render { message: String },
    #[error("failed to write rooted search report {path}: {source}")]
    WriteArtifact {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct RootedSearchHit {
    pub position: usize,
    pub leaf_block_id: String,
    pub media_type: String,
    pub text: String,
    pub metadata: BTreeMap<String, String>,
    pub source_kind: Option<String>,
    pub source_path: Option<String>,
    pub source_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct EmbeddingSpecReport {
    pub dims: u64,
    pub encoding: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct RootedSearchReport {
    pub root_id: String,
    pub query: String,
    pub top_k: usize,
    pub traversal_width: usize,
    pub embedding_spec: EmbeddingSpecReport,
    pub results: Vec<RootedSearchHit>,
}

pub async fn search_rooted_tree<EP>(
    store: &dyn BlockStore,
    embedding_provider: &EP,
    root_id: &BlockHash,
    query: &str,
    top_k: usize,
    traversal_width: usize,
) -> Result<RootedSearchReport, RootedSearchError>
where
    EP: EmbeddingProvider,
    EP::Error: std::error::Error + 'static,
{
    if top_k == 0 {
        return Err(RootedSearchError::InvalidTopK);
    }
    if traversal_width == 0 {
        return Err(RootedSearchError::InvalidTraversalWidth);
    }

    let Some(root) = store.get(root_id).map_err(RootedSearchError::BlockStore)? else {
        return Err(RootedSearchError::MissingRootBlock {
            root_id: root_id.to_string(),
        });
    };
    let embedding_spec = embedding_spec_for_block(&root.block);
    let target_embedding = embedding_provider
        .embed(
            &EmbeddingInput {
                media_type: "text/plain".into(),
                body: query.as_bytes().to_vec(),
            },
            &embedding_spec,
        )
        .await
        .map_err(|error| RootedSearchError::Provider {
            message: error.to_string(),
        })?;
    let target = EncodedTargetEmbedding::new(target_embedding, embedding_spec.clone());
    let searcher = Searcher::new(DefaultEmbeddingCompatibility, DefaultCandidateScorer);
    let result =
        search_with_partial_retry(&searcher, root_id, &target, traversal_width, top_k, store)
            .map_err(RootedSearchError::Search)?;

    Ok(RootedSearchReport {
        root_id: root_id.to_string(),
        query: query.to_string(),
        top_k,
        traversal_width,
        embedding_spec: EmbeddingSpecReport {
            dims: embedding_spec.dims,
            encoding: embedding_spec.encoding,
        },
        results: result
            .leaves
            .into_iter()
            .map(|leaf| {
                let metadata = metadata_values_to_text_map(&leaf.entry.metadata);
                RootedSearchHit {
                    position: leaf.position,
                    leaf_block_id: leaf.leaf_block_id.to_string(),
                    media_type: leaf.entry.content.media_type,
                    text: String::from_utf8_lossy(&leaf.entry.content.body).into_owned(),
                    source_kind: metadata.get("source_kind").cloned(),
                    source_path: metadata.get("source_path").cloned(),
                    source_name: source_name_from_metadata(&metadata),
                    metadata,
                }
            })
            .collect(),
    })
}

pub fn default_report_path(root_id: &BlockHash, query: &str) -> PathBuf {
    let query_hash = Sha256::digest(query.as_bytes());
    PathBuf::from(format!(
        "rooted-search-{}-{:02x}{:02x}{:02x}{:02x}.json",
        &root_id.to_string()[..8],
        query_hash[0],
        query_hash[1],
        query_hash[2],
        query_hash[3]
    ))
}

pub fn write_report(path: &Path, report: &RootedSearchReport) -> Result<(), RootedSearchError> {
    let rendered =
        serde_json::to_vec_pretty(report).map_err(|error| RootedSearchError::Render {
            message: error.to_string(),
        })?;
    fs::write(path, rendered).map_err(|source| RootedSearchError::WriteArtifact {
        path: path.display().to_string(),
        source,
    })
}

pub fn render_report_summary(report: &RootedSearchReport) -> String {
    let mut lines = vec![
        format!(
            "Rooted search results for {} (top_k {}, traversal_width {})",
            report.root_id, report.top_k, report.traversal_width
        ),
        format!("Query: {}", report.query),
    ];
    for hit in &report.results {
        let label = hit
            .source_name
            .as_ref()
            .or(hit.source_path.as_ref())
            .cloned()
            .unwrap_or_else(|| hit.leaf_block_id.clone());
        lines.push(format!(
            "{}. {} [{}]",
            hit.position + 1,
            label,
            hit.leaf_block_id
        ));
        lines.push(format!("   {}", hit.text.replace('\n', " ").trim()));
    }
    lines.join("\n")
}

pub fn default_traversal_width() -> usize {
    DEFAULT_TRAVERSAL_WIDTH
}

fn embedding_spec_for_block(block: &Block) -> EmbeddingSpec {
    match block {
        Block::Branch(branch) => branch.embedding_spec.clone(),
        Block::Leaf(leaf) => leaf.embedding_spec.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use super::*;
    use ciborium::Value;
    use lexongraph_block::{
        Block, BranchBlock, BranchEntry, Content, LeafBlock, LeafEntry, VERSION_1,
    };
    use lexongraph_block_store_fs::FilesystemBlockStore;
    use tempfile::tempdir;

    #[derive(Debug)]
    struct FakeProvider {
        bytes: Vec<u8>,
    }

    #[derive(Debug)]
    struct FakeProviderError;

    impl fmt::Display for FakeProviderError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("fake provider error")
        }
    }

    impl std::error::Error for FakeProviderError {}

    impl EmbeddingProvider for FakeProvider {
        type Error = FakeProviderError;

        async fn embed(
            &self,
            _: &EmbeddingInput,
            _: &EmbeddingSpec,
        ) -> Result<Vec<u8>, Self::Error> {
            Ok(self.bytes.clone())
        }

        async fn embed_batch(
            &self,
            inputs: &[EmbeddingInput],
            _: &EmbeddingSpec,
        ) -> Result<Vec<Vec<u8>>, Self::Error> {
            Ok(vec![self.bytes.clone(); inputs.len()])
        }
    }

    #[tokio::test]
    async fn rooted_search_returns_top_k_reachable_leaves() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let alpha = store
            .put(&leaf_block("alpha", &[1.0, 0.0], "alpha body"))
            .unwrap();
        let beta = store
            .put(&leaf_block("beta", &[0.0, 1.0], "beta body"))
            .unwrap();
        let root = store
            .put(&Block::Branch(BranchBlock {
                version: VERSION_1,
                level: 1,
                embedding_spec: EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                entries: vec![
                    BranchEntry {
                        embedding: encode_f32(&[1.0, 0.0]),
                        child: alpha,
                    },
                    BranchEntry {
                        embedding: encode_f32(&[0.0, 1.0]),
                        child: beta,
                    },
                ],
                ext: None,
            }))
            .unwrap();

        let report = search_rooted_tree(
            &store,
            &FakeProvider {
                bytes: encode_f32(&[1.0, 0.0]),
            },
            &root,
            "alpha",
            1,
            2,
        )
        .await
        .unwrap();

        assert_eq!(report.results.len(), 1);
        assert_eq!(report.results[0].text, "alpha body");
        assert!(render_report_summary(&report).contains("alpha body"));
    }

    #[test]
    fn default_report_path_is_stable_per_root_and_query() {
        let root = BlockHash::from_bytes([7u8; BlockHash::LEN]);

        let path = default_report_path(&root, "hello");

        assert!(
            path.display()
                .to_string()
                .starts_with("rooted-search-07070707-")
        );
    }

    #[tokio::test]
    async fn rooted_search_writes_json_artifact_with_same_result_set() {
        let dir = tempdir().unwrap();
        let store = FilesystemBlockStore::new(dir.path().join("blocks")).unwrap();

        let alpha = store
            .put(&leaf_block("alpha", &[1.0, 0.0], "alpha body"))
            .unwrap();
        let beta = store
            .put(&leaf_block("beta", &[0.0, 1.0], "beta body"))
            .unwrap();
        let root = store
            .put(&Block::Branch(BranchBlock {
                version: VERSION_1,
                level: 1,
                embedding_spec: EmbeddingSpec {
                    dims: 2,
                    encoding: "f32le".into(),
                },
                entries: vec![
                    BranchEntry {
                        embedding: encode_f32(&[1.0, 0.0]),
                        child: alpha,
                    },
                    BranchEntry {
                        embedding: encode_f32(&[0.0, 1.0]),
                        child: beta,
                    },
                ],
                ext: None,
            }))
            .unwrap();

        let report = search_rooted_tree(
            &store,
            &FakeProvider {
                bytes: encode_f32(&[1.0, 0.0]),
            },
            &root,
            "alpha",
            1,
            2,
        )
        .await
        .unwrap();
        let path = dir.path().join("rooted-search.json");
        write_report(&path, &report).unwrap();

        let rendered = fs::read_to_string(path).unwrap();
        assert!(rendered.contains(&format!("\"root_id\": \"{}\"", root)));
        assert!(rendered.contains("\"query\": \"alpha\""));
        assert!(rendered.contains(&format!(
            "\"leaf_block_id\": \"{}\"",
            report.results[0].leaf_block_id
        )));
        assert!(rendered.contains("\"text\": \"alpha body\""));
    }

    fn leaf_block(name: &str, embedding: &[f32; 2], body: &str) -> Block {
        Block::Leaf(LeafBlock {
            version: VERSION_1,
            level: 0,
            embedding_spec: EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            entries: vec![LeafEntry {
                embedding: encode_f32(embedding),
                metadata: vec![(Value::Text("source_name".into()), Value::Text(name.into()))],
                content: Content {
                    media_type: "text/plain".into(),
                    body: body.as_bytes().to_vec(),
                },
            }],
            ext: None,
        })
    }

    fn encode_f32(values: &[f32; 2]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }
}
