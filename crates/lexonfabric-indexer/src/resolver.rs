use std::fs;
use std::path::PathBuf;

use lexongraph_indexer::{Content, ContentResolver};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContentRef {
    Mailbox { path: PathBuf },
    Document { path: PathBuf },
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LocalFilesystemContentResolver;

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
}

impl ContentResolver<ContentRef> for LocalFilesystemContentResolver {
    type Error = LocalFilesystemContentResolverError;

    fn resolve(&self, content_ref: &ContentRef) -> Result<Content, Self::Error> {
        match content_ref {
            ContentRef::Mailbox { path } => resolve_file(path, "mbox", "application/mbox"),
            ContentRef::Document { path } => resolve_file(path, "txt", "text/plain"),
        }
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

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::fs;

    use lexongraph_indexer::Metadata;
    use lexongraph_indexer::{IndexItem, conformance};
    use tempfile::tempdir;

    use super::*;

    #[derive(Clone)]
    struct ResolverHarness {
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
            ResolverMode::Working(LocalFilesystemContentResolver)
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
        Working(LocalFilesystemContentResolver),
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
    }

    #[test]
    fn mailbox_resolver_passes_conformance_suite() {
        let dir = tempdir().unwrap();
        let mailbox_path = dir.path().join("2026-01.mbox");
        let body = b"From user@example.com Sat Jan 01 00:00:00 2026\nSubject: Hello\n\nBody\n";
        fs::write(&mailbox_path, body).unwrap();
        let harness = ResolverHarness {
            content_ref: ContentRef::Mailbox { path: mailbox_path },
            expected_content: Content {
                media_type: "application/mbox".into(),
                body: body.to_vec(),
            },
        };

        conformance::run_content_resolver_suite(&harness).unwrap();
    }

    #[test]
    fn document_resolver_passes_conformance_suite() {
        let dir = tempdir().unwrap();
        let document_path = dir.path().join("readme.txt");
        let body = b"LexonFabric document body\n";
        fs::write(&document_path, body).unwrap();
        let harness = ResolverHarness {
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

        let error = LocalFilesystemContentResolver
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
    fn mailbox_resolver_normalizes_non_utf8_bytes_for_embedding() {
        let dir = tempdir().unwrap();
        let mailbox_path = dir.path().join("2026-02.mbox");
        fs::write(&mailbox_path, [0x66, 0x6f, 0x80, 0x6f]).unwrap();

        let content = LocalFilesystemContentResolver
            .resolve(&ContentRef::Mailbox { path: mailbox_path })
            .unwrap();

        let normalized = String::from_utf8(content.body).unwrap();
        assert_eq!(normalized, "fo\u{fffd}o");
    }
}
