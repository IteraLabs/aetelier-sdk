use std::path::PathBuf;

use async_trait::async_trait;

use crate::error::EntrepotError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedObject {
    pub bytes: Vec<u8>,
    pub etag: Option<String>,
    pub request_charged: bool,
}

impl FetchedObject {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            etag: None,
            request_charged: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransferSnapshot {
    pub get_requests: u64,
    pub list_requests: u64,
    pub retries: u64,
    pub bytes_in: u64,
    pub unpaid_responses: u64,
    pub integrity_fail: u64,
}

#[async_trait]
pub trait ObjectSource: Send + Sync {
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, EntrepotError>;
    async fn get(&self, key: &str) -> Result<Vec<u8>, EntrepotError>;
    async fn get_object(&self, key: &str) -> Result<FetchedObject, EntrepotError> {
        Ok(FetchedObject::from_bytes(self.get(key).await?))
    }
    fn transfer_snapshot(&self) -> Option<TransferSnapshot> {
        None
    }
}

pub struct LocalDirSource {
    root: PathBuf,
}

impl LocalDirSource {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn guard_key(key: &str) -> Result<(), EntrepotError> {
        if key.split('/').any(|seg| seg == "..") {
            return Err(EntrepotError::Io {
                path: key.to_string(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "parent traversal rejected",
                ),
            });
        }
        Ok(())
    }

    fn walk(
        &self,
        dir: &PathBuf,
        out: &mut Vec<ObjectMeta>,
    ) -> Result<(), EntrepotError> {
        let entries = std::fs::read_dir(dir).map_err(|source| EntrepotError::Io {
            path: dir.display().to_string(),
            source,
        })?;
        for entry in entries {
            let entry = entry.map_err(|source| EntrepotError::Io {
                path: dir.display().to_string(),
                source,
            })?;
            let path = entry.path();
            if path.is_dir() {
                self.walk(&path, out)?;
            } else if path.is_file() {
                let meta = entry.metadata().map_err(|source| EntrepotError::Io {
                    path: path.display().to_string(),
                    source,
                })?;
                let rel = path
                    .strip_prefix(&self.root)
                    .expect("walk stays under root")
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/");
                out.push(ObjectMeta {
                    key: rel,
                    size: meta.len(),
                    etag: None,
                    last_modified: None,
                });
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectSource for LocalDirSource {
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, EntrepotError> {
        let mut out = Vec::new();
        self.walk(&self.root.clone(), &mut out)?;
        out.retain(|m| m.key.starts_with(prefix));
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, EntrepotError> {
        Self::guard_key(key)?;
        let path = self.root.join(key);
        std::fs::read(&path).map_err(|source| EntrepotError::Io {
            path: path.display().to_string(),
            source,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_tree() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("market_data/20230916/9/l2Book");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(base.join("SOL.lz4"), b"sol-bytes").unwrap();
        std::fs::write(base.join("BTC.lz4"), b"btc-bytes").unwrap();
        std::fs::create_dir_all(dir.path().join("asset_ctxs")).unwrap();
        std::fs::write(dir.path().join("asset_ctxs/20230916.csv.lz4"), b"csv").unwrap();
        dir
    }

    #[tokio::test]
    async fn lists_by_prefix_sorted_with_forward_slash_keys() {
        let dir = fixture_tree();
        let src = LocalDirSource::new(dir.path());
        let all = src.list("").await.unwrap();
        assert_eq!(all.len(), 3);
        let books = src.list("market_data/20230916/9/l2Book/").await.unwrap();
        let keys: Vec<&str> = books.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "market_data/20230916/9/l2Book/BTC.lz4",
                "market_data/20230916/9/l2Book/SOL.lz4"
            ]
        );
        assert_eq!(books[1].size, 9);
    }

    #[tokio::test]
    async fn gets_bytes_and_rejects_traversal() {
        let dir = fixture_tree();
        let src = LocalDirSource::new(dir.path());
        let bytes = src
            .get("market_data/20230916/9/l2Book/SOL.lz4")
            .await
            .unwrap();
        assert_eq!(bytes, b"sol-bytes");
        assert!(src.get("../outside").await.is_err());
        assert!(src.get("missing/key").await.is_err());
    }
}
