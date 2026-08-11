use std::path::Path;

use crate::error::EntrepotError;
use crate::source::ObjectSource;

pub fn already_complete(dest: &Path, expected_size: u64) -> bool {
    std::fs::metadata(dest)
        .map(|m| m.is_file() && m.len() == expected_size)
        .unwrap_or(false)
}

pub async fn fetch_to_file(
    src: &dyn ObjectSource,
    key: &str,
    dest: &Path,
) -> Result<u64, EntrepotError> {
    let bytes = src.get(key).await?;
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|source| EntrepotError::Io {
            path: parent.display().to_string(),
            source,
        })?;
    }
    let part = dest.with_file_name(format!(
        "{}.part",
        dest.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "download".to_string())
    ));
    std::fs::write(&part, &bytes).map_err(|source| EntrepotError::Io {
        path: part.display().to_string(),
        source,
    })?;
    std::fs::rename(&part, dest).map_err(|source| EntrepotError::Io {
        path: dest.display().to_string(),
        source,
    })?;
    Ok(bytes.len() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::LocalDirSource;

    #[tokio::test]
    async fn fetches_atomically_and_detects_completion() {
        let src_dir = tempfile::tempdir().unwrap();
        std::fs::write(src_dir.path().join("obj.bin"), b"payload").unwrap();
        let src = LocalDirSource::new(src_dir.path());

        let out_dir = tempfile::tempdir().unwrap();
        let dest = out_dir.path().join("nested/obj.bin");
        assert!(!already_complete(&dest, 7));

        let n = fetch_to_file(&src, "obj.bin", &dest).await.unwrap();
        assert_eq!(n, 7);
        assert_eq!(std::fs::read(&dest).unwrap(), b"payload");
        assert!(already_complete(&dest, 7));
        assert!(!already_complete(&dest, 8));
        assert!(!dest.with_file_name("obj.bin.part").exists());
    }

    #[tokio::test]
    async fn missing_object_propagates_the_source_error() {
        let src_dir = tempfile::tempdir().unwrap();
        let src = LocalDirSource::new(src_dir.path());
        let out = tempfile::tempdir().unwrap();
        assert!(
            fetch_to_file(&src, "absent", &out.path().join("x"))
                .await
                .is_err()
        );
    }
}
