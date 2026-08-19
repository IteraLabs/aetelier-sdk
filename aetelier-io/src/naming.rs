use std::path::{Path, PathBuf};

pub(crate) fn effective_us(primary: u64, fallback: u64) -> u64 {
    if primary > 0 { primary } else { fallback }
}

pub(crate) fn batch_stamp<I: IntoIterator<Item = u64>>(ts_us: I) -> String {
    let min = ts_us.into_iter().filter(|t| *t > 0).min().unwrap_or(0);
    match chrono::DateTime::from_timestamp_micros(min as i64) {
        Some(dt) if min > 0 => dt.format("%Y%m%d_%H%M%S%.3f").to_string(),
        _ => chrono::Utc::now().format("%Y%m%d_%H%M%S%.3f").to_string(),
    }
}

pub(crate) fn unique_path(dir: &Path, filename: &str) -> PathBuf {
    let candidate = dir.join(filename);
    if !candidate.exists() {
        return candidate;
    }
    let stem = filename.strip_suffix(".parquet").unwrap_or(filename);
    let mut n: u64 = 1;
    loop {
        let alt = dir.join(format!("{stem}-{n}.parquet"));
        if !alt.exists() {
            return alt;
        }
        n += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamp_derives_from_the_min_positive_timestamp() {
        let stamp = batch_stamp([1_694_854_800_550_000, 1_694_854_800_000_000, 0]);
        assert_eq!(stamp, "20230916_090000.000");
    }

    #[test]
    fn effective_falls_back_when_the_venue_gave_no_timestamp() {
        assert_eq!(effective_us(0, 42), 42);
        assert_eq!(effective_us(7, 42), 7);
    }

    #[test]
    fn all_zero_timestamps_fall_back_to_now_never_epoch() {
        let stamp = batch_stamp([0, 0]);
        assert!(!stamp.starts_with("1970"));
    }

    #[test]
    fn colliding_names_get_a_numbered_sibling() {
        let dir = tempfile::tempdir().unwrap();
        let first = unique_path(dir.path(), "a_b_ob_sync_x.parquet");
        std::fs::write(&first, b"one").unwrap();
        let second = unique_path(dir.path(), "a_b_ob_sync_x.parquet");
        assert_eq!(
            second.file_name().unwrap().to_str().unwrap(),
            "a_b_ob_sync_x-1.parquet"
        );
        std::fs::write(&second, b"two").unwrap();
        let third = unique_path(dir.path(), "a_b_ob_sync_x.parquet");
        assert_eq!(
            third.file_name().unwrap().to_str().unwrap(),
            "a_b_ob_sync_x-2.parquet"
        );
        assert_eq!(std::fs::read(&first).unwrap(), b"one");
    }
}
