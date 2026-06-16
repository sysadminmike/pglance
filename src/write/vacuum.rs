use crate::write::storage::open_dataset;
use chrono::{Duration, Utc};
use lance_rs::dataset::cleanup::CleanupPolicy;
use std::time::Instant;
use tokio::runtime::Runtime;

type VacuumStats = (i64, i64, i64, i64, i64, i64, i64);

fn optional_u64(value: Option<i64>, name: &str) -> Result<Option<u64>, String> {
    match value {
        Some(value) if value < 0 => Err(format!("{} must be non-negative", name)),
        Some(value) => u64::try_from(value)
            .map(Some)
            .map_err(|_| format!("{} is too large", name)),
        None => Ok(None),
    }
}

/// Execute `lance_vacuum` - remove old unreferenced Lance dataset files.
///
/// Returns removal statistics and duration.
#[allow(clippy::too_many_arguments)]
pub fn lance_vacuum_impl(
    uri: &str,
    older_than_seconds: Option<i64>,
    before_version: Option<i64>,
    delete_unverified: bool,
    error_if_tagged_old_versions: bool,
    clean_referenced_branches: bool,
    delete_rate_limit: Option<i64>,
    server_name: Option<&str>,
) -> Result<VacuumStats, String> {
    let start = Instant::now();

    if let Some(seconds) = older_than_seconds {
        if seconds < 0 {
            return Err("older_than_seconds must be non-negative".to_string());
        }
    }

    let before_timestamp = older_than_seconds
        .map(Duration::seconds)
        .map(|duration| Utc::now() - duration);

    let policy = CleanupPolicy {
        before_timestamp,
        before_version: optional_u64(before_version, "before_version")?,
        delete_unverified,
        error_if_tagged_old_versions,
        clean_referenced_branches,
        delete_rate_limit: optional_u64(delete_rate_limit, "delete_rate_limit")?,
    };

    let rt = Runtime::new().map_err(|e| format!("failed to create tokio runtime: {}", e))?;

    let stats = rt.block_on(async {
        let dataset = open_dataset(uri, server_name).await?;
        dataset
            .cleanup_with_policy(policy)
            .await
            .map_err(|e| format!("lance vacuum failed: {}", e))
    })?;

    let duration_ms = start.elapsed().as_millis() as i64;
    Ok((
        stats.bytes_removed as i64,
        stats.old_versions as i64,
        stats.data_files_removed as i64,
        stats.transaction_files_removed as i64,
        stats.index_files_removed as i64,
        stats.deletion_files_removed as i64,
        duration_ms,
    ))
}
