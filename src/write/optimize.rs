use crate::write::storage::open_dataset;
use lance_rs::dataset::optimize::{compact_files, CompactionMode, CompactionOptions};
use std::time::Instant;
use tokio::runtime::Runtime;

fn optional_usize(value: Option<i64>, name: &str) -> Result<Option<usize>, String> {
    match value {
        Some(value) if value < 0 => Err(format!("{} must be non-negative", name)),
        Some(value) => usize::try_from(value)
            .map(Some)
            .map_err(|_| format!("{} is too large", name)),
        None => Ok(None),
    }
}

fn optional_u64(value: Option<i64>, name: &str) -> Result<Option<u64>, String> {
    match value {
        Some(value) if value < 0 => Err(format!("{} must be non-negative", name)),
        Some(value) => u64::try_from(value)
            .map(Some)
            .map_err(|_| format!("{} is too large", name)),
        None => Ok(None),
    }
}

fn parse_compaction_mode(value: Option<&str>) -> Result<Option<CompactionMode>, String> {
    match value {
        Some("reencode") => Ok(Some(CompactionMode::Reencode)),
        Some("try_binary_copy") => Ok(Some(CompactionMode::TryBinaryCopy)),
        Some("force_binary_copy") => Ok(Some(CompactionMode::ForceBinaryCopy)),
        Some(value) => Err(format!(
            "invalid compaction_mode '{}': must be 'reencode', 'try_binary_copy', or 'force_binary_copy'",
            value
        )),
        None => Ok(None),
    }
}

/// Execute `lance_optimize` - compact small or delete-heavy fragments.
///
/// Returns `(fragments_removed, fragments_added, files_removed, files_added, duration_ms)`.
#[allow(clippy::too_many_arguments)]
pub fn lance_optimize_impl(
    uri: &str,
    target_rows_per_fragment: Option<i64>,
    max_rows_per_group: Option<i64>,
    max_bytes_per_file: Option<i64>,
    materialize_deletions: bool,
    materialize_deletions_threshold: f32,
    num_threads: Option<i64>,
    batch_size: Option<i64>,
    defer_index_remap: bool,
    compaction_mode: Option<&str>,
    max_source_fragments: Option<i64>,
    io_buffer_size: Option<i64>,
    server_name: Option<&str>,
) -> Result<(i64, i64, i64, i64, i64), String> {
    let start = Instant::now();

    if !materialize_deletions_threshold.is_finite() {
        return Err("materialize_deletions_threshold must be finite".to_string());
    }

    let mut options = CompactionOptions::default();
    if let Some(value) = optional_usize(target_rows_per_fragment, "target_rows_per_fragment")? {
        options.target_rows_per_fragment = value;
    }
    if let Some(value) = optional_usize(max_rows_per_group, "max_rows_per_group")? {
        options.max_rows_per_group = value;
    }
    options.max_bytes_per_file = optional_usize(max_bytes_per_file, "max_bytes_per_file")?;
    options.materialize_deletions = materialize_deletions;
    options.materialize_deletions_threshold = materialize_deletions_threshold;
    options.num_threads = optional_usize(num_threads, "num_threads")?;
    options.batch_size = optional_usize(batch_size, "batch_size")?;
    options.defer_index_remap = defer_index_remap;
    options.compaction_mode = parse_compaction_mode(compaction_mode)?;
    options.max_source_fragments = optional_usize(max_source_fragments, "max_source_fragments")?;
    options.io_buffer_size = optional_u64(io_buffer_size, "io_buffer_size")?;

    let rt = Runtime::new().map_err(|e| format!("failed to create tokio runtime: {}", e))?;

    let metrics = rt.block_on(async {
        let mut dataset = open_dataset(uri, server_name).await?;
        compact_files(&mut dataset, options, None)
            .await
            .map_err(|e| format!("lance optimize failed: {}", e))
    })?;

    let duration_ms = start.elapsed().as_millis() as i64;
    Ok((
        metrics.fragments_removed as i64,
        metrics.fragments_added as i64,
        metrics.files_removed as i64,
        metrics.files_added as i64,
        duration_ms,
    ))
}
