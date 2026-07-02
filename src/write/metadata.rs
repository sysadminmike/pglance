use crate::write::storage::open_dataset;
use chrono::{Duration, Utc};
use lance_rs::dataset::cleanup::CleanupPolicy;
use lance_rs::dataset::refs::{BranchContents, TagContents};
use lance_rs::dataset::Version;
use lance_rs::Dataset;
use serde_json::{json, Value};
use std::time::Instant;
use tokio::runtime::Runtime;

pub type TableInfoRow = (String, i64, i64, i64, i64, String, String, i64);
pub type TableFieldRow = (String, i32, i32, String, String, bool, String);
pub type CountRowsRow = (i64, i64);
pub type FragmentRow = (i64, Option<i64>, Option<i64>, i64, bool, String);
pub type FragmentStatsRow = (i64, i64, i64, i64, f64, i64, i64);
pub type VersionRow = (i64, String, String);
pub type TagRow = (
    String,
    Option<String>,
    i64,
    Option<String>,
    Option<String>,
    i64,
    String,
);
pub type BranchRow = (String, Option<String>, i64, i64, i64, String, String);
pub type CleanupPlanRow = (
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    i64,
    bool,
    i64,
    String,
    String,
    String,
    i64,
);

fn runtime() -> Result<Runtime, String> {
    Runtime::new().map_err(|e| format!("failed to create tokio runtime: {}", e))
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

fn optional_usize(value: Option<i64>, name: &str) -> Result<Option<usize>, String> {
    match value {
        Some(value) if value < 0 => Err(format!("{} must be non-negative", name)),
        Some(value) => usize::try_from(value)
            .map(Some)
            .map_err(|_| format!("{} is too large", name)),
        None => Ok(None),
    }
}

fn cleanup_policy(
    older_than_seconds: Option<i64>,
    before_version: Option<i64>,
    delete_unverified: bool,
    error_if_tagged_old_versions: bool,
    clean_referenced_branches: bool,
    delete_rate_limit: Option<i64>,
) -> Result<CleanupPolicy, String> {
    if let Some(seconds) = older_than_seconds {
        if seconds < 0 {
            return Err("older_than_seconds must be non-negative".to_string());
        }
    }

    let before_timestamp = older_than_seconds
        .map(Duration::seconds)
        .map(|duration| Utc::now() - duration);

    Ok(CleanupPolicy {
        before_timestamp,
        before_version: optional_u64(before_version, "before_version")?,
        delete_unverified,
        error_if_tagged_old_versions,
        clean_referenced_branches,
        delete_rate_limit: optional_u64(delete_rate_limit, "delete_rate_limit")?,
    })
}

pub fn lance_table_info_impl(uri: &str, server_name: Option<&str>) -> Result<TableInfoRow, String> {
    let started = Instant::now();
    let rt = runtime()?;

    let row = rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        let rows = dataset
            .count_rows(None)
            .await
            .map_err(|e| format!("failed to count rows: {}", e))?;
        let latest_version = dataset
            .latest_version_id()
            .await
            .map_err(|e| format!("failed to read latest version: {}", e))?;

        Ok::<_, String>((
            uri.to_string(),
            dataset.version_id() as i64,
            latest_version as i64,
            rows as i64,
            dataset.count_fragments() as i64,
            schema_json(&dataset).to_string(),
            json!(dataset.schema().metadata).to_string(),
            started.elapsed().as_millis() as i64,
        ))
    })?;

    Ok(row)
}

pub fn lance_table_fields_impl(
    uri: &str,
    server_name: Option<&str>,
) -> Result<Vec<TableFieldRow>, String> {
    let rt = runtime()?;

    rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        let mut rows = Vec::new();
        for field in &dataset.schema().fields {
            collect_field_rows(field, None, &mut rows);
        }
        Ok(rows)
    })
}

pub fn lance_count_rows_impl(
    uri: &str,
    filter: Option<&str>,
    server_name: Option<&str>,
) -> Result<CountRowsRow, String> {
    let started = Instant::now();
    let rt = runtime()?;

    let rows = rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        dataset
            .count_rows(filter.map(ToOwned::to_owned))
            .await
            .map_err(|e| format!("failed to count rows: {}", e))
    })?;

    Ok((rows as i64, started.elapsed().as_millis() as i64))
}

pub fn lance_list_fragments_impl(
    uri: &str,
    server_name: Option<&str>,
) -> Result<Vec<FragmentRow>, String> {
    let rt = runtime()?;

    rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        let mut rows = Vec::new();
        for fragment in dataset.iter_fragments() {
            let physical_rows = fragment.physical_rows.map(|n| n as i64);
            let logical_rows = fragment.num_rows().map(|n| n as i64);
            rows.push((
                fragment.id as i64,
                physical_rows,
                logical_rows,
                fragment.files.len() as i64,
                fragment.deletion_file.is_some(),
                json!(fragment).to_string(),
            ));
        }
        Ok(rows)
    })
}

pub fn lance_fragment_stats_impl(
    uri: &str,
    server_name: Option<&str>,
) -> Result<FragmentStatsRow, String> {
    let started = Instant::now();
    let rt = runtime()?;

    rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        let mut fragment_count = 0_i64;
        let mut physical_rows = 0_i64;
        let mut logical_rows = 0_i64;
        let mut deletion_files = 0_i64;

        for fragment in dataset.iter_fragments() {
            fragment_count += 1;
            if let Some(rows) = fragment.physical_rows {
                physical_rows += rows as i64;
            }
            if let Some(rows) = fragment.num_rows() {
                logical_rows += rows as i64;
            }
            if fragment.deletion_file.is_some() {
                deletion_files += 1;
            }
        }

        let avg_rows_per_fragment = if fragment_count == 0 {
            0.0
        } else {
            logical_rows as f64 / fragment_count as f64
        };
        let deleted_rows = physical_rows.saturating_sub(logical_rows);

        Ok((
            fragment_count,
            physical_rows,
            logical_rows,
            deleted_rows,
            avg_rows_per_fragment,
            deletion_files,
            started.elapsed().as_millis() as i64,
        ))
    })
}

pub fn lance_list_versions_impl(
    uri: &str,
    server_name: Option<&str>,
) -> Result<Vec<VersionRow>, String> {
    let rt = runtime()?;

    rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        let versions = dataset
            .versions()
            .await
            .map_err(|e| format!("failed to list versions: {}", e))?;
        versions
            .into_iter()
            .map(version_row)
            .collect::<Result<Vec<_>, _>>()
    })
}

pub fn lance_list_tags_impl(uri: &str, server_name: Option<&str>) -> Result<Vec<TagRow>, String> {
    let rt = runtime()?;

    rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        let tags = dataset
            .refs
            .tags()
            .list_tags_ordered(None)
            .await
            .map_err(|e| format!("failed to list tags: {}", e))?;

        tags.into_iter()
            .map(|(name, contents)| tag_row(name, contents))
            .collect::<Result<Vec<_>, _>>()
    })
}

pub fn lance_list_branches_impl(
    uri: &str,
    server_name: Option<&str>,
) -> Result<Vec<BranchRow>, String> {
    let rt = runtime()?;

    rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        let mut branches = dataset
            .refs
            .branches()
            .fetch()
            .await
            .map_err(|e| format!("failed to list branches: {}", e))?;
        branches.sort_by(|a, b| a.0.cmp(&b.0));

        branches
            .into_iter()
            .map(|(name, contents)| branch_row(name, contents))
            .collect::<Result<Vec<_>, _>>()
    })
}

#[allow(clippy::too_many_arguments)]
pub fn lance_cleanup_plan_impl(
    uri: &str,
    older_than_seconds: Option<i64>,
    before_version: Option<i64>,
    delete_unverified: bool,
    error_if_tagged_old_versions: bool,
    clean_referenced_branches: bool,
    delete_rate_limit: Option<i64>,
    max_candidate_files: Option<i64>,
    server_name: Option<&str>,
) -> Result<CleanupPlanRow, String> {
    let started = Instant::now();
    let policy = cleanup_policy(
        older_than_seconds,
        before_version,
        delete_unverified,
        error_if_tagged_old_versions,
        clean_referenced_branches,
        delete_rate_limit,
    )?;
    let max_candidate_files = optional_usize(max_candidate_files, "max_candidate_files")?;
    let rt = runtime()?;

    rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        let mut operation = dataset.cleanup(policy);
        if let Some(max_candidate_files) = max_candidate_files {
            operation = operation.with_max_candidate_files(max_candidate_files);
        }
        let plan = operation
            .explain()
            .await
            .map_err(|e| format!("failed to build cleanup plan: {}", e))?;

        let candidate_files = plan
            .candidate_files
            .iter()
            .map(|file| {
                json!({
                    "path": file.path,
                    "kind": format!("{:?}", file.kind),
                    "unverified": file.unverified,
                    "size_bytes": file.size_bytes,
                })
            })
            .collect::<Vec<_>>();
        let referenced_branches = plan
            .referenced_branches
            .iter()
            .map(|branch| {
                json!({
                    "name": branch.name,
                    "referenced_version": branch.referenced_version,
                    "cleanup_candidate": branch.cleanup_candidate,
                })
            })
            .collect::<Vec<_>>();

        Ok((
            plan.read_version as i64,
            plan.stats.bytes_removed as i64,
            plan.stats.old_versions as i64,
            plan.stats.data_files_removed as i64,
            plan.stats.transaction_files_removed as i64,
            plan.stats.index_files_removed as i64,
            plan.stats.deletion_files_removed as i64,
            plan.candidate_files_truncated,
            plan.candidate_file_limit as i64,
            Value::Array(candidate_files).to_string(),
            Value::Array(referenced_branches).to_string(),
            json!(plan.warnings).to_string(),
            started.elapsed().as_millis() as i64,
        ))
    })
}

fn schema_json(dataset: &Dataset) -> Value {
    json!({
        "fields": dataset.schema().fields.iter().map(field_json).collect::<Vec<_>>(),
        "metadata": dataset.schema().metadata,
    })
}

fn field_json(field: &lance_rs::datatypes::Field) -> Value {
    json!({
        "name": field.name,
        "id": field.id,
        "parent_id": field.parent_id,
        "data_type": field.data_type().to_string(),
        "nullable": field.nullable,
        "metadata": field.metadata,
        "children": field.children.iter().map(field_json).collect::<Vec<_>>(),
    })
}

fn collect_field_rows(
    field: &lance_rs::datatypes::Field,
    parent_path: Option<&str>,
    rows: &mut Vec<TableFieldRow>,
) {
    let path = match parent_path {
        Some(parent) => format!("{}.{}", parent, field.name),
        None => field.name.clone(),
    };
    rows.push((
        path.clone(),
        field.id,
        field.parent_id,
        field.name.clone(),
        field.data_type().to_string(),
        field.nullable,
        json!(field.metadata).to_string(),
    ));

    for child in &field.children {
        collect_field_rows(child, Some(&path), rows);
    }
}

fn version_row(version: Version) -> Result<VersionRow, String> {
    Ok((
        version.version as i64,
        version.timestamp.to_rfc3339(),
        json!(version.metadata).to_string(),
    ))
}

fn tag_row(name: String, contents: TagContents) -> Result<TagRow, String> {
    Ok((
        name,
        contents.branch,
        contents.version as i64,
        contents.created_at.map(|v| v.to_rfc3339()),
        contents.updated_at.map(|v| v.to_rfc3339()),
        contents.manifest_size as i64,
        json!(contents.metadata).to_string(),
    ))
}

fn branch_row(name: String, contents: BranchContents) -> Result<BranchRow, String> {
    Ok((
        name,
        contents.parent_branch,
        contents.parent_version as i64,
        contents.create_at as i64,
        contents.manifest_size as i64,
        json!(contents.identifier).to_string(),
        json!(contents.metadata).to_string(),
    ))
}
