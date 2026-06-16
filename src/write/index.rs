use crate::write::storage::open_dataset;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::{
    BuiltinIndexType, FullTextSearchQuery, InvertedIndexParams, ScalarIndexParams,
};
use lance_index::IndexType;
use lance_rs::index::DatasetIndexExt;
use serde_json::json;
use std::time::Instant;
use tokio::runtime::Runtime;

pub type IndexCreateResult = (String, String, String, i64);
pub type IndexOptimizeResult = (i64, i64);
pub type IndexListRow = (String, String, String, String, i64, Option<i64>, String);
pub type IndexStatsResult = (String, String);
pub type IndexDropResult = (String, i64);
pub type FtsSearchCountResult = (String, String, i64, i64);

fn runtime() -> Result<Runtime, String> {
    Runtime::new().map_err(|e| format!("failed to create tokio runtime: {}", e))
}

fn parse_scalar_index_type(index_type: &str) -> Result<(IndexType, ScalarIndexParams), String> {
    match index_type.to_ascii_lowercase().as_str() {
        "btree" | "b-tree" => Ok((
            IndexType::BTree,
            ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
        )),
        "bitmap" => Ok((
            IndexType::Bitmap,
            ScalarIndexParams::for_builtin(BuiltinIndexType::Bitmap),
        )),
        "label_list" | "labellist" | "label-list" => Ok((
            IndexType::LabelList,
            ScalarIndexParams::for_builtin(BuiltinIndexType::LabelList),
        )),
        other => Err(format!(
            "unsupported scalar index type '{}'; expected btree, bitmap, or label_list",
            other
        )),
    }
}

fn optional_usize(value: Option<i64>, name: &str) -> Result<Option<usize>, String> {
    value
        .map(|n| usize::try_from(n).map_err(|_| format!("{} must be a non-negative integer", name)))
        .transpose()
}

fn optional_u32(value: Option<i64>, name: &str) -> Result<Option<u32>, String> {
    value
        .map(|n| u32::try_from(n).map_err(|_| format!("{} must fit in u32", name)))
        .transpose()
}

fn optional_u64(value: Option<i64>, name: &str) -> Result<Option<u64>, String> {
    value
        .map(|n| u64::try_from(n).map_err(|_| format!("{} must be a non-negative integer", name)))
        .transpose()
}

fn parse_optimize_mode(
    mode: &str,
    num_indices_to_merge: Option<i64>,
) -> Result<OptimizeOptions, String> {
    let mode = mode.to_ascii_lowercase();
    match mode.as_str() {
        "append" => Ok(OptimizeOptions::append()),
        "merge" => Ok(OptimizeOptions::merge(
            optional_usize(num_indices_to_merge, "num_indices_to_merge")?.unwrap_or(1),
        )),
        "retrain" => Ok(OptimizeOptions::retrain()),
        "auto" | "default" => Ok(OptimizeOptions::new().num_indices_to_merge(optional_usize(
            num_indices_to_merge,
            "num_indices_to_merge",
        )?)),
        other => Err(format!(
            "unsupported optimize mode '{}'; expected append, merge, retrain, or auto",
            other
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_fts_params(
    tokenizer: &str,
    language: &str,
    with_position: bool,
    lower_case: bool,
    stem: bool,
    remove_stop_words: bool,
    ascii_folding: bool,
    max_token_length: Option<i64>,
    ngram_min_length: Option<i64>,
    ngram_max_length: Option<i64>,
    ngram_prefix_only: bool,
    memory_limit_mb: Option<i64>,
    num_workers: Option<i64>,
) -> Result<InvertedIndexParams, String> {
    let mut params = InvertedIndexParams::default()
        .base_tokenizer(tokenizer.to_string())
        .language(language)
        .map_err(|e| format!("invalid FTS language '{}': {}", language, e))?
        .with_position(with_position)
        .lower_case(lower_case)
        .stem(stem)
        .remove_stop_words(remove_stop_words)
        .ascii_folding(ascii_folding)
        .ngram_prefix_only(ngram_prefix_only);

    params = params.max_token_length(optional_usize(max_token_length, "max_token_length")?);

    if let Some(min_length) = optional_u32(ngram_min_length, "ngram_min_length")? {
        params = params.ngram_min_length(min_length);
    }
    if let Some(max_length) = optional_u32(ngram_max_length, "ngram_max_length")? {
        params = params.ngram_max_length(max_length);
    }
    if let Some(limit) = optional_u64(memory_limit_mb, "memory_limit_mb")? {
        params = params.memory_limit_mb(limit);
    }
    if let Some(workers) = optional_usize(num_workers, "num_workers")? {
        params = params.num_workers(workers);
    }

    Ok(params)
}

pub fn lance_create_scalar_index_impl(
    uri: &str,
    column_name: &str,
    index_name: &str,
    index_type: &str,
    replace: bool,
    server_name: Option<&str>,
) -> Result<IndexCreateResult, String> {
    let (index_type, params) = parse_scalar_index_type(index_type)?;
    let started = Instant::now();
    let rt = runtime()?;

    rt.block_on(async move {
        let mut dataset = open_dataset(uri, server_name).await?;
        let metadata = dataset
            .create_index(
                &[column_name],
                index_type,
                Some(index_name.to_string()),
                &params,
                replace,
            )
            .await
            .map_err(|e| format!("failed to create scalar index: {}", e))?;

        Ok((
            metadata.name,
            column_name.to_string(),
            index_type.to_string(),
            started.elapsed().as_millis() as i64,
        ))
    })
}

#[allow(clippy::too_many_arguments)]
pub fn lance_create_fts_index_impl(
    uri: &str,
    column_name: &str,
    index_name: &str,
    replace: bool,
    tokenizer: &str,
    language: &str,
    with_position: bool,
    lower_case: bool,
    stem: bool,
    remove_stop_words: bool,
    ascii_folding: bool,
    max_token_length: Option<i64>,
    ngram_min_length: Option<i64>,
    ngram_max_length: Option<i64>,
    ngram_prefix_only: bool,
    memory_limit_mb: Option<i64>,
    num_workers: Option<i64>,
    server_name: Option<&str>,
) -> Result<IndexCreateResult, String> {
    let params = build_fts_params(
        tokenizer,
        language,
        with_position,
        lower_case,
        stem,
        remove_stop_words,
        ascii_folding,
        max_token_length,
        ngram_min_length,
        ngram_max_length,
        ngram_prefix_only,
        memory_limit_mb,
        num_workers,
    )?;
    let started = Instant::now();
    let rt = runtime()?;

    rt.block_on(async move {
        let mut dataset = open_dataset(uri, server_name).await?;
        let metadata = dataset
            .create_index(
                &[column_name],
                IndexType::Inverted,
                Some(index_name.to_string()),
                &params,
                replace,
            )
            .await
            .map_err(|e| format!("failed to create FTS index: {}", e))?;

        Ok((
            metadata.name,
            column_name.to_string(),
            IndexType::Inverted.to_string(),
            started.elapsed().as_millis() as i64,
        ))
    })
}

pub fn lance_optimize_indices_impl(
    uri: &str,
    index_names: Vec<String>,
    mode: &str,
    num_indices_to_merge: Option<i64>,
    server_name: Option<&str>,
) -> Result<IndexOptimizeResult, String> {
    let mut options = parse_optimize_mode(mode, num_indices_to_merge)?;
    let optimized_index_count = index_names.len() as i64;
    if !index_names.is_empty() {
        options = options.index_names(index_names);
    }
    let started = Instant::now();
    let rt = runtime()?;

    rt.block_on(async move {
        let mut dataset = open_dataset(uri, server_name).await?;
        dataset
            .optimize_indices(&options)
            .await
            .map_err(|e| format!("failed to optimize indices: {}", e))?;

        Ok((optimized_index_count, started.elapsed().as_millis() as i64))
    })
}

pub fn lance_list_indices_impl(
    uri: &str,
    server_name: Option<&str>,
) -> Result<Vec<IndexListRow>, String> {
    let rt = runtime()?;

    rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        let descriptions = dataset
            .describe_indices(None)
            .await
            .map_err(|e| format!("failed to list indices: {}", e))?;
        let schema = dataset.schema();
        let mut rows = Vec::with_capacity(descriptions.len());

        for desc in descriptions {
            let columns = desc
                .field_ids()
                .iter()
                .map(|id| {
                    schema
                        .field_path(*id as i32)
                        .unwrap_or_else(|_| format!("<field:{}>", id))
                })
                .collect::<Vec<_>>();
            let column_names_json = serde_json::to_string(&columns)
                .map_err(|e| format!("failed to serialize index columns: {}", e))?;
            let details_json = desc
                .details()
                .unwrap_or_else(|e| json!({ "error": e.to_string() }).to_string());

            rows.push((
                desc.name().to_string(),
                desc.index_type().to_string(),
                column_names_json,
                desc.type_url().to_string(),
                desc.rows_indexed() as i64,
                desc.total_size_bytes().map(|n| n as i64),
                details_json,
            ));
        }

        Ok(rows)
    })
}

pub fn lance_index_stats_impl(
    uri: &str,
    index_name: &str,
    server_name: Option<&str>,
) -> Result<IndexStatsResult, String> {
    let rt = runtime()?;

    rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        let stats = dataset
            .index_statistics(index_name)
            .await
            .map_err(|e| format!("failed to get index statistics: {}", e))?;
        Ok((index_name.to_string(), stats))
    })
}

pub fn lance_drop_index_impl(
    uri: &str,
    index_name: &str,
    server_name: Option<&str>,
) -> Result<IndexDropResult, String> {
    let started = Instant::now();
    let rt = runtime()?;

    rt.block_on(async move {
        let mut dataset = open_dataset(uri, server_name).await?;
        dataset
            .drop_index(index_name)
            .await
            .map_err(|e| format!("failed to drop index: {}", e))?;

        Ok((index_name.to_string(), started.elapsed().as_millis() as i64))
    })
}

pub fn lance_fts_search_count_impl(
    uri: &str,
    column_name: &str,
    query_text: &str,
    limit: Option<i64>,
    server_name: Option<&str>,
) -> Result<FtsSearchCountResult, String> {
    let started = Instant::now();
    let rt = runtime()?;

    rt.block_on(async move {
        let dataset = open_dataset(uri, server_name).await?;
        let query = FullTextSearchQuery::new(query_text.to_string())
            .with_column(column_name.to_string())
            .map_err(|e| format!("failed to build FTS query: {}", e))?
            .limit(limit);
        let batch = dataset
            .scan()
            .project(&[column_name])
            .map_err(|e| format!("failed to project FTS column: {}", e))?
            .full_text_search(query)
            .map_err(|e| format!("failed to configure FTS search: {}", e))?
            .try_into_batch()
            .await
            .map_err(|e| format!("FTS search failed: {}", e))?;

        Ok((
            column_name.to_string(),
            query_text.to_string(),
            batch.num_rows() as i64,
            started.elapsed().as_millis() as i64,
        ))
    })
}
