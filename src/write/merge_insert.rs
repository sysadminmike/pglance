use crate::write::pg_to_arrow::{
    for_each_spi_chunk, for_each_spi_chunk_with_type_overrides, ArrowTypeOverrides,
};
use crate::write::storage::open_dataset;
use lance_rs::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched};
use lance_rs::Dataset;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;

/// Execute `lance_merge_insert` — upsert rows from a PostgreSQL query into a Lance dataset.
///
/// Returns `(rows_merged, rows_inserted, rows_updated, duration_ms, chunk_txns, chunk_rows)`.
/// Note: The Lance SDK may not return separate insert/update counts. If unavailable,
/// `rows_inserted` and `rows_updated` will be -1 (indicating unknown).
///
/// The source query is streamed through a server-side cursor in chunks of
/// `lance.write_chunk_rows` rows so that very large merges do not buffer the
/// entire result set in memory. Each chunk is applied as its own Lance merge
/// commit; a failure partway through therefore leaves earlier chunks applied.
pub fn lance_merge_insert_impl(
    uri: &str,
    source_query: &str,
    on_columns: Vec<String>,
    when_matched: &str,
    when_not_matched: &str,
    batch_size: usize,
    server_name: Option<&str>,
) -> Result<(i64, i64, i64, i64, i64, i64), String> {
    lance_merge_insert_impl_inner(
        uri,
        source_query,
        on_columns,
        when_matched,
        when_not_matched,
        batch_size,
        server_name,
        None,
    )
}

pub fn lance_merge_insert_with_schema_impl(
    uri: &str,
    source_query: &str,
    on_columns: Vec<String>,
    when_matched: &str,
    when_not_matched: &str,
    batch_size: usize,
    server_name: Option<&str>,
    type_overrides: &ArrowTypeOverrides,
) -> Result<(i64, i64, i64, i64, i64, i64), String> {
    lance_merge_insert_impl_inner(
        uri,
        source_query,
        on_columns,
        when_matched,
        when_not_matched,
        batch_size,
        server_name,
        Some(type_overrides),
    )
}

#[allow(clippy::too_many_arguments)]
fn lance_merge_insert_impl_inner(
    uri: &str,
    source_query: &str,
    on_columns: Vec<String>,
    when_matched: &str,
    when_not_matched: &str,
    batch_size: usize,
    server_name: Option<&str>,
    type_overrides: Option<&ArrowTypeOverrides>,
) -> Result<(i64, i64, i64, i64, i64, i64), String> {
    let start = Instant::now();

    if on_columns.is_empty() {
        return Err("on_columns must not be empty".to_string());
    }

    let rt = Runtime::new().map_err(|e| format!("failed to create tokio runtime: {}", e))?;
    let chunk_rows = crate::write_chunk_rows();
    let merge_use_index = crate::merge_use_index();

    // The dataset is opened on the first chunk and replaced with the updated
    // dataset returned by each merge so the next chunk sees prior changes.
    let mut dataset: Option<Arc<Dataset>> = None;
    let mut chunk_txns: i64 = 0;

    let mut handle_chunk =
        |schema: &Arc<arrow::datatypes::Schema>, batches: Vec<arrow::record_batch::RecordBatch>| {
            if batches.is_empty() {
                return Ok(());
            }

            if dataset.is_none() {
                let ds = rt.block_on(open_dataset(uri, server_name))?;

                // Validate on_columns exist in both the source and Lance schemas.
                for col in &on_columns {
                    if schema.column_with_name(col).is_none() {
                        return Err(format!(
                            "on_column '{}' not found in source query result (columns: {})",
                            col,
                            schema
                                .fields()
                                .iter()
                                .map(|f| f.name().as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ));
                    }
                }
                let lance_field_names: Vec<String> =
                    ds.schema().fields.iter().map(|f| f.name.clone()).collect();
                for col in &on_columns {
                    if !lance_field_names.contains(col) {
                        return Err(format!(
                            "on_column '{}' not found in Lance schema (columns: {})",
                            col,
                            lance_field_names.join(", ")
                        ));
                    }
                }

                dataset = Some(Arc::new(ds));
            }

            let current = dataset.take().expect("dataset initialized on first chunk");

            let new_dataset = rt.block_on(async {
                let reader = arrow::record_batch::RecordBatchIterator::new(
                    batches.into_iter().map(Ok),
                    schema.clone(),
                );

                let mut builder = MergeInsertBuilder::try_new(current, on_columns.clone())
                    .map_err(|e| format!("MergeInsertBuilder::try_new failed: {}", e))?;

                if !merge_use_index {
                    builder.use_index(false);
                }

                match when_matched {
                    "update" => {
                        builder.when_matched(WhenMatched::UpdateAll);
                    }
                    "nothing" => {
                        builder.when_matched(WhenMatched::DoNothing);
                    }
                    _ => {
                        return Err(format!(
                            "invalid when_matched value '{}': must be 'update' or 'nothing'",
                            when_matched
                        ));
                    }
                }

                match when_not_matched {
                    "insert" => {
                        builder.when_not_matched(WhenNotMatched::InsertAll);
                    }
                    "nothing" => {
                        builder.when_not_matched(WhenNotMatched::DoNothing);
                    }
                    _ => {
                        return Err(format!(
                            "invalid when_not_matched value '{}': must be 'insert' or 'nothing'",
                            when_not_matched
                        ));
                    }
                }

                let (ds, _stats) = builder
                    .try_build()
                    .map_err(|e| format!("merge_insert try_build failed: {}", e))?
                    .execute_reader(reader)
                    .await
                    .map_err(|e| format!("lance merge_insert failed: {}", e))?;

                Ok(ds)
            })?;

            dataset = Some(new_dataset);
            chunk_txns += 1;
            Ok(())
        };

    let total_rows = if let Some(type_overrides) = type_overrides {
        for_each_spi_chunk_with_type_overrides(
            source_query,
            batch_size,
            chunk_rows,
            Some(type_overrides),
            &mut handle_chunk,
        )?
    } else {
        for_each_spi_chunk(source_query, batch_size, chunk_rows, &mut handle_chunk)?
    };

    let duration_ms = start.elapsed().as_millis() as i64;
    let chunk_rows_i64 = chunk_rows as i64;
    // Lance merge_insert does not provide separate insert/update counts here.
    // For an empty source (no rows processed) report 0/0; otherwise unknown (-1).
    let (rows_inserted, rows_updated) = if total_rows == 0 { (0, 0) } else { (-1, -1) };
    Ok((
        total_rows as i64,
        rows_inserted,
        rows_updated,
        duration_ms,
        chunk_txns,
        chunk_rows_i64,
    ))
}
