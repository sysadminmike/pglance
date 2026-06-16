use crate::write::pg_to_arrow::for_each_spi_chunk;
use crate::write::storage::{open_dataset, storage_options_vec};
use lance_rs::dataset::WriteMode;
use lance_rs::io::{ObjectStoreParams, StorageOptionsAccessor};
use lance_rs::Dataset;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;

/// Execute `lance_append` — write rows from a PostgreSQL query into a Lance dataset.
///
/// Returns `(rows_written, duration_ms)`.
///
/// The source query is streamed through a server-side cursor in chunks of
/// `lance.write_chunk_rows` rows so that large writes do not buffer the entire
/// result set in memory. The first chunk uses the requested `mode`; subsequent
/// chunks append to the dataset created/overwritten by the first chunk.
pub fn lance_append_impl(
    uri: &str,
    source_query: &str,
    mode: &str,
    batch_size: usize,
    server_name: Option<&str>,
) -> Result<(i64, i64), String> {
    let start = Instant::now();

    match mode {
        "create" | "append" | "overwrite" => {}
        _ => {
            return Err(format!(
                "invalid mode '{}': must be 'create', 'append', or 'overwrite'",
                mode
            ))
        }
    }

    let rt = Runtime::new().map_err(|e| format!("failed to create tokio runtime: {}", e))?;
    let storage_opts: HashMap<String, String> =
        storage_options_vec(server_name)?.into_iter().collect();
    let chunk_rows = crate::write_chunk_rows();

    let mut first = true;

    let total_rows = for_each_spi_chunk(source_query, batch_size, chunk_rows, |schema, batches| {
        if batches.is_empty() {
            return Ok(());
        }

        // First chunk honors the requested mode; later chunks always append to it.
        let write_mode = if first {
            match mode {
                "create" => WriteMode::Create,
                "overwrite" => WriteMode::Overwrite,
                _ => WriteMode::Append,
            }
        } else {
            WriteMode::Append
        };

        rt.block_on(async {
            // For an append into a pre-existing dataset, verify it exists up front
            // so we surface a clear error (matching prior behavior).
            if first && mode == "append" {
                open_dataset(uri, server_name).await?;
            }

            let mut params = lance_rs::dataset::WriteParams {
                mode: write_mode,
                ..Default::default()
            };

            if !storage_opts.is_empty() {
                params.store_params = Some(ObjectStoreParams {
                    storage_options_accessor: Some(Arc::new(
                        StorageOptionsAccessor::with_static_options(storage_opts.clone()),
                    )),
                    ..Default::default()
                });
            }

            let reader = arrow::record_batch::RecordBatchIterator::new(
                batches.into_iter().map(Ok),
                schema.clone(),
            );

            Dataset::write(reader, uri, Some(params))
                .await
                .map_err(|e| format!("lance write failed: {}", e))?;

            Ok::<_, String>(())
        })?;

        first = false;
        Ok(())
    })?;

    let duration_ms = start.elapsed().as_millis() as i64;
    Ok((total_rows as i64, duration_ms))
}
