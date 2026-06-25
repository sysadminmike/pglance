use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use pgrx::JsonB;

mod fdw;
mod write;

pgrx::pg_module_magic!();

/// Maximum amount of source data (in MB) buffered in memory before a Lance
/// write/merge aborts. Guards against the OOM killer taking down PostgreSQL on
/// very large `lance_append` / `lance_merge_insert` operations. `0` disables it.
static MAX_WRITE_BUFFER_MB: GucSetting<i32> = GucSetting::<i32>::new(2048);

/// Number of source rows fetched and processed per chunk during
/// `lance_append` / `lance_merge_insert`. Chunking bounds peak memory so large
/// operations complete instead of being OOM-killed. `0` disables chunking
/// (the entire source is processed in a single pass).
static WRITE_CHUNK_ROWS: GucSetting<i32> = GucSetting::<i32>::new(100_000);

/// Whether Lance merge-insert may use scalar indexes on the join keys.
/// Disable this to force Lance's full-scan merge path when indexed merge hits
/// upstream Lance bugs or memory pressure.
static MERGE_USE_INDEX: GucSetting<bool> = GucSetting::<bool>::new(true);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_int_guc(
        "lance.max_write_buffer_mb",
        "Max MB of source rows buffered in memory before a Lance write/merge aborts.",
        "lance_append and lance_merge_insert stage the entire source query in memory as Arrow \
         batches before writing. This limit aborts cleanly (rolling back the transaction) once \
         the buffered data exceeds the configured size, instead of letting the Linux OOM killer \
         terminate PostgreSQL. Set to 0 to disable the guard.",
        &MAX_WRITE_BUFFER_MB,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "lance.write_chunk_rows",
        "Source rows processed per chunk during a Lance write/merge.",
        "lance_append and lance_merge_insert stream the source query through a server-side cursor \
         in chunks of this many rows, bounding peak memory so very large operations complete \
         instead of being OOM-killed. Note: merges run one Lance commit per chunk, so a failure \
         partway through leaves earlier chunks applied. Set to 0 to process the whole source in a \
         single pass (subject to lance.max_write_buffer_mb).",
        &WRITE_CHUNK_ROWS,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "lance.merge_use_index",
        "Allow Lance merge-insert to use scalar indexes on join keys.",
        "When enabled, lance_merge_insert allows the Lance SDK to use scalar indexes on merge \
         join keys when available. Disable this to force Lance's full-scan merge path, which is \
         useful as a workaround for indexed merge-insert bugs or excessive indexed-join memory \
         use.",
        &MERGE_USE_INDEX,
        GucContext::Userset,
        GucFlags::default(),
    );
}

/// The configured write-buffer guard limit in bytes, or `0` when disabled.
pub fn max_write_buffer_bytes() -> usize {
    let mb = MAX_WRITE_BUFFER_MB.get();
    if mb <= 0 {
        0
    } else {
        (mb as usize).saturating_mul(1024 * 1024)
    }
}

/// The configured per-chunk source row count, or `0` when chunking is disabled.
pub fn write_chunk_rows() -> usize {
    let n = WRITE_CHUNK_ROWS.get();
    if n <= 0 {
        0
    } else {
        n as usize
    }
}

/// Whether Lance merge-insert may use scalar indexes on join keys.
pub fn merge_use_index() -> bool {
    MERGE_USE_INDEX.get()
}

fn optional_build_value(value: &'static str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn pg_feature() -> &'static str {
    #[cfg(feature = "pg17")]
    {
        "pg17"
    }
    #[cfg(all(not(feature = "pg17"), feature = "pg16"))]
    {
        "pg16"
    }
    #[cfg(all(not(feature = "pg17"), not(feature = "pg16"), feature = "pg15"))]
    {
        "pg15"
    }
    #[cfg(all(
        not(feature = "pg17"),
        not(feature = "pg16"),
        not(feature = "pg15"),
        feature = "pg14"
    ))]
    {
        "pg14"
    }
    #[cfg(all(
        not(feature = "pg17"),
        not(feature = "pg16"),
        not(feature = "pg15"),
        not(feature = "pg14"),
        feature = "pg13"
    ))]
    {
        "pg13"
    }
    #[cfg(not(any(
        feature = "pg17",
        feature = "pg16",
        feature = "pg15",
        feature = "pg14",
        feature = "pg13"
    )))]
    {
        "unknown"
    }
}

#[pg_extern]
fn lance_build_info() -> TableIterator<
    'static,
    (
        name!(extension_name, String),
        name!(pglance_version, String),
        name!(pglance_git_revision, Option<String>),
        name!(lance_version, Option<String>),
        name!(lance_git_revision, Option<String>),
        name!(lance_source, Option<String>),
        name!(lance_index_version, Option<String>),
        name!(lance_namespace_version, Option<String>),
        name!(lance_namespace_impls_version, Option<String>),
        name!(pg_feature, String),
        name!(build_profile, Option<String>),
        name!(rustc_version, Option<String>),
    ),
> {
    TableIterator::new(vec![(
        "lance".to_string(),
        env!("CARGO_PKG_VERSION").to_string(),
        optional_build_value(env!("PGLANCE_GIT_REVISION")),
        optional_build_value(env!("PGLANCE_DEP_LANCE_VERSION")),
        optional_build_value(env!("PGLANCE_DEP_LANCE_REVISION")),
        optional_build_value(env!("PGLANCE_DEP_LANCE_SOURCE")),
        optional_build_value(env!("PGLANCE_DEP_LANCE_INDEX_VERSION")),
        optional_build_value(env!("PGLANCE_DEP_LANCE_NAMESPACE_VERSION")),
        optional_build_value(env!("PGLANCE_DEP_LANCE_NAMESPACE_IMPLS_VERSION")),
        pg_feature().to_string(),
        optional_build_value(env!("PGLANCE_BUILD_PROFILE")),
        optional_build_value(env!("PGLANCE_RUSTC_VERSION")),
    )])
}

pgrx::extension_sql!(
    r#"
CREATE FOREIGN DATA WRAPPER lance_fdw
HANDLER lance_fdw_handler
VALIDATOR lance_fdw_validator;
"#,
    name = "lance_fdw",
    requires = [lance_fdw_handler, lance_fdw_validator]
);

#[pg_extern]
unsafe fn lance_fdw_handler() -> PgBox<pg_sys::FdwRoutine> {
    fdw::handler::build_fdw_routine()
}

#[pg_extern]
fn lance_fdw_validator(options: Vec<String>, _catalog: pg_sys::Oid) {
    for opt in options {
        let (k, _v) = opt
            .split_once('=')
            .ok_or_else(|| format!("invalid option format: {}", opt))
            .unwrap_or_else(|e| pgrx::error!("{}", e));

        if k == "uri"
            || k == "batch_size"
            || k.starts_with("aws_")
            || k.starts_with("s3_")
            || k.starts_with("ns.")
        {
            continue;
        }

        pgrx::error!("unsupported option: {}", k);
    }
}

#[pg_extern]
fn lance_import(
    server_name: &str,
    local_schema: &str,
    table_name: &str,
    uri: &str,
    batch_size: default!(Option<i64>, "NULL"),
) {
    fdw::import::import_lance_table(server_name, local_schema, table_name, uri, batch_size)
        .unwrap_or_else(|e| pgrx::error!("lance_import failed: {}", e));
}

#[pg_extern]
fn lance_attach_namespace(
    server_name: &str,
    root_namespace_id: default!(Vec<String>, "ARRAY[]::text[]"),
    schema_prefix: default!(&str, "'lance'"),
    batch_size: default!(Option<i64>, "NULL"),
    limit_per_list_call: default!(i32, "1000"),
    dry_run: default!(bool, "false"),
) -> TableIterator<
    'static,
    (
        name!(table_id, JsonB),
        name!(local_schema, String),
        name!(local_table, String),
        name!(action, String),
        name!(status, String),
        name!(detail, String),
    ),
> {
    let rows = fdw::attach_namespace::attach_namespace(
        server_name,
        root_namespace_id,
        schema_prefix,
        batch_size,
        limit_per_list_call,
        dry_run,
    )
    .unwrap_or_else(|e| pgrx::error!("lance_attach_namespace failed: {}", e));

    TableIterator::new(rows)
}

#[pg_extern]
fn lance_sync_namespace(
    server_name: &str,
    root_namespace_id: default!(Vec<String>, "ARRAY[]::text[]"),
    schema_prefix: default!(&str, "'lance'"),
    drop_missing: default!(bool, "false"),
    recreate_changed: default!(bool, "false"),
    dry_run: default!(bool, "false"),
) -> TableIterator<
    'static,
    (
        name!(table_id, JsonB),
        name!(local_schema, String),
        name!(local_table, String),
        name!(action, String),
        name!(status, String),
        name!(detail, String),
    ),
> {
    let rows = fdw::sync_namespace::sync_namespace(
        server_name,
        root_namespace_id,
        schema_prefix,
        drop_missing,
        recreate_changed,
        dry_run,
    )
    .unwrap_or_else(|e| pgrx::error!("lance_sync_namespace failed: {}", e));

    TableIterator::new(rows)
}

#[pg_extern]
fn lance_append(
    uri: &str,
    source_query: &str,
    mode: default!(&str, "'append'"),
    batch_size: default!(i32, "1024"),
    server_name: default!(Option<&str>, "NULL"),
) -> TableIterator<'static, (name!(rows_written, i64), name!(duration_ms, i64))> {
    let (rows_written, duration_ms) =
        write::append::lance_append_impl(uri, source_query, mode, batch_size as usize, server_name)
            .unwrap_or_else(|e| pgrx::error!("lance_append failed: {}", e));

    TableIterator::new(vec![(rows_written, duration_ms)])
}

#[pg_extern]
fn lance_merge_insert(
    uri: &str,
    source_query: &str,
    on_columns: Vec<String>,
    when_matched: default!(&str, "'update'"),
    when_not_matched: default!(&str, "'insert'"),
    batch_size: default!(i32, "1024"),
    server_name: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(rows_merged, i64),
        name!(rows_inserted, Option<i64>),
        name!(rows_updated, Option<i64>),
        name!(duration_ms, i64),
        name!(chunk_txns, i64),
        name!(chunk_rows, i64),
    ),
> {
    let (rows_merged, rows_inserted, rows_updated, duration_ms, chunk_txns, chunk_rows) =
        write::merge_insert::lance_merge_insert_impl(
            uri,
            source_query,
            on_columns,
            when_matched,
            when_not_matched,
            batch_size as usize,
            server_name,
        )
        .unwrap_or_else(|e| pgrx::error!("lance_merge_insert failed: {}", e));

    let inserted = if rows_inserted < 0 {
        None
    } else {
        Some(rows_inserted)
    };
    let updated = if rows_updated < 0 {
        None
    } else {
        Some(rows_updated)
    };

    TableIterator::new(vec![(
        rows_merged,
        inserted,
        updated,
        duration_ms,
        chunk_txns,
        chunk_rows,
    )])
}

#[pg_extern]
fn lance_delete(
    uri: &str,
    predicate: &str,
    server_name: default!(Option<&str>, "NULL"),
) -> TableIterator<'static, (name!(fragments_removed, i64), name!(duration_ms, i64))> {
    let (fragments_removed, duration_ms) =
        write::delete::lance_delete_impl(uri, predicate, server_name)
            .unwrap_or_else(|e| pgrx::error!("lance_delete failed: {}", e));

    TableIterator::new(vec![(fragments_removed, duration_ms)])
}

#[pg_extern]
fn lance_create_scalar_index(
    uri: &str,
    column_name: &str,
    index_name: &str,
    index_type: default!(&str, "'btree'"),
    replace: default!(bool, "false"),
    server_name: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(index_name, String),
        name!(column_name, String),
        name!(index_type, String),
        name!(duration_ms, i64),
    ),
> {
    let row = write::index::lance_create_scalar_index_impl(
        uri,
        column_name,
        index_name,
        index_type,
        replace,
        server_name,
    )
    .unwrap_or_else(|e| pgrx::error!("lance_create_scalar_index failed: {}", e));

    TableIterator::new(vec![row])
}

#[pg_extern]
#[allow(clippy::too_many_arguments)]
fn lance_create_fts_index(
    uri: &str,
    column_name: &str,
    index_name: &str,
    replace: default!(bool, "false"),
    tokenizer: default!(&str, "'simple'"),
    language: default!(&str, "'English'"),
    with_position: default!(bool, "false"),
    lower_case: default!(bool, "true"),
    stem: default!(bool, "false"),
    remove_stop_words: default!(bool, "false"),
    ascii_folding: default!(bool, "false"),
    max_token_length: default!(Option<i64>, "NULL"),
    ngram_min_length: default!(Option<i64>, "NULL"),
    ngram_max_length: default!(Option<i64>, "NULL"),
    ngram_prefix_only: default!(bool, "false"),
    memory_limit_mb: default!(Option<i64>, "NULL"),
    num_workers: default!(Option<i64>, "NULL"),
    server_name: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(index_name, String),
        name!(column_name, String),
        name!(index_type, String),
        name!(duration_ms, i64),
    ),
> {
    let row = write::index::lance_create_fts_index_impl(
        uri,
        column_name,
        index_name,
        replace,
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
        server_name,
    )
    .unwrap_or_else(|e| pgrx::error!("lance_create_fts_index failed: {}", e));

    TableIterator::new(vec![row])
}

#[pg_extern]
fn lance_optimize_indices(
    uri: &str,
    index_names: default!(Vec<String>, "ARRAY[]::text[]"),
    mode: default!(&str, "'append'"),
    num_indices_to_merge: default!(Option<i64>, "NULL"),
    server_name: default!(Option<&str>, "NULL"),
) -> TableIterator<'static, (name!(requested_index_count, i64), name!(duration_ms, i64))> {
    let row = write::index::lance_optimize_indices_impl(
        uri,
        index_names,
        mode,
        num_indices_to_merge,
        server_name,
    )
    .unwrap_or_else(|e| pgrx::error!("lance_optimize_indices failed: {}", e));

    TableIterator::new(vec![row])
}

#[pg_extern]
#[allow(clippy::type_complexity)]
fn lance_list_indices(
    uri: &str,
    server_name: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(index_name, String),
        name!(index_type, String),
        name!(column_names, JsonB),
        name!(type_url, String),
        name!(rows_indexed, i64),
        name!(total_size_bytes, Option<i64>),
        name!(details, JsonB),
    ),
> {
    let rows = write::index::lance_list_indices_impl(uri, server_name)
        .unwrap_or_else(|e| pgrx::error!("lance_list_indices failed: {}", e))
        .into_iter()
        .map(
            |(
                index_name,
                index_type,
                column_names,
                type_url,
                rows_indexed,
                total_size_bytes,
                details,
            )| {
                let column_names = serde_json::from_str(&column_names).unwrap_or_else(|e| {
                    pgrx::error!("lance_list_indices failed to parse columns: {}", e)
                });
                let details = serde_json::from_str(&details).unwrap_or_else(|e| {
                    pgrx::error!("lance_list_indices failed to parse details: {}", e)
                });

                (
                    index_name,
                    index_type,
                    JsonB(column_names),
                    type_url,
                    rows_indexed,
                    total_size_bytes,
                    JsonB(details),
                )
            },
        )
        .collect::<Vec<_>>();

    TableIterator::new(rows)
}

#[pg_extern]
fn lance_index_stats(
    uri: &str,
    index_name: &str,
    server_name: default!(Option<&str>, "NULL"),
) -> TableIterator<'static, (name!(index_name, String), name!(stats, JsonB))> {
    let (index_name, stats) = write::index::lance_index_stats_impl(uri, index_name, server_name)
        .unwrap_or_else(|e| pgrx::error!("lance_index_stats failed: {}", e));
    let stats = serde_json::from_str(&stats)
        .unwrap_or_else(|e| pgrx::error!("lance_index_stats failed to parse stats: {}", e));

    TableIterator::new(vec![(index_name, JsonB(stats))])
}

#[pg_extern]
fn lance_drop_index(
    uri: &str,
    index_name: &str,
    server_name: default!(Option<&str>, "NULL"),
) -> TableIterator<'static, (name!(index_name, String), name!(duration_ms, i64))> {
    let row = write::index::lance_drop_index_impl(uri, index_name, server_name)
        .unwrap_or_else(|e| pgrx::error!("lance_drop_index failed: {}", e));

    TableIterator::new(vec![row])
}

#[pg_extern]
fn lance_fts_search_count(
    uri: &str,
    column_name: &str,
    query_text: &str,
    limit: default!(Option<i64>, "NULL"),
    server_name: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(column_name, String),
        name!(query_text, String),
        name!(rows_matched, i64),
        name!(duration_ms, i64),
    ),
> {
    let row =
        write::index::lance_fts_search_count_impl(uri, column_name, query_text, limit, server_name)
            .unwrap_or_else(|e| pgrx::error!("lance_fts_search_count failed: {}", e));

    TableIterator::new(vec![row])
}

#[pg_extern]
#[allow(clippy::too_many_arguments)]
fn lance_optimize(
    uri: &str,
    target_rows_per_fragment: default!(Option<i64>, "NULL"),
    max_rows_per_group: default!(Option<i64>, "NULL"),
    max_bytes_per_file: default!(Option<i64>, "NULL"),
    materialize_deletions: default!(bool, "true"),
    materialize_deletions_threshold: default!(f32, "0.1"),
    num_threads: default!(Option<i64>, "NULL"),
    batch_size: default!(Option<i64>, "NULL"),
    defer_index_remap: default!(bool, "false"),
    compaction_mode: default!(Option<&str>, "NULL"),
    max_source_fragments: default!(Option<i64>, "NULL"),
    io_buffer_size: default!(Option<i64>, "NULL"),
    server_name: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(fragments_removed, i64),
        name!(fragments_added, i64),
        name!(files_removed, i64),
        name!(files_added, i64),
        name!(duration_ms, i64),
    ),
> {
    let (fragments_removed, fragments_added, files_removed, files_added, duration_ms) =
        write::optimize::lance_optimize_impl(
            uri,
            target_rows_per_fragment,
            max_rows_per_group,
            max_bytes_per_file,
            materialize_deletions,
            materialize_deletions_threshold,
            num_threads,
            batch_size,
            defer_index_remap,
            compaction_mode,
            max_source_fragments,
            io_buffer_size,
            server_name,
        )
        .unwrap_or_else(|e| pgrx::error!("lance_optimize failed: {}", e));

    TableIterator::new(vec![(
        fragments_removed,
        fragments_added,
        files_removed,
        files_added,
        duration_ms,
    )])
}

#[pg_extern]
#[allow(clippy::too_many_arguments)]
fn lance_vacuum(
    uri: &str,
    older_than_seconds: default!(Option<i64>, "604800"),
    before_version: default!(Option<i64>, "NULL"),
    delete_unverified: default!(bool, "false"),
    error_if_tagged_old_versions: default!(bool, "false"),
    clean_referenced_branches: default!(bool, "false"),
    delete_rate_limit: default!(Option<i64>, "NULL"),
    server_name: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(bytes_removed, i64),
        name!(old_versions, i64),
        name!(data_files_removed, i64),
        name!(transaction_files_removed, i64),
        name!(index_files_removed, i64),
        name!(deletion_files_removed, i64),
        name!(duration_ms, i64),
    ),
> {
    let (
        bytes_removed,
        old_versions,
        data_files_removed,
        transaction_files_removed,
        index_files_removed,
        deletion_files_removed,
        duration_ms,
    ) = write::vacuum::lance_vacuum_impl(
        uri,
        older_than_seconds,
        before_version,
        delete_unverified,
        error_if_tagged_old_versions,
        clean_referenced_branches,
        delete_rate_limit,
        server_name,
    )
    .unwrap_or_else(|e| pgrx::error!("lance_vacuum failed: {}", e));

    TableIterator::new(vec![(
        bytes_removed,
        old_versions,
        data_files_removed,
        transaction_files_removed,
        index_files_removed,
        deletion_files_removed,
        duration_ms,
    )])
}

#[cfg(any(test, feature = "pg_test"))]
mod tests;

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
