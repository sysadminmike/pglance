#!/usr/bin/env bash
set -euo pipefail

PSQL=${PSQL:-psql}
ROOT=${LANCE_UUID_MERGE_BENCH_DIR:-/tmp/lance_uuid_merge_index_bench}
SERVER=${LANCE_UUID_MERGE_BENCH_SERVER:-lance_uuid_merge_bench_srv}
BASE_TABLE=${LANCE_UUID_BASE_TABLE:-pglance_bench_source.uuid_base_100m}
MERGE_TABLE=${LANCE_UUID_MERGE_TABLE:-pglance_bench_source.uuid_merge_1m}
BATCH_SIZE=${BENCH_BATCH_SIZE:-100000}
BASE_CHUNK_ROWS=${BENCH_BASE_CHUNK_ROWS:-5000000}
BASE_ROW_COLUMN=${LANCE_UUID_BASE_ROW_COLUMN:-row_num}

BASE_QUERY="SELECT id, tenant_id, order_id, order_state, amount_cents FROM ${BASE_TABLE}"
MERGE_QUERY="SELECT id, tenant_id, order_id, order_state, amount_cents FROM ${MERGE_TABLE}"

run_psql() {
  "$PSQL" -v ON_ERROR_STOP=1 "$@"
}

psql_scalar() {
  "$PSQL" -v ON_ERROR_STOP=1 -At "$@"
}

prepare_dataset() {
  local uri=$1
  shift

  local max_row
  max_row=$(psql_scalar "$@" -c "SELECT max(${BASE_ROW_COLUMN}) FROM ${BASE_TABLE};")
  if [[ -z "$max_row" ]]; then
    echo "No rows found in ${BASE_TABLE}" >&2
    exit 1
  fi

  local start_row=1
  local mode=create

  while (( start_row <= max_row )); do
    local end_row=$(( start_row + BASE_CHUNK_ROWS - 1 ))
    if (( end_row > max_row )); then
      end_row=$max_row
    fi

    echo "   appending ${start_row}-${end_row} (${mode})"
    run_psql "$@" <<SQL
SELECT * FROM lance_append(
  '${uri}',
  '${BASE_QUERY} WHERE ${BASE_ROW_COLUMN} BETWEEN ${start_row} AND ${end_row}',
  mode := '${mode}',
  batch_size := ${BATCH_SIZE}
);
SQL

    start_row=$(( end_row + 1 ))
    mode=append
  done
}

run_case() {
  local label=$1
  local uri=$2
  local merge_keys=$3
  local index_sql=${4:-}
  local stats_sql=${5:-}
  shift 5

  rm -rf "$uri"

  echo
  echo "== ${label}: prepare dataset =="
  prepare_dataset "$uri" "$@"

  if [[ -n "$index_sql" ]]; then
    echo
    echo "== ${label}: create indexes =="
    run_psql "$@" <<<"${index_sql}"
  fi

  echo
  echo "== ${label}: merge =="
  run_psql "$@" <<SQL
WITH started AS (SELECT clock_timestamp() AS t),
merged AS MATERIALIZED (
  SELECT * FROM lance_merge_insert(
    '${uri}',
    '${MERGE_QUERY}',
    ${merge_keys},
    batch_size := ${BATCH_SIZE}
  )
),
elapsed AS (
  SELECT round(extract(epoch FROM clock_timestamp() - started.t) * 1000)::bigint AS wall_ms
    FROM started, (SELECT count(*) FROM merged) force_merge
)
SELECT '${label}' AS case_name,
       merged.rows_merged,
       merged.duration_ms AS lance_reported_ms,
       elapsed.wall_ms
  FROM merged, elapsed;
SQL

  if [[ -n "$stats_sql" ]]; then
    echo
    echo "== ${label}: index health before catch-up =="
    run_psql "$@" <<<"${stats_sql}"

    echo
    echo "== ${label}: index catch-up =="
    run_psql "$@" <<SQL
SELECT '${label}' AS case_name, *
  FROM lance_optimize_indices('${uri}');
SQL

    echo
    echo "== ${label}: index health after catch-up =="
    run_psql "$@" <<<"${stats_sql}"
  fi

  rm -rf "$uri"
}

rm -rf "$ROOT"
mkdir -p "$ROOT"
chmod 0777 "$ROOT"

run_psql "$@" <<SQL
CREATE EXTENSION IF NOT EXISTS lance;
DROP SERVER IF EXISTS ${SERVER} CASCADE;
CREATE SERVER ${SERVER} FOREIGN DATA WRAPPER lance_fdw;

SELECT tablename AS table_name,
       reltuples::bigint AS estimated_rows,
       pg_size_pretty(pg_total_relation_size(format('%I.%I', schemaname, tablename)::regclass)) AS total_size
  FROM pg_tables
  JOIN pg_class ON pg_class.oid = format('%I.%I', schemaname, tablename)::regclass
 WHERE schemaname = split_part('${BASE_TABLE}', '.', 1)
   AND tablename IN (split_part('${BASE_TABLE}', '.', 2), split_part('${MERGE_TABLE}', '.', 2))
 ORDER BY tablename;

SELECT should_match, count(*) AS rows
  FROM ${MERGE_TABLE}
 GROUP BY should_match
 ORDER BY should_match DESC;
SQL

echo
echo "UUID merge benchmark settings: base_table=${BASE_TABLE}, merge_table=${MERGE_TABLE}, batch_size=${BATCH_SIZE}, base_chunk_rows=${BASE_CHUNK_ROWS}"
echo "Each case uses the saved UUID source tables; datasets are deleted after each case."

run_case \
  "no_index_uuid_id" \
  "${ROOT}/no_index_uuid_id.lance" \
  "ARRAY['id']" \
  "" \
  "" \
  "$@"

run_case \
  "no_index_uuid_multi_key" \
  "${ROOT}/no_index_uuid_multi_key.lance" \
  "ARRAY['tenant_id', 'order_id']" \
  "" \
  "" \
  "$@"

run_case \
  "indexed_uuid_id" \
  "${ROOT}/indexed_uuid_id.lance" \
  "ARRAY['id']" \
  "SELECT * FROM lance_create_scalar_index('${ROOT}/indexed_uuid_id.lance', 'id', 'uuid_id_idx', 'btree');" \
  "SELECT 'uuid_id_idx' AS index_name, stats FROM lance_index_stats('${ROOT}/indexed_uuid_id.lance', 'uuid_id_idx');" \
  "$@"

run_case \
  "indexed_uuid_multi_key" \
  "${ROOT}/indexed_uuid_multi_key.lance" \
  "ARRAY['tenant_id', 'order_id']" \
  "SELECT * FROM lance_create_scalar_index('${ROOT}/indexed_uuid_multi_key.lance', 'tenant_id', 'uuid_tenant_idx', 'btree');
SELECT * FROM lance_create_scalar_index('${ROOT}/indexed_uuid_multi_key.lance', 'order_id', 'uuid_order_idx', 'btree');" \
  "SELECT 'uuid_tenant_idx' AS index_name, stats FROM lance_index_stats('${ROOT}/indexed_uuid_multi_key.lance', 'uuid_tenant_idx')
UNION ALL
SELECT 'uuid_order_idx' AS index_name, stats FROM lance_index_stats('${ROOT}/indexed_uuid_multi_key.lance', 'uuid_order_idx');" \
  "$@"