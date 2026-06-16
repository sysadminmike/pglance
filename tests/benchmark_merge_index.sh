#!/usr/bin/env bash
set -euo pipefail

PSQL=${PSQL:-psql}
ROOT=${LANCE_MERGE_BENCH_DIR:-/tmp/lance_merge_index_bench}
SERVER=${LANCE_MERGE_BENCH_SERVER:-lance_merge_bench_srv}
SCHEMA=${LANCE_MERGE_BENCH_SCHEMA:-lance_merge_bench}
INITIAL_ROWS=${BENCH_INITIAL_ROWS:-100000}
MERGE_ROWS=${BENCH_MERGE_ROWS:-25000}
BATCH_SIZE=${BENCH_BATCH_SIZE:-8192}

NO_INDEX_URI="${ROOT}/merge_no_index.lance"
SINGLE_INDEX_URI="${ROOT}/merge_single_index.lance"
MULTI_INDEX_URI="${ROOT}/merge_multi_index.lance"

run_psql() {
  "$PSQL" -v ON_ERROR_STOP=1 "$@"
}

rm -rf "$ROOT"
mkdir -p "$ROOT"
chmod 0777 "$ROOT"

run_psql "$@" <<SQL
CREATE EXTENSION IF NOT EXISTS lance;
DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE;
CREATE SCHEMA ${SCHEMA};
DROP SERVER IF EXISTS ${SERVER} CASCADE;
CREATE SERVER ${SERVER} FOREIGN DATA WRAPPER lance_fdw;

CREATE UNLOGGED TABLE ${SCHEMA}.bench_initial AS
SELECT (v % 50)::int4 AS tenant_id,
       v::int4 AS order_id,
       ('order_' || v::text) AS order_name,
       (v % 8)::int4 AS order_state,
       ('payload_' || md5(v::text)) AS payload
  FROM generate_series(1, ${INITIAL_ROWS}) v;

CREATE UNLOGGED TABLE ${SCHEMA}.bench_merge AS
SELECT (v % 50)::int4 AS tenant_id,
       v::int4 AS order_id,
       ('order_merged_' || v::text) AS order_name,
       ((v + 3) % 8)::int4 AS order_state,
       ('payload_merged_' || md5(v::text)) AS payload
  FROM generate_series((${INITIAL_ROWS} / 2), (${INITIAL_ROWS} / 2) + ${MERGE_ROWS} - 1) v;
SQL

echo "Preparing Lance datasets under ${ROOT}"
run_psql "$@" <<SQL
SELECT * FROM lance_append(
  '${NO_INDEX_URI}',
  'SELECT * FROM ${SCHEMA}.bench_initial',
  mode := 'create',
  batch_size := ${BATCH_SIZE}
);

SELECT * FROM lance_append(
  '${SINGLE_INDEX_URI}',
  'SELECT * FROM ${SCHEMA}.bench_initial',
  mode := 'create',
  batch_size := ${BATCH_SIZE}
);
SELECT * FROM lance_create_scalar_index('${SINGLE_INDEX_URI}', 'order_id', 'single_order_idx', 'btree');

SELECT * FROM lance_append(
  '${MULTI_INDEX_URI}',
  'SELECT * FROM ${SCHEMA}.bench_initial',
  mode := 'create',
  batch_size := ${BATCH_SIZE}
);
SELECT * FROM lance_create_scalar_index('${MULTI_INDEX_URI}', 'tenant_id', 'multi_tenant_idx', 'bitmap');
SELECT * FROM lance_create_scalar_index('${MULTI_INDEX_URI}', 'order_id', 'multi_order_idx', 'btree');
SQL

echo
echo "Merge benchmark settings: initial_rows=${INITIAL_ROWS}, merge_rows=${MERGE_ROWS}, batch_size=${BATCH_SIZE}"
echo

run_case() {
  local label=$1
  local sql=$2
  shift 2
  echo "== ${label} =="
  run_psql "$@" <<SQL
WITH started AS (SELECT clock_timestamp() AS t),
merged AS MATERIALIZED (
  ${sql}
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
}

run_case "no_index_multi_key" "SELECT * FROM lance_merge_insert('${NO_INDEX_URI}', 'SELECT * FROM ${SCHEMA}.bench_merge', ARRAY['tenant_id', 'order_id'], batch_size := ${BATCH_SIZE})" "$@"
run_case "indexed_single_id" "SELECT * FROM lance_merge_insert('${SINGLE_INDEX_URI}', 'SELECT * FROM ${SCHEMA}.bench_merge', ARRAY['tenant_id', 'order_id'], batch_size := ${BATCH_SIZE})" "$@"
run_case "indexed_multiple_ids" "SELECT * FROM lance_merge_insert('${MULTI_INDEX_URI}', 'SELECT * FROM ${SCHEMA}.bench_merge', ARRAY['tenant_id', 'order_id'], batch_size := ${BATCH_SIZE})" "$@"

echo
echo "Index health after benchmarked merges before catch-up"
run_psql "$@" <<SQL
SELECT 'single_order_idx' AS index_name, stats
  FROM lance_index_stats('${SINGLE_INDEX_URI}', 'single_order_idx')
UNION ALL
SELECT 'multi_tenant_idx' AS index_name, stats
  FROM lance_index_stats('${MULTI_INDEX_URI}', 'multi_tenant_idx')
UNION ALL
SELECT 'multi_order_idx' AS index_name, stats
  FROM lance_index_stats('${MULTI_INDEX_URI}', 'multi_order_idx');
SQL

echo
echo "Index catch-up maintenance"
run_psql "$@" <<SQL
SELECT 'indexed_single_id' AS case_name, *
  FROM lance_optimize_indices('${SINGLE_INDEX_URI}')
UNION ALL
SELECT 'indexed_multiple_ids' AS case_name, *
  FROM lance_optimize_indices('${MULTI_INDEX_URI}');
SQL

echo
echo "Index health after catch-up"
run_psql "$@" <<SQL
SELECT 'single_order_idx' AS index_name, stats
  FROM lance_index_stats('${SINGLE_INDEX_URI}', 'single_order_idx')
UNION ALL
SELECT 'multi_tenant_idx' AS index_name, stats
  FROM lance_index_stats('${MULTI_INDEX_URI}', 'multi_tenant_idx')
UNION ALL
SELECT 'multi_order_idx' AS index_name, stats
  FROM lance_index_stats('${MULTI_INDEX_URI}', 'multi_order_idx');
SQL
