#!/usr/bin/env bash
set -euo pipefail

PSQL=${PSQL:-psql}
ROOT=${LANCE_INDEX_E2E_DIR:-/tmp/lance_index_e2e}
SERVER=${LANCE_INDEX_E2E_SERVER:-lance_index_e2e_srv}
SCHEMA=${LANCE_INDEX_E2E_SCHEMA:-lance_index_e2e}
DATASET="${ROOT}/indexed_docs.lance"

run_psql() {
  "$PSQL" -v ON_ERROR_STOP=1 "$@"
}

scalar_sql() {
  run_psql "$@" -Atq
}

assert_eq() {
  local actual=$1
  local expected=$2
  local message=$3
  if [[ "$actual" != "$expected" ]]; then
    echo "assertion failed: ${message}: expected '${expected}', got '${actual}'" >&2
    exit 1
  fi
}

rm -rf "$ROOT"
mkdir -p "$ROOT"

run_psql "$@" <<SQL
CREATE EXTENSION IF NOT EXISTS lance;
DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE;
CREATE SCHEMA ${SCHEMA};
DROP SERVER IF EXISTS ${SERVER} CASCADE;
CREATE SERVER ${SERVER} FOREIGN DATA WRAPPER lance_fdw;
SQL

run_psql "$@" <<SQL
SELECT * FROM lance_append(
  '${DATASET}',
  'SELECT v::int4 AS id, (v % 3)::int4 AS bucket, ''document '' || v::text || '' alpha'' AS body FROM generate_series(1, 12) v',
  mode := 'create'
);

SELECT * FROM lance_create_scalar_index('${DATASET}', 'id', 'id_idx', 'btree');
SELECT * FROM lance_create_scalar_index('${DATASET}', 'bucket', 'bucket_idx', 'bitmap');
SELECT * FROM lance_create_fts_index('${DATASET}', 'body', 'body_fts_idx', with_position := true);
SQL

index_count=$(scalar_sql "$@" -c "SELECT count(*) FROM lance_list_indices('${DATASET}')")
assert_eq "$index_count" "3" "three indexes should exist"

initial_unindexed=$(scalar_sql "$@" -c "SELECT stats->>'num_unindexed_rows' FROM lance_index_stats('${DATASET}', 'id_idx')")
assert_eq "$initial_unindexed" "0" "fresh scalar index should cover all rows"

run_psql "$@" <<SQL
SELECT * FROM lance_append(
  '${DATASET}',
  'SELECT v::int4 AS id, (v % 3)::int4 AS bucket, ''document '' || v::text || '' beta'' AS body FROM generate_series(13, 18) v'
);
SQL

unindexed_after_append=$(scalar_sql "$@" -c "SELECT (stats->>'num_unindexed_rows')::bigint > 0 FROM lance_index_stats('${DATASET}', 'id_idx')")
assert_eq "$unindexed_after_append" "t" "append should leave scalar index with unindexed rows"

run_psql "$@" <<SQL
SELECT * FROM lance_optimize_indices(
  '${DATASET}',
  index_names := ARRAY['id_idx', 'bucket_idx', 'body_fts_idx'],
  mode := 'append'
);
SQL

id_unindexed=$(scalar_sql "$@" -c "SELECT stats->>'num_unindexed_rows' FROM lance_index_stats('${DATASET}', 'id_idx')")
fts_unindexed=$(scalar_sql "$@" -c "SELECT stats->>'num_unindexed_rows' FROM lance_index_stats('${DATASET}', 'body_fts_idx')")
assert_eq "$id_unindexed" "0" "scalar index should be caught up after optimize_indices"
assert_eq "$fts_unindexed" "0" "FTS index should be caught up after optimize_indices"

run_psql "$@" <<SQL
SELECT lance_import('${SERVER}', '${SCHEMA}', 'indexed_docs', '${DATASET}', batch_size => NULL);
SQL

row_count=$(scalar_sql "$@" -c "SELECT count(*) FROM ${SCHEMA}.indexed_docs")
assert_eq "$row_count" "18" "foreign table should read all indexed dataset rows"

run_psql "$@" <<SQL
SELECT * FROM lance_drop_index('${DATASET}', 'bucket_idx');
SQL

remaining_count=$(scalar_sql "$@" -c "SELECT count(*) FROM lance_list_indices('${DATASET}')")
assert_eq "$remaining_count" "2" "drop index should remove one index"

echo "index management e2e passed: ${DATASET}"
