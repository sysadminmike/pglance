#!/usr/bin/env bash
set -euo pipefail

PSQL=${PSQL:-psql}
DATA_ROOT=${LANCE_ADMIN_E2E_DIR:-/tmp/lance_admin_e2e}
DATASET="${DATA_ROOT}/t_admin_files.lance"
SERVER=${LANCE_ADMIN_E2E_SERVER:-lance_admin_e2e}
SCHEMA=${LANCE_ADMIN_E2E_SCHEMA:-ladmin}
TABLE=${LANCE_ADMIN_E2E_TABLE:-t_admin_files}

psql_args=("$@")

run_psql() {
  "$PSQL" -X -v ON_ERROR_STOP=1 "${psql_args[@]}" "$@"
}

scalar_sql() {
  run_psql -Atqc "$1"
}

count_rows() {
  run_psql -At <<SQL | tail -n 1
DROP FOREIGN TABLE IF EXISTS ${SCHEMA}.${TABLE} CASCADE;
SELECT lance_import('${SERVER}', '${SCHEMA}', '${TABLE}', '${DATASET}', batch_size => NULL);
SELECT count(*) FROM ${SCHEMA}.${TABLE};
SQL
}

file_count() {
  if [[ ! -d "$DATASET" ]]; then
    echo 0
    return
  fi
  find "$DATASET" -type f | wc -l | tr -d '[:space:]'
}

show_files() {
  local title=$1
  echo
  echo ">>> ${title}"
  echo "file_count=$(file_count)"
  if [[ -d "$DATASET" ]]; then
    find "$DATASET" -type f -printf '%P\n' | sort
  else
    echo "dataset directory does not exist"
  fi
}

assert_eq() {
  local expected=$1
  local actual=$2
  local label=$3
  if [[ "$actual" != "$expected" ]]; then
    echo "FAIL: ${label}: expected ${expected}, got ${actual}" >&2
    exit 1
  fi
}

assert_lt() {
  local left=$1
  local right=$2
  local label=$3
  if (( left >= right )); then
    echo "FAIL: ${label}: expected ${left} < ${right}" >&2
    exit 1
  fi
}

assert_gt() {
  local left=$1
  local right=$2
  local label=$3
  if (( left <= right )); then
    echo "FAIL: ${label}: expected ${left} > ${right}" >&2
    exit 1
  fi
}

rm -rf "$DATA_ROOT"
mkdir -p "$DATA_ROOT"

cat <<EOF
============================================================
 Lance admin maintenance e2e
 Dataset: ${DATASET}
============================================================
EOF

run_psql <<SQL
CREATE EXTENSION IF NOT EXISTS lance;
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = '${SERVER}') THEN
    EXECUTE 'CREATE SERVER ${SERVER} FOREIGN DATA WRAPPER lance_fdw';
  END IF;
END \$\$;
CREATE SCHEMA IF NOT EXISTS ${SCHEMA};
SQL

echo
echo '>>> create dataset and generate old versions/files'
run_psql <<SQL
SELECT * FROM lance_append(
  '${DATASET}',
  'SELECT v::int4 AS id, ''row_'' || v::text AS label FROM generate_series(1, 1000) v',
  mode := 'create'
);

SELECT * FROM lance_append(
  '${DATASET}',
  'SELECT v::int4 AS id, ''row_'' || v::text AS label FROM generate_series(1001, 2000) v'
);

SELECT * FROM lance_append(
  '${DATASET}',
  'SELECT v::int4 AS id, ''row_'' || v::text AS label FROM generate_series(2001, 3000) v'
);
SQL

merge_stats=$(run_psql -At <<SQL | tail -n 1
SET lance.write_chunk_rows = 125;

SELECT rows_merged || '|' || chunk_txns || '|' || chunk_rows
  FROM lance_merge_insert(
    '${DATASET}',
    'SELECT v::int4 AS id, ''updated_'' || v::text AS label FROM generate_series(1, 500) v
     UNION ALL
     SELECT v::int4 AS id, ''new_'' || v::text AS label FROM generate_series(3001, 3500) v',
    on_columns := ARRAY['id'],
    when_matched := 'update',
    when_not_matched := 'insert'
  );
SQL
)
IFS='|' read -r merge_rows merge_chunk_txns merge_chunk_rows <<<"$merge_stats"
echo "merge_stats rows_merged=${merge_rows} chunk_txns=${merge_chunk_txns} chunk_rows=${merge_chunk_rows}"
assert_eq 1000 "$merge_rows" "merge rows_merged"
assert_eq 8 "$merge_chunk_txns" "merge chunk_txns"
assert_eq 125 "$merge_chunk_rows" "merge chunk_rows"

run_psql <<SQL
SELECT * FROM lance_delete(
  '${DATASET}',
  'id >= 2500 AND id <= 3500'
);
SQL

rows_before=$(count_rows)
assert_eq 2499 "$rows_before" "row count after updates/deletes"
show_files 'files after append/merge/delete, before optimize/vacuum'
files_before_admin=$(file_count)
assert_gt "$files_before_admin" 0 "file count before maintenance"

echo
echo '>>> run optimize'
run_psql <<SQL
SELECT fragments_removed,
       fragments_added,
       files_removed,
       files_added,
       duration_ms
  FROM lance_optimize(
    '${DATASET}',
    target_rows_per_fragment := 1000000,
    compaction_mode := 'reencode'
  );
SQL

rows_after_optimize=$(count_rows)
assert_eq "$rows_before" "$rows_after_optimize" "row count after optimize"
show_files 'files after optimize, before vacuum'
files_after_optimize=$(file_count)
assert_gt "$files_after_optimize" 0 "file count after optimize"

echo
echo '>>> run vacuum'
vacuum_stats=$(scalar_sql "SELECT bytes_removed || '|' || old_versions || '|' || data_files_removed || '|' || transaction_files_removed || '|' || index_files_removed || '|' || deletion_files_removed FROM lance_vacuum('${DATASET}', older_than_seconds := 0, error_if_tagged_old_versions := false)")
IFS='|' read -r bytes_removed old_versions data_files_removed transaction_files_removed index_files_removed deletion_files_removed <<<"$vacuum_stats"
echo "vacuum_stats bytes_removed=${bytes_removed} old_versions=${old_versions} data_files_removed=${data_files_removed} transaction_files_removed=${transaction_files_removed} index_files_removed=${index_files_removed} deletion_files_removed=${deletion_files_removed}"

rows_after_vacuum=$(count_rows)
assert_eq "$rows_before" "$rows_after_vacuum" "row count after vacuum"
show_files 'files after vacuum'
files_after_vacuum=$(file_count)

assert_gt "$bytes_removed" 0 "vacuum bytes_removed"
assert_gt "$old_versions" 0 "vacuum old_versions"
assert_lt "$files_after_vacuum" "$files_after_optimize" "filesystem files after vacuum vs after optimize"

cat <<EOF

PASS: optimize/vacuum preserved ${rows_after_vacuum} rows and reduced dataset files from ${files_after_optimize} to ${files_after_vacuum}.
EOF
