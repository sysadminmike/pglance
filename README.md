# pglance (PostgreSQL extension name: `lance`)

`pglance` is a PostgreSQL extension built with [pgrx](https://github.com/pgcentralfoundation/pgrx) that exposes [Lance](https://lancedb.github.io/lance/) datasets as PostgreSQL foreign tables via an FDW, and provides write operations (append, merge-insert, delete) to push data from PostgreSQL into Lance tables.

> Note: The Rust crate/package is currently named `pglance`, but the PostgreSQL extension name is `lance` (i.e. you run `CREATE EXTENSION lance;`).

## Features

### Read (FDW)

- Foreign Data Wrapper: `lance_fdw`
- Auto schema discovery + DDL: `lance_import(server, schema, table, uri, batch_size => NULL)`
- Namespace attach/sync: bulk-import entire Lance namespace trees
- Native-first type mapping:
  - Scalars map to native PostgreSQL scalar types where possible
  - `list<T>` maps to `T[]` when possible
  - `struct{...}` maps to PostgreSQL composite types (created automatically during import)
  - `map<...>` currently falls back to `jsonb`

### Write

- `lance_append()` — bulk-load rows from any SQL query into a Lance dataset (create / append / overwrite modes)
- `lance_merge_insert()` — upsert rows with match-on-key semantics (insert new, update existing)
- `lance_delete()` — remove rows matching a Lance predicate expression

### Maintenance

- `lance_optimize()` - compact small or delete-heavy fragments in a Lance dataset
- `lance_create_scalar_index()` / `lance_create_fts_index()` - create Lance scalar and full-text indexes
- `lance_optimize_indices()` - catch indexes up after appends, merge-inserts, deletes, and updates
- `lance_list_indices()` / `lance_index_stats()` / `lance_drop_index()` - inspect and manage Lance indexes
- `lance_vacuum()` - remove old unreferenced dataset files after writes, deletes, and optimization

## Quick Start (PostgreSQL 17)

### Prerequisites

- Rust (stable)
- `protoc` (Protocol Buffers compiler)
- `cargo-pgrx` (must match the pinned `pgrx` version)

### Build and Run Locally

If you have [`just`](https://github.com/casey/just) installed:

```bash
just run
```

This starts a pgrx-managed PostgreSQL instance and reloads the extension to match the latest code.

Without `just`:

```bash
cargo install cargo-pgrx --version=0.14.3 --locked
cargo pgrx init --pg17=download

cargo pgrx install --features pg17
cargo pgrx run --features pg17 pg17
```

## Usage

### 1) Create the extension and server

```sql
CREATE EXTENSION lance;
CREATE SERVER lance_srv FOREIGN DATA WRAPPER lance_fdw;
```

### 2) Import a Lance dataset as a foreign table

```sql
SELECT lance_import(
  'lance_srv',
  'public',
  'my_lance_table',
  '/path/to/your/lance/table',
  batch_size => NULL
);
```

`lance_import` creates (if not already present):

- The foreign table `public.my_lance_table`
- Composite types for nested `struct` fields, e.g. `public.lance_my_lance_table_meta`

### 3) Query like a regular table

```sql
SELECT count(*) FROM public.my_lance_table;

SELECT * FROM public.my_lance_table LIMIT 10;
```

For simple column aggregates, use the direct Lance helpers instead of importing
the dataset as a foreign table and running PostgreSQL aggregates over it. These
helpers scan only the requested Lance column and keep values in Arrow form until
the final scalar result is returned.

```sql
SELECT value::timestamptz AS latest_update
  FROM lance_max('s3://my-bucket/lakehouse/customers.lance', 'updated_at', 'lance_srv');

SELECT value::numeric AS smallest_balance
  FROM lance_min('/path/to/customers.lance', 'balance');
```

`lance_min()` and `lance_max()` support integer, floating point, date,
timestamp, and Arrow decimal128 columns. The result is returned as text together
with the Lance/Arrow data type and execution duration.

### 4) Attach and sync a Lance namespace

```sql
-- Plan only (no DDL).
SELECT *
  FROM lance_attach_namespace('lance_srv', dry_run => true);

-- Attach a namespace subtree into local schemas/tables.
SELECT *
  FROM lance_attach_namespace(
    'lance_srv',
    root_namespace_id => ARRAY[]::text[],
    schema_prefix => 'lance',
    batch_size => NULL,
    limit_per_list_call => 1000,
    dry_run => false
  );

-- Reconcile local objects with the remote namespace.
SELECT *
  FROM lance_sync_namespace(
    'lance_srv',
    schema_prefix => 'lance',
    drop_missing => false,
    recreate_changed => false,
    dry_run => true
  );
```

### 5) Write data into Lance datasets

#### Append rows

```sql
SELECT * FROM lance_append(
    uri           := '/path/to/dataset.lance',
    source_query  := 'SELECT id, name, value FROM my_pg_table',
    mode          := 'create',     -- 'create' | 'append' | 'overwrite'
    batch_size    := 1024,
    server_name   := NULL          -- foreign server for S3 credentials
);
--  rows_written | duration_ms
-- --------------+-------------
--          5000 |         320
```

| Mode | Behaviour |
|---|---|
| `create` | Creates a new Lance dataset. Errors if it already exists. |
| `append` | Adds rows to an existing dataset. Errors if it doesn't exist. |
| `overwrite` | Replaces all data in an existing dataset. |

#### Upsert rows (merge-insert)

```sql
SELECT * FROM lance_merge_insert(
    uri              := '/path/to/dataset.lance',
    source_query     := 'SELECT id, name, email FROM staging.customers',
    on_columns       := ARRAY['id'],
    when_matched     := 'update',    -- 'update' | 'nothing'
    when_not_matched := 'insert',    -- 'insert' | 'nothing'
    batch_size       := 1024,
    server_name      := NULL
);
--  rows_merged | rows_inserted | rows_updated | duration_ms
-- -------------+---------------+--------------+-------------
--         1500 |               |              |        2340
```

Chunk diagnostics are also returned:

- `chunk_txns`: number of per-chunk Lance merge commits executed
- `chunk_rows`: configured `lance.write_chunk_rows` value used for the merge (`0` means chunking disabled)

> Note: The Lance SDK does not currently return separate insert/update counts, so `rows_inserted` and `rows_updated` may be NULL.

#### Upsert with Arrow-ready source columns

`lance_merge_insert_with_schema()` accepts a JSONB map of source column names to
Arrow type overrides. This is useful when a source query pre-normalizes expensive
types, for example by returning timestamps as Unix microseconds (`int8`) and JSON
values as text.

```sql
SELECT * FROM lance_merge_insert_with_schema(
    uri          := '/path/to/customers.lance',
    source_query := $query$
        SELECT id,
               name,
               email,
               floor(extract(epoch from created_at) * 1000000)::int8 AS created_at,
               floor(extract(epoch from updated_at) * 1000000)::int8 AS updated_at,
               floor(extract(epoch from deleted_at) * 1000000)::int8 AS deleted_at,
               jsondata::text AS jsondata
          FROM staging.customers
         ORDER BY updated_at
    $query$,
    on_columns   := ARRAY['id'],
    column_types := '{
      "created_at": "timestamp_us_utc",
      "updated_at": "timestamp_us_utc",
      "deleted_at": "timestamp_us_utc",
      "jsondata": "utf8"
    }'::jsonb,
    when_matched     := 'update',
    when_not_matched := 'insert',
    batch_size       := 50000,
    server_name      := NULL
);
```

This still executes the source query through PostgreSQL SPI because pglance is a
PostgreSQL extension, but it avoids pgrx timestamp/JSON datum conversion for the
overridden columns. For completely bypassing SPI, use an external loader process.

#### Performance note: pglance vs an external loader

PostgreSQL still reads every source row in both designs. The performance
difference is how those rows cross into Rust and become Arrow batches.

The pglance write path runs inside a PostgreSQL backend and reads query results
through SPI. Each value is pulled from an SPI tuple and converted through pgrx's
datum APIs before it is appended to Arrow builders. This is convenient and keeps
the operation inside SQL, but it is a per-cell conversion path; wide rows with
several timestamps and JSON/text fields can require millions of datum conversions
per merge window.

An external Rust loader uses PostgreSQL's normal client protocol over a local
socket, asks SQL to pre-normalize expensive values (for example timestamps as
Unix microseconds and JSON as text), and builds large Arrow batches in a normal
Rust process before calling Lance directly. That avoids SPI and pgrx datum
conversion overhead and can be significantly faster for large backfills or
catch-up merges.

pglance cannot use that same client-protocol path for the current query result
without opening a second connection back to PostgreSQL, which would introduce a
separate session, different snapshot/transaction semantics, authentication
concerns. A faster in-extension path would need lower-level PostgreSQL tuple
decoding instead of pgrx's generic SPI value conversion, but it would still be
an in-backend execution model. For the larger merge workloads, an external loader
would be the better hot path.

#### Memory safety for large writes and merges

`lance_append` and `lance_merge_insert` stream their `source_query` through a
server-side cursor in chunks instead of buffering the entire result set in
memory. This keeps very large operations (millions of rows) from exhausting RAM
and triggering the Linux OOM killer. These GUCs control the behaviour:

| GUC | Default | Purpose |
|---|---|---|
| `lance.write_chunk_rows` | `100000` | Source rows fetched and processed per chunk. Lower it to reduce peak memory; set to `0` to process the whole source in a single pass. |
| `lance.max_write_buffer_mb` | `2048` | Hard ceiling on the in-memory Arrow buffer for a chunk. If a chunk exceeds it, the operation aborts cleanly (rolling back the transaction) instead of being OOM-killed. Set to `0` to disable the guard. |
| `lance.merge_use_index` | `true` | Allow Lance merge-insert to use scalar indexes on join keys. Set to `false` to force Lance's full-scan merge path while keeping dataset indexes in place. |

```sql
SET lance.write_chunk_rows = 50000;    -- smaller chunks = lower peak memory
SET lance.max_write_buffer_mb = 2048;  -- abort cleanly rather than OOM
SET lance.merge_use_index = false;     -- work around indexed merge issues
```

> **Atomicity caveat:** `lance_merge_insert` performs one Lance commit per chunk,
> so it is **not** a single atomic operation. If it fails partway through, chunks
> that already committed remain applied. Re-running an idempotent merge
> (`when_matched := 'update'`) is safe. If you require strict all-or-nothing
> semantics, set `lance.write_chunk_rows = 0` to force a single-pass commit
> (subject to having enough memory, bounded by `lance.max_write_buffer_mb`).

#### Delete rows by predicate

```sql
SELECT * FROM lance_delete(
    uri         := '/path/to/dataset.lance',
    predicate   := 'id > 100 AND status = ''old''',
    server_name := NULL
);
--  fragments_removed | duration_ms
-- -------------------+-------------
--                  3 |         120
```

The `predicate` uses Lance's own expression syntax (a subset of SQL). Column names must match the Lance schema.

### 6) Create and maintain Lance indexes

Indexes are Lance dataset metadata and files. They are not PostgreSQL btree/gin indexes, and PostgreSQL does not create them with `CREATE INDEX`. Use the `lance_*_index` helper functions against the Lance dataset URI.

#### Scalar indexes

```sql
SELECT * FROM lance_create_scalar_index(
    uri         := '/path/to/dataset.lance',
    column_name := 'customer_id',
    index_name  := 'customer_id_idx',
    index_type  := 'btree',       -- 'btree' | 'bitmap' | 'label_list'
    replace     := false,
    server_name := NULL
);
```

Use `btree` for high-cardinality equality/range columns such as ids and timestamps. Use `bitmap` for low-cardinality columns such as status or category. Use `label_list` for list/tag fields.

Merge-insert keys should be indexed deliberately by creating one scalar index per key column. Lance's public scalar index support is single-column; pglance does not expose a composite scalar index helper. For a multi-key merge, index each key with the type that fits that column:

```sql
SELECT * FROM lance_create_scalar_index('/data/orders.lance', 'tenant_id', 'tenant_id_idx', 'bitmap');
SELECT * FROM lance_create_scalar_index('/data/orders.lance', 'order_id',  'order_id_idx',  'btree');
```

#### Full-text search indexes

```sql
SELECT * FROM lance_create_fts_index(
    uri           := '/path/to/dataset.lance',
    column_name   := 'body',
    index_name    := 'body_fts_idx',
    replace       := false,
    tokenizer     := 'simple',
    language      := 'English',
    with_position := true
);
```

Useful tokenizer values include `simple`, `whitespace`, `raw`, and `ngram`. `with_position := true` is required for phrase-style full-text queries and increases index size.

#### Index maintenance after writes

Appending, merge-inserting, deleting, or updating a Lance dataset can leave existing indexes with unindexed rows/fragments. Lance can still use fallback scanning in some cases, but latency can increase. Run index maintenance explicitly after write batches:

```sql
-- Append delta index segments for all indexes.
SELECT * FROM lance_optimize_indices('/path/to/dataset.lance');

-- Maintain only selected indexes.
SELECT * FROM lance_optimize_indices(
    uri         := '/path/to/dataset.lance',
    index_names := ARRAY['customer_id_idx', 'body_fts_idx'],
    mode        := 'append'
);

-- Merge recent index deltas.
SELECT * FROM lance_optimize_indices(
    uri                  := '/path/to/dataset.lance',
    index_names          := ARRAY['body_fts_idx'],
    mode                 := 'merge',
    num_indices_to_merge := 4
);
```

Inspect index health with `lance_index_stats`. The key field to monitor is `num_unindexed_rows`; a fully caught-up index reports `0`.

```sql
SELECT stats->>'index_type' AS index_type,
       (stats->>'num_indexed_rows')::bigint AS indexed_rows,
       (stats->>'num_unindexed_rows')::bigint AS unindexed_rows
  FROM lance_index_stats('/path/to/dataset.lance', 'customer_id_idx');
```

List or drop indexes when needed:

```sql
SELECT index_name, index_type, column_names, rows_indexed
  FROM lance_list_indices('/path/to/dataset.lance');

SELECT * FROM lance_drop_index('/path/to/dataset.lance', 'customer_id_idx');
```

Vector index creation is intentionally not wrapped yet. pglance should expose vector indexes together with a PostgreSQL-facing vector search/query API so the index surface and query surface arrive as one coherent feature.

#### S3 credentials via foreign server

```sql
-- Reuse credentials from a lance_fdw foreign server
SELECT * FROM lance_append(
    's3://my-bucket/lakehouse/events.lance',
    'SELECT * FROM staging.events',
    server_name := 'my_lance_server'
);
```

### 7) Maintain Lance datasets

Lance datasets accumulate fragments, old versions, transaction files, index files, and replaced data files as you append, merge-insert, delete, and overwrite data. Use `lance_optimize_indices()` to keep indexes current, `lance_optimize()` to compact the active dataset state, then `lance_vacuum()` to remove old unreferenced files from storage.

`lance_optimize()` and `lance_optimize_indices()` do different work: `lance_optimize()` compacts dataset fragments, while `lance_optimize_indices()` builds or merges index delta segments.

#### Optimize fragments

```sql
SELECT * FROM lance_optimize(
    uri                       := '/path/to/dataset.lance',
    target_rows_per_fragment  := 1000000,
    max_rows_per_group        := NULL,
    max_bytes_per_file        := NULL,
    materialize_deletions     := true,
    materialize_deletions_threshold := 0.1,
    num_threads               := NULL,
    batch_size                := NULL,
    defer_index_remap         := false,
    compaction_mode           := 'reencode',
    max_source_fragments      := NULL,
    server_name               := NULL
);
--  fragments_removed | fragments_added | files_removed | files_added | duration_ms
-- -------------------+-----------------+---------------+-------------+-------------
--                  6 |               1 |             8 |           2 |        1850
```

Useful options:

| Option | Default | Meaning |
|---|---:|---|
| `target_rows_per_fragment` | `NULL` | Target row count for compacted fragments. |
| `max_rows_per_group` | `NULL` | Maximum rows per row group when writing compacted data. |
| `max_bytes_per_file` | `NULL` | Approximate maximum compacted file size. |
| `materialize_deletions` | `true` | Rewrite fragments with deletion vectors into clean data files. |
| `materialize_deletions_threshold` | `0.1` | Deletion fraction threshold for materializing deletions. |
| `num_threads` | `NULL` | Worker thread count for compaction. |
| `batch_size` | `NULL` | Read/write batch size used by the optimizer. |
| `defer_index_remap` | `false` | Defer index remapping during compaction. |
| `compaction_mode` | `NULL` | One of `reencode`, `try_binary_copy`, or `force_binary_copy`. |
| `max_source_fragments` | `NULL` | Limit how many source fragments are compacted in one run. |
| `server_name` | `NULL` | Foreign server to reuse for object-store credentials. |

`lance_optimize()` preserves the current logical rows, but it can create a new dataset version and leave older physical files on disk until vacuum removes them.

#### Vacuum old files

```sql
SELECT * FROM lance_vacuum(
    uri                          := '/path/to/dataset.lance',
    older_than_seconds           := 604800,
    before_version               := NULL,
    delete_unverified            := false,
    error_if_tagged_old_versions := false,
    clean_referenced_branches    := false,
    delete_rate_limit            := NULL,
    server_name                  := NULL
);
--  bytes_removed | old_versions | data_files_removed | transaction_files_removed | index_files_removed | deletion_files_removed | duration_ms
-- ---------------+--------------+--------------------+---------------------------+---------------------+------------------------+-------------
--       10485760 |            4 |                 12 |                         3 |                   0 |                      2 |         420
```

Useful options:

| Option | Default | Meaning |
|---|---:|---|
| `older_than_seconds` | `604800` | Remove files older than this many seconds. The default is seven days. Use `0` for aggressive cleanup in tests or controlled maintenance windows. |
| `before_version` | `NULL` | Remove files associated with versions before this dataset version. |
| `delete_unverified` | `false` | Delete files that cannot be verified as safe by the normal cleanup checks. |
| `error_if_tagged_old_versions` | `false` | Error instead of cleaning up when old tagged versions would be affected. |
| `clean_referenced_branches` | `false` | Also clean files referenced by old branch history. |
| `delete_rate_limit` | `NULL` | Limit delete throughput during cleanup. |
| `server_name` | `NULL` | Foreign server to reuse for object-store credentials. |

For routine maintenance, run optimize first and vacuum after the retention window you want to keep:

```sql
SELECT * FROM lance_optimize_indices('/path/to/dataset.lance');
SELECT * FROM lance_optimize('/path/to/dataset.lance');
SELECT * FROM lance_vacuum('/path/to/dataset.lance');
```

For test environments where you want to prove files are removed immediately:

```sql
SELECT * FROM lance_optimize('/tmp/example.lance', target_rows_per_fragment := 1000000);
SELECT * FROM lance_vacuum('/tmp/example.lance', older_than_seconds := 0);
```

## Type Mapping

### Read path (Arrow → PostgreSQL)

| Arrow/Lance Type | PostgreSQL Type |
|------------------|-----------------|
| Boolean          | boolean         |
| Int8/UInt8       | int2            |
| Int16/UInt16     | int2            |
| Int32/UInt32     | int4            |
| Int64/UInt64     | int8            |
| Float16/Float32  | float4          |
| Float64          | float8          |
| Utf8/LargeUtf8   | text            |
| Binary           | bytea           |
| Date32/Date64    | date            |
| Timestamp        | timestamp / timestamptz |
| List             | array types     |
| Struct           | composite types |
| Map              | jsonb           |

### Write path (PostgreSQL → Arrow)

| PostgreSQL Type | Arrow Type |
|---|---|
| bool | Boolean |
| int2 | Int16 |
| int4 | Int32 |
| int8 | Int64 |
| float4 | Float32 |
| float8 | Float64 |
| numeric | Utf8 (string representation) |
| text / varchar | Utf8 |
| bytea | Binary |
| date | Date32 |
| timestamp | Timestamp(Microsecond, None) |
| timestamptz | Timestamp(Microsecond, UTC) |
| jsonb | Utf8 (serialised JSON) |

## Development

Recommended workflow:

```bash
just ci       # format check + clippy + tests
just run      # build, install, start PG, reload extension
just test     # run unit + SLT integration tests
```

### Running tests

Tests use the [sqllogictest](https://github.com/risinglightdb/sqllogictest-rs) framework. Test files live in `tests/sql/*.slt`.

```bash
just test
# or explicitly:
cargo test --no-default-features --features pg17
```

### End-to-end test with DuckDB verification

A standalone SQL script exercises every write operation and cross-verifies each mutation by reading the Lance files independently through DuckDB's `lance_scan`.

**Prerequisites:** PostgreSQL with both the `lance` and `pg_duckdb` extensions installed. `pg_duckdb` must be able to install/load DuckDB's official `lance` extension; this requires a DuckDB version that publishes the `lance` extension for your platform.

```bash
psql -v lance_dir='/tmp/lance_e2e_test' -f tests/e2e_lance_duckdb.sql
```

Notes:

- `just reload-ext` drops and recreates the extension, so it will also drop dependent objects (e.g. foreign servers / foreign tables) and you may need to recreate them.

### End-to-end optimize/vacuum filesystem test

A standalone bash script creates a Lance dataset through psql, performs append, merge-insert, and delete operations, lists the dataset files on disk, then runs `lance_optimize` and `lance_vacuum`. It fails unless vacuum reports removed bytes/versions and the actual file count under the `.lance` directory drops after cleanup.

```bash
tests/e2e_admin_maintenance.sh
# or
just e2e-admin
```

You can pass normal psql connection flags through the script, and override the dataset directory with `LANCE_ADMIN_E2E_DIR`:

```bash
LANCE_ADMIN_E2E_DIR=/tmp/lance_admin_e2e tests/e2e_admin_maintenance.sh -d pglance
```

### End-to-end index management test

A standalone bash script creates scalar and FTS indexes, appends additional rows, checks `num_unindexed_rows`, runs `lance_optimize_indices`, and verifies the indexes are caught up.

```bash
tests/e2e_index_management.sh
# or
just e2e-index
```

You can override the dataset directory with `LANCE_INDEX_E2E_DIR`:

```bash
LANCE_INDEX_E2E_DIR=/tmp/lance_index_e2e tests/e2e_index_management.sh -d pglance
```

### Merge benchmark with and without indexes

The merge benchmark script compares no-index merge-insert, single-key merge with a scalar index, and multi-key merge with one scalar index per key field.

```bash
tests/benchmark_merge_index.sh -d pglance
```

Tune row counts with environment variables:

```bash
BENCH_INITIAL_ROWS=100000 BENCH_MERGE_ROWS=25000 tests/benchmark_merge_index.sh -d pglance
```
