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
- `lance_vacuum()` - remove old unreferenced dataset files after writes, deletes, and optimization

## Quick Start (PostgreSQL 16)

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

> Note: The Lance SDK does not currently return separate insert/update counts, so `rows_inserted` and `rows_updated` may be NULL.

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

#### S3 credentials via foreign server

```sql
-- Reuse credentials from a lance_fdw foreign server
SELECT * FROM lance_append(
    's3://my-bucket/lakehouse/events.lance',
    'SELECT * FROM staging.events',
    server_name := 'my_lance_server'
);
```

### 6) Maintain Lance datasets

Lance datasets accumulate fragments, old versions, transaction files, and replaced data files as you append, merge-insert, delete, and overwrite data. Use `lance_optimize()` to compact the active dataset state, then `lance_vacuum()` to remove old unreferenced files from storage.

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
