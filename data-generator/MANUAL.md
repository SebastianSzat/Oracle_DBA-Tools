# PKG_DATGEN_TOY — Manual and Reference

## Contents

1. [Deployment](#deployment)
2. [How it works](#how-it-works)
3. [Command table: T_DATGEN_TOY_CMD](#command-table-t_datgen_toy_cmd)
4. [Column overrides: T_DATGEN_TOY_COLS](#column-overrides-t_datgen_toy_cols)
5. [Public API](#public-api)
6. [Supported column types](#supported-column-types)
7. [Primary key auto-detection](#primary-key-auto-detection)
8. [LOB handling](#lob-handling)
9. [Direct-path inserts (APPEND hint)](#direct-path-inserts-append-hint)
10. [Logging](#logging)
11. [Worked examples](#worked-examples)
12. [Known limitations](#known-limitations)

---

## Deployment

Run the scripts in numbered order against the target schema:

```
01_cleanup.sql              -- optional: wipe any existing DATGEN_TOY objects
02_T_DATGEN_TOY_CMD.sql     -- command/configuration table
03_T_DATGEN_TOY_COLS.sql    -- column override table
04_T_DATGEN_TOY_LOG.sql     -- log table + sequence + indexes
05_PKG_DATGEN_TOY.sql       -- package specification and body
```

`06_DATGEN_TOY_test_structure.sql` is optional — it creates two test tables that cover every supported column type and can be used to validate a fresh installation.

`01_cleanup.sql` is safe to run repeatedly. It drops all objects whose names match `%DATGEN_TOY%` in dependency-safe order (triggers → package → tables → sequences). The test tables from `06_` match this pattern and are removed by the same script.

---

## How it works

A single call to `p_run` or `f_fill_tables`:

1. Reads `T_DATGEN_TOY_CMD` for all rows where `IF_GENERATE = 1`, ordered by `LVL` ascending.
2. For each target table:
   - Counts existing rows; calculates how many to insert (capped at `MAX_ROWS`).
   - Queries the data dictionary (`USER_TAB_COLS`, `USER_CONSTRAINTS`) once and caches the results — PK columns, COLS-configured columns, and regular columns are each collected into an in-memory array.
   - Runs an inner loop inserting one row per iteration, building the `INSERT` statement dynamically from the cached column metadata.
   - Commits every `COMMIT_EVERY` rows (or after every row when the APPEND hint is active).
   - Sets `IF_GENERATE = 0` on success so the table is not re-processed unless explicitly re-enabled.
3. Logs every step to `T_DATGEN_TOY_LOG`, grouped by `RUN_ID`.

Each target table runs inside its own `BEGIN … EXCEPTION … END` block. A failure on one table is logged and skipped; the run continues to the next table.

---

## Command table: T_DATGEN_TOY_CMD

One row per target table. Controls everything about how that table is filled.

| Column | Type | Default | Description |
|--------|------|---------|-------------|
| `TABLE_NAME` | VARCHAR2(64) | — | Target table name. Must match `USER_TABLES` exactly (uppercase). Primary key. |
| `LVL` | NUMBER(10) | — | Processing order. Lower values run first. Set parent tables lower than the child tables that reference them. |
| `ROW_AMOUNT` | NUMBER(10) | — | Rows to insert per run. Automatically capped if the total would exceed `MAX_ROWS`. |
| `MAX_ROWS` | NUMBER(10) | — | Hard ceiling. Processing is skipped entirely if the current row count is already at or above this value. |
| `IF_GENERATE` | NUMBER(1) | 1 | Run flag. `1` = process on next run. `0` = skip. Set to `0` automatically after a successful run. Reset with `p_enable` or `p_enable_all`. |
| `COMMIT_EVERY` | NUMBER(10) | 1000 | Rows between intermediate commits. Limits UNDO tablespace pressure. Use ~100 for tables with LOB columns. |
| `LOB_SIZE_KB` | NUMBER(10) | 10 | Approximate size in KB for each generated BLOB or CLOB value. Has no effect on tables with no LOB columns. |
| `DATE_FROM` | DATE | 2000-01-01 | Earliest date for randomly generated DATE and TIMESTAMP values in this table. |
| `DATE_TO` | DATE | 2030-12-31 | Latest date for randomly generated DATE and TIMESTAMP values in this table. |
| `USE_APPEND` | NUMBER(1) | 0 | `1` = use the `/*+ APPEND */` direct-path insert hint (faster for large inserts). Automatically overridden to `0` for tables with LOB columns. See [Direct-path inserts](#direct-path-inserts-append-hint). |

The `MODIFIED` column is maintained automatically by a trigger and does not need to be populated manually.

---

## Column overrides: T_DATGEN_TOY_COLS

One row per column that needs a custom value expression. Covers foreign keys, unique constraints, custom primary keys, and LOBs with specific content requirements.

Columns **not** listed here are handled automatically by the package based on their data type.

| Column | Type | Description |
|--------|------|-------------|
| `TABLE_NAME` | VARCHAR2(64) | Target table. Must have a matching entry in `T_DATGEN_TOY_CMD`. Part of the composite primary key. |
| `COLUMN_NAME` | VARCHAR2(64) | Column within the target table. Part of the composite primary key. |
| `FILLING_STRING` | VARCHAR2(4000) | A complete `SELECT` statement (including `FROM` clause) that returns exactly one value. Executed at row-generation time via `EXECUTE IMMEDIATE … INTO`. The result is embedded directly into the `VALUES` clause — see quoting rules below. |
| `CONSTRAINT_TYPE` | VARCHAR2(16) | Category: `PK`, `FK`, `UI` (unique index), or `LOB`. Informational — used for documentation and filtering. |

### Quoting rules for FILLING_STRING

The value returned by the `SELECT` is inserted **as-is** into the dynamic SQL `VALUES` clause. This means:

- **NUMBER result** — no quoting needed.
  ```sql
  'SELECT seq_orders.NEXTVAL FROM DUAL'
  ```
- **VARCHAR2 result** — the expression must include its own surrounding single quotes. Use `CHR(39)` to avoid nesting complexity.
  ```sql
  'SELECT CHR(39)||SYS_GUID()||CHR(39) FROM DUAL'
  ```
- **FK column (NUMBER)** — select a random existing parent key.
  ```sql
  'SELECT ID FROM PARENT_TABLE ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROW ONLY'
  ```
- **FK column (VARCHAR2)** — same, with quoting.
  ```sql
  'SELECT CHR(39)||CODE||CHR(39) FROM REF_TABLE ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROW ONLY'
  ```

---

## Public API

### `p_run`

```sql
BEGIN PKG_DATGEN_TOY.p_run; END;
```

Processes all tables with `IF_GENERATE = 1`, in `LVL` order. Prints the total row count to `DBMS_OUTPUT`.

---

### `p_run(in_table_name)`

```sql
BEGIN PKG_DATGEN_TOY.p_run('MY_TABLE'); END;
```

Processes a single named table (the flag still controls whether the table is processed — the argument acts as an additional filter on the cursor, so `IF_GENERATE` must still be `1`).

---

### `f_fill_tables(in_table_name)`

```sql
DECLARE
    v_rows NUMBER;
BEGIN
    v_rows := PKG_DATGEN_TOY.f_fill_tables('MY_TABLE');
    DBMS_OUTPUT.PUT_LINE('Rows inserted: ' || v_rows);
END;
```

Same as `p_run` but returns the total rows inserted as a `NUMBER`. Pass `NULL` (or omit the argument) to process all enabled tables.

---

### `p_enable(in_table_name)`

```sql
BEGIN PKG_DATGEN_TOY.p_enable('MY_TABLE'); END;
```

Sets `IF_GENERATE = 1` for a single table. Use this to re-enable a table after a successful run has set it to `0`.

---

### `p_enable_all`

```sql
BEGIN PKG_DATGEN_TOY.p_enable_all; END;
```

Sets `IF_GENERATE = 1` for every row in `T_DATGEN_TOY_CMD`.

---

### `p_reset(in_table_name)`

Alias for `p_enable`. Provided for readability — "reset for another run" reads more naturally than "enable" in some contexts.

---

### `p_analyze(in_table_name)`

```sql
BEGIN PKG_DATGEN_TOY.p_analyze('MY_TABLE'); END;
```

Dry-run pre-analysis. Logs what `f_fill_tables` would do — PK strategy for each column, COLS overrides, regular columns with their auto-detection path, warnings for unsupported NOT NULL types — without inserting any data. Prints the `RUN_ID` to `DBMS_OUTPUT` so you can query the log immediately.

Use this to validate configuration before committing to a real run:

```sql
BEGIN PKG_DATGEN_TOY.p_analyze('MY_TABLE'); END;
/
SELECT STATUS_INFO FROM T_DATGEN_TOY_LOG WHERE RUN_ID = <id> ORDER BY ID;
```

---

### Random helper functions

These are also available for direct use outside the package:

| Function | Returns | Notes |
|----------|---------|-------|
| `f_random_string(i_length)` | VARCHAR2 | Random uppercase alphanumeric. Actual length is random between 1 and `i_length` (max 4000). |
| `f_random_number(i_precision, i_scale)` | NUMBER | Random number with the given precision and scale. |
| `f_random_date(i_from, i_to)` | DATE | Uniformly distributed between the two dates (both inclusive). |
| `f_random_timestamp(i_from, i_to)` | TIMESTAMP | Uniformly distributed, sub-second precision. |
| `f_random_raw(i_length)` | RAW | Random bytes up to 2000. Uses `SYS_GUID()` internally. |
| `f_random_binary_float` | BINARY_FLOAT | Random value in [0, 1). |
| `f_random_binary_double` | BINARY_DOUBLE | Random value in [0, 1). |
| `f_random_clob(i_times)` | CLOB | Temporary CLOB of approximately `(i_times × 2000)` characters. Caller must free with `DBMS_LOB.FREETEMPORARY`. |
| `f_random_blob(i_times)` | BLOB | Temporary BLOB of approximately `(i_times × 2000)` bytes. Caller must free with `DBMS_LOB.FREETEMPORARY`. |

---

## Supported column types

The following data types are handled automatically. Columns with any other type receive `NULL` (a warning is logged; a `NOT NULL` unsupported column will cause the insert to fail with ORA-01400).

| Oracle data type | Auto-detection strategy |
|-----------------|------------------------|
| `VARCHAR2`, `CHAR` | Random alphanumeric string, length up to the column's declared character limit. |
| `NVARCHAR2`, `NCHAR` | Same as above, using the column's character length (not byte length). |
| `NUMBER` | Random number within the column's declared precision and scale. |
| `INTEGER`, `INT`, `SMALLINT` | Stored as `NUMBER` in the data dictionary; handled by the same NUMBER path. |
| `FLOAT` | Random number; uses the FLOAT branch (same generation logic as NUMBER). |
| `BINARY_FLOAT` | Random IEEE 754 single-precision value via `TO_BINARY_FLOAT`. |
| `BINARY_DOUBLE` | Random IEEE 754 double-precision value via `TO_BINARY_DOUBLE`. |
| `DATE` | Random date between `DATE_FROM` and `DATE_TO` from the CMD row. |
| `TIMESTAMP` | Random timestamp between `DATE_FROM` and `DATE_TO`, sub-second precision. |
| `TIMESTAMP WITH TIME ZONE` | Same as TIMESTAMP; Oracle applies the session timezone on insert. |
| `TIMESTAMP WITH LOCAL TIME ZONE` | Same as TIMESTAMP; Oracle normalises to the database timezone on insert. |
| `RAW` | Random bytes up to the column's declared length, generated via `SYS_GUID()`. |
| `BLOB` | Random binary content of approximately `LOB_SIZE_KB` kilobytes. |
| `CLOB` | Random alphanumeric text of approximately `LOB_SIZE_KB` kilobytes. |
| `NCLOB` | Same as CLOB. |

Virtual and hidden columns are automatically excluded from all insert operations.

---

## Primary key auto-detection

PK columns not listed in `T_DATGEN_TOY_COLS` are handled automatically:

| PK column type | Strategy |
|---------------|---------|
| `NUMBER` | `MAX(column) + row_index`. Queried once before the insert loop; collision-free within and across runs. |
| `VARCHAR2` / `CHAR` / `NVARCHAR2` / `NCHAR` with length ≥ 32 | `RAWTOHEX(SYS_GUID())` — 32-character uppercase hex string, globally unique. |
| `VARCHAR2` / `CHAR` / `NVARCHAR2` / `NCHAR` with length < 32 | Sequential numeric string: `COUNT(*) + row_index` as a string. A warning is logged recommending a COLS override with a tailored expression. |
| `RAW` | `SYS_GUID()` as raw bytes — 16 bytes, globally unique. |
| Any other type | `NULL` is inserted and a warning is logged. Will fail with ORA-01400 if the column is `NOT NULL`. Add a COLS entry with a custom expression to handle these. |

Composite PKs are supported. Each column in the PK is handled independently by its data type. A composite-PK notice is logged before the insert loop.

To override auto-detection for any PK column, add a row to `T_DATGEN_TOY_COLS` with `CONSTRAINT_TYPE = 'PK'`.

---

## LOB handling

BLOB, CLOB, and NCLOB columns cannot be embedded as literals in dynamic SQL. The package uses a two-step approach for each LOB row:

1. `INSERT` the row with `EMPTY_BLOB()` or `EMPTY_CLOB()` placeholders and capture the new row's `ROWID` via a `RETURNING` clause.
2. For each LOB column, `UPDATE` it with a bind variable (`:1`) containing the generated content. The temporary LOB is freed immediately after the `UPDATE` to prevent temp-LOB cache growth.

Important notes:
- The `/*+ APPEND */` hint is **automatically disabled** for tables with LOB columns. Direct-path inserts are incompatible with the `RETURNING ROWID` pattern.
- For LOB-heavy tables, use a lower `COMMIT_EVERY` (50–100 is recommended) to limit UNDO tablespace growth per transaction.
- `LOB_SIZE_KB` controls the approximate size of each generated value. Each generation cycle appends ~2000 characters/bytes, so the actual count is `CEIL(LOB_SIZE_KB * 1024 / 2000)` cycles.

---

## Direct-path inserts (APPEND hint)

Setting `USE_APPEND = 1` in `T_DATGEN_TOY_CMD` adds the `/*+ APPEND */` hint to all `INSERT` statements for that table. Direct-path inserts write directly to new extents, bypassing the buffer cache, and are significantly faster for large `ROW_AMOUNT` values.

**Constraints:**

- Oracle requires a `COMMIT` after every direct-path insert before the same table can be accessed again in the same transaction. The package handles this automatically: when APPEND is active, every row is committed individually, regardless of the `COMMIT_EVERY` setting.
- APPEND is automatically disabled for tables with LOB columns (see above).
- APPEND acquires an exclusive lock on the table segment for the duration of the transaction. In single-session testing this is not a concern; in a multi-session environment, plan accordingly.

---

## Logging

Every step taken by the package is written to `T_DATGEN_TOY_LOG`. All entries from a single run share the same `RUN_ID`, making it straightforward to trace a complete run.

```sql
-- Retrieve all entries for the most recent run
SELECT ID, TABLE_NAME, STEP, STATUS_INFO, MODIFIED
FROM   T_DATGEN_TOY_LOG
WHERE  RUN_ID = (SELECT MAX(RUN_ID) FROM T_DATGEN_TOY_LOG)
ORDER  BY ID;

-- Retrieve all entries for a specific run
SELECT * FROM T_DATGEN_TOY_LOG WHERE RUN_ID = <value> ORDER BY ID;

-- Show all errors across all runs
SELECT * FROM T_DATGEN_TOY_LOG WHERE STATUS_INFO LIKE 'ERROR%' ORDER BY ID DESC;

-- Show warnings from the latest run
SELECT * FROM T_DATGEN_TOY_LOG
WHERE  RUN_ID = (SELECT MAX(RUN_ID) FROM T_DATGEN_TOY_LOG)
  AND  STATUS_INFO LIKE 'WARN%'
ORDER  BY ID;
```

Log entries use the following prefixes in `STATUS_INFO`:

| Prefix | Meaning |
|--------|---------|
| (none) | Normal progress: run started, rows inserted, run complete. |
| `INFO:` | Non-critical notice: composite PK detected, APPEND overridden, short-column PK fallback used. |
| `WARN:` | Potential problem: unsupported column type, missing table in USER_TABLES. |
| `ERROR:` | Per-table failure with rollback. The run continues to the next table. |
| `FATAL:` | Failure outside the per-table block. The run is aborted. |

Each `f_log` call issues its own `COMMIT`, so log entries survive even if the calling transaction is later rolled back.

Indexes on `TABLE_NAME`, `MODIFIED`, and `RUN_ID` support the three most common query patterns.

---

## Worked examples

### Example 1: Single table, no foreign keys

```sql
-- Register the table
INSERT INTO T_DATGEN_TOY_CMD (TABLE_NAME, LVL, ROW_AMOUNT, MAX_ROWS)
VALUES ('ORDERS', 10, 5000, 100000);
COMMIT;

-- Run
BEGIN PKG_DATGEN_TOY.p_run; END;
/

-- Re-enable and run again
BEGIN PKG_DATGEN_TOY.p_enable('ORDERS'); END;
/
BEGIN PKG_DATGEN_TOY.p_run; END;
/
```

### Example 2: Parent-child with a foreign key

```sql
-- Parent table (LVL=10, populated first)
INSERT INTO T_DATGEN_TOY_CMD (TABLE_NAME, LVL, ROW_AMOUNT, MAX_ROWS)
VALUES ('CUSTOMERS', 10, 1000, 10000);

-- Child table (LVL=20, populated after parent has rows)
INSERT INTO T_DATGEN_TOY_CMD (TABLE_NAME, LVL, ROW_AMOUNT, MAX_ROWS, COMMIT_EVERY)
VALUES ('ORDERS', 20, 5000, 50000, 500);

-- FK override: pick a random existing customer
INSERT INTO T_DATGEN_TOY_COLS (TABLE_NAME, COLUMN_NAME, FILLING_STRING, CONSTRAINT_TYPE)
VALUES ('ORDERS', 'CUSTOMER_ID',
        'SELECT CUSTOMER_ID FROM CUSTOMERS ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROW ONLY',
        'FK');

COMMIT;

BEGIN PKG_DATGEN_TOY.p_run; END;
/
```

### Example 3: Table with LOB columns

```sql
INSERT INTO T_DATGEN_TOY_CMD (
    TABLE_NAME, LVL, ROW_AMOUNT, MAX_ROWS, COMMIT_EVERY, LOB_SIZE_KB
) VALUES (
    'DOCUMENTS', 10, 500, 5000, 50, 100
);
COMMIT;

BEGIN PKG_DATGEN_TOY.p_run; END;
/
```

`LOB_SIZE_KB = 100` generates approximately 100 KB per BLOB/CLOB value. `COMMIT_EVERY = 50` keeps UNDO growth manageable.

### Example 4: Large table with direct-path inserts

```sql
INSERT INTO T_DATGEN_TOY_CMD (
    TABLE_NAME, LVL, ROW_AMOUNT, MAX_ROWS, USE_APPEND
) VALUES (
    'EVENTS', 10, 100000, 1000000, 1
);
COMMIT;

BEGIN PKG_DATGEN_TOY.p_run; END;
/
```

`USE_APPEND = 1` is only effective on tables with no LOB columns. Each row is committed individually when APPEND is active.

### Example 5: Validate configuration before running

```sql
BEGIN PKG_DATGEN_TOY.p_analyze('ORDERS'); END;
/

-- Check the output
SELECT STATUS_INFO
FROM   T_DATGEN_TOY_LOG
WHERE  RUN_ID = (SELECT MAX(RUN_ID) FROM T_DATGEN_TOY_LOG)
ORDER  BY ID;
```

### Example 6: Re-enable all tables after a complete run

```sql
BEGIN PKG_DATGEN_TOY.p_enable_all; END;
/
BEGIN PKG_DATGEN_TOY.p_run; END;
/
```

---

## Known limitations

- **Unsupported types receive NULL.** `XMLTYPE`, `INTERVAL`, `SDO_GEOMETRY`, and any other type not listed in the [Supported column types](#supported-column-types) table will be inserted as `NULL`. If the column is `NOT NULL`, the insert will fail with ORA-01400. Use a `T_DATGEN_TOY_COLS` entry with a custom `FILLING_STRING` to handle these columns.

- **FILLING_STRING result must fit in VARCHAR2(4000).** The value returned by a COLS expression is captured into a `VARCHAR2(4000)` before being embedded in the dynamic SQL. Expressions returning longer values will be truncated or will error.

- **Partial runs after tablespace errors.** Intermediate commits mean that if the tablespace fills up mid-run, already-committed rows remain in the table. The package will log the error and skip the remaining rows for that table. `IF_GENERATE` is not set to `0` for a failed table, so the next run will attempt to fill it again, starting from the current row count.

- **No uniqueness guarantee for random strings.** `f_random_string` generates random alphanumeric content. For columns with a unique constraint that are not registered in `T_DATGEN_TOY_COLS`, duplicate values may occasionally occur and cause insert failures. Register these columns in `T_DATGEN_TOY_COLS` with `CONSTRAINT_TYPE = 'UI'` and supply a guaranteed-unique expression such as `SYS_GUID()`.

- **FK expressions rely on the parent table already having rows.** If a COLS expression selects from a parent table that is empty at insert time, `EXECUTE IMMEDIATE … INTO` will raise ORA-01403 (no data found). Ensure parent tables are at a lower `LVL` than their dependants and that they are populated in the same run.

- **All objects created in the current schema.** The package queries `USER_TAB_COLS`, `USER_CONSTRAINTS`, and `USER_TABLES`. It cannot generate data for tables in other schemas.
