# ANON_TOY — Reference Manual

## Table of contents

1. [Architecture overview](#1-architecture-overview)
2. [Schema and privileges](#2-schema-and-privileges)
3. [Configuration tables](#3-configuration-tables)
   - [T_ANON_TOY_CMD](#t_anon_toy_cmd)
   - [T_ANON_TOY_COLS](#t_anon_toy_cols)
   - [T_ANON_TOY_MAP](#t_anon_toy_map)
   - [T_ANON_TOY_LOG](#t_anon_toy_log)
   - [T_ANON_TOY_POOL](#t_anon_toy_pool)
4. [Package API](#4-package-api)
5. [Anonymization methods](#5-anonymization-methods)
6. [FK cascade strategy](#6-fk-cascade-strategy)
7. [Logging](#7-logging)
8. [Typical workflow](#8-typical-workflow)
9. [Known limitations](#9-known-limitations)

---

## 1. Architecture overview

```
T_ANON_TOY_CMD          ← which tables to process, in what order
T_ANON_TOY_COLS         ← which columns to anonymize and how
       │
       ▼
PKG_ANON_TOY.p_build_map
       │   reads distinct values from target tables
       │   generates replacements
       ▼
T_ANON_TOY_MAP          ← persists every old_value → new_value translation
       │
       ▼
PKG_ANON_TOY.p_apply_map
       │   disables FK constraints
       │   UPDATE target tables using MAP entries
       │   re-enables FK constraints
       ▼
T_ANON_TOY_LOG          ← full audit trail of every step
T_ANON_TOY_POOL         ← pool of realistic fake values (POOL method)
```

The two-phase design (build map, then apply) means:

- The same old value always produces the same new value, both within a run and
  across subsequent runs, because existing MAP entries are reused rather than
  replaced.
- You can rebuild the map without touching target tables, or re-apply an
  existing map without rebuilding it.
- Referential integrity is maintained: child tables that reference a parent PK
  look up the same MAP entry as the parent and receive the identical new value.

---

## 2. Schema and privileges

The tool lives in its own Oracle schema (`ANON_TOY`) created by
`02_create_schema.sql` (must be run as SYSDBA). The following privileges are
granted to the schema:

| Privilege | Purpose |
|-----------|---------|
| `CREATE SESSION/TABLE/SEQUENCE/PROCEDURE/TRIGGER/VIEW` | Schema setup |
| `SELECT ANY TABLE` | Read distinct values from any target schema |
| `UPDATE ANY TABLE` | Write anonymized values back to any target schema |
| `ALTER ANY TABLE` | Disable / enable FK constraints on any schema |
| `SELECT ANY DICTIONARY` | Discover FK constraints via `DBA_CONSTRAINTS` / `DBA_TAB_COLUMNS` |
| `EXECUTE ON DBMS_RANDOM` | Value generation |
| `EXECUTE ON DBMS_ASSERT` | SQL-injection-safe identifier quoting |
| `EXECUTE ON DBMS_UTILITY` | Error backtrace in log messages |
| `EXECUTE ON DBMS_OUTPUT` | Optional debug output |

The package is compiled with `AUTHID DEFINER` (the default). No `EXECUTE`
privilege on `PKG_ANON_TOY` is granted to any other user or `PUBLIC`, ensuring
the package cannot be called accidentally from another session.

---

## 3. Configuration tables

### T_ANON_TOY_CMD

One row per target table. Controls processing order and run flags.

| Column | Type | Notes |
|--------|------|-------|
| `TABLE_SCHEMA` | VARCHAR2(64) | Schema owner, uppercase. Must match `DBA_TABLES.OWNER`. |
| `TABLE_NAME` | VARCHAR2(64) | Table name, uppercase. Must match `DBA_TABLES.TABLE_NAME`. |
| `LVL` | NUMBER(10) | Processing order. Lower values run first. Set parent tables to a lower LVL than their child tables so parent mappings exist before FK columns are resolved. |
| `IF_ANONYMIZE` | NUMBER(1) | 1 = process on next run; 0 = skip. Automatically set to 0 after a successful run. Reset with `p_enable` or `p_enable_all`. |
| `COMMIT_EVERY` | NUMBER(10) | Number of MAP entries inserted between intermediate commits during `p_build_map` (counted per column). Has **no effect** on `p_apply_map`, which commits once per column regardless. Use to control UNDO pressure during the build phase. Default 1000. |
| `MODIFIED` | DATE | Maintained by trigger. |

### T_ANON_TOY_COLS

One row per column to anonymize.

| Column | Type | Notes |
|--------|------|-------|
| `TABLE_SCHEMA` | VARCHAR2(64) | Must match a row in `T_ANON_TOY_CMD`. |
| `TABLE_NAME` | VARCHAR2(64) | Must match a row in `T_ANON_TOY_CMD`. |
| `COLUMN_NAME` | VARCHAR2(64) | Column to anonymize. |
| `ANON_METHOD` | VARCHAR2(16) | One of: `POOL`, `SCRAMBLE`, `SHIFT`, `FORMAT`, `NULL_VAL`, `CUSTOM`, `KEEP`. |
| `DATA_CATEGORY` | VARCHAR2(64) | Pool category for `POOL` method; also used for `p_analyze` reporting. |
| `SHIFT_RANGE` | NUMBER(10) | Maximum shift magnitude for `SHIFT` method. DATE/TIMESTAMP: days. NUMBER: percentage of original value. Default 180. |
| `IS_PK_SOURCE` | VARCHAR2(1) | Informational flag. `'Y'` documents that this column is a PK/UK whose values are referenced by FK columns elsewhere. The package does **not** auto-discover or auto-update those FK columns; configure each FK column explicitly using `MAP_SOURCE_*`. Default `'N'`. |
| `MAP_SOURCE_SCHEMA` | VARCHAR2(64) | For application-logic FKs (no DB constraint): the schema of the column whose mapping to reuse. |
| `MAP_SOURCE_TABLE` | VARCHAR2(64) | For application-logic FKs: the table whose mapping to reuse. |
| `MAP_SOURCE_COLUMN` | VARCHAR2(64) | For application-logic FKs: the column whose mapping to reuse. |
| `CUSTOM_EXPR` | VARCHAR2(4000) | Full `SELECT … FROM DUAL` statement for `CUSTOM` method. Executed via `EXECUTE IMMEDIATE … INTO`. Tested by `p_analyze`. |
| `CONSTRAINT_TYPE` | VARCHAR2(16) | Informational label: `PK`, `FK`, `UI`, `DATA`, `LOB`. Does not affect processing. |
| `MODIFIED` | DATE | Maintained by trigger. |

### T_ANON_TOY_MAP

The translation dictionary. Populated by `p_build_map`, read by `p_apply_map`.

| Column | Type | Notes |
|--------|------|-------|
| `SOURCE_SCHEMA` | VARCHAR2(64) | Schema of the column being mapped (uppercase). |
| `SOURCE_TABLE` | VARCHAR2(64) | Table name (uppercase). |
| `SOURCE_COLUMN` | VARCHAR2(64) | Column name (uppercase). |
| `OLD_VALUE` | VARCHAR2(4000) | Original value cast to VARCHAR2 using a canonical `TO_CHAR` format. |
| `NEW_VALUE` | VARCHAR2(4000) | Anonymized replacement value. `NULL_VAL` columns produce no MAP entries at all (applied via direct `UPDATE SET col = NULL`); this column is always populated in practice. |
| `ANON_METHOD` | VARCHAR2(16) | Method that produced `NEW_VALUE`. Stored for audit. |
| `RUN_ID` | NUMBER(12) | RUN_ID of the `p_build_map` run that created this entry. |
| `CREATED` | DATE | Insert timestamp. |

**Primary key**: `(SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, OLD_VALUE)`.
This guarantees that the same old value always maps to the same new value,
regardless of how many times `p_build_map` is called.

Special row: for `SHIFT` columns, a sentinel entry with
`OLD_VALUE = '__SHIFT_OFFSET__'` and `ANON_METHOD = '__SENTINEL__'` stores
the per-column random offset so it is reused consistently across multiple runs.

### T_ANON_TOY_LOG

Append-only audit log. Every `p_analyze`, `p_build_map`, `p_apply_map`, and
`p_run` call writes entries here.

| Column | Type | Notes |
|--------|------|-------|
| `ID` | NUMBER(12) | Unique entry ID from `SEQ_ANON_TOY_LOG_ID`. Provides strict ordering within a run. |
| `RUN_ID` | NUMBER(12) | Groups all entries from one invocation. Use `WHERE RUN_ID = <n> ORDER BY ID` to replay a run. |
| `TABLE_NAME` | VARCHAR2(128) | `SCHEMA.TABLE` format, or NULL for run-level entries. |
| `STEP` | VARCHAR2(64) | Procedure name that produced this entry. |
| `STATUS_INFO` | VARCHAR2(4000) | Free-text message. Prefixes: _(none)_ normal progress; `INFO:` non-critical notice; `WARN:` potential problem; `ERROR:` per-table failure (run continues); `FATAL:` run-level failure (run aborted). |
| `MODIFIED` | DATE | Timestamp when the entry was written. |

Log writes use `PRAGMA AUTONOMOUS_TRANSACTION` so they always commit
independently of the caller's transaction. Log entries survive even if the
caller rolls back.

### T_ANON_TOY_POOL

Pre-populated table of realistic fake values used by the `POOL` method.

| Column | Type | Notes |
|--------|------|-------|
| `ID` | NUMBER(12) | Sequence-generated primary key. |
| `CATEGORY` | VARCHAR2(64) | Groups values by data type. Must match `T_ANON_TOY_COLS.DATA_CATEGORY`. |
| `VALUE` | VARCHAR2(4000) | The replacement value string. |
| `MODIFIED` | DATE | Insert timestamp. |

Pre-populated categories: `FIRST_NAME`, `LAST_NAME`, `FULL_NAME`,
`COMPANY_NAME`, `CITY`, `STREET_ADDRESS`, `PHONE_HU`, `PHONE_INTL`,
`BANK_ACCOUNT_HU3`, `BANK_ACCOUNT_HU2`, `IBAN`, `EMAIL`, `DEPARTMENT`,
`NATIONAL_ID_HU`, `NATIONAL_ID_EU`, `TAX_NUMBER`, `DIAGNOSIS_CODE`,
`MEDICATION`, `DOCTOR_NAME`.

Add your own rows with `INSERT INTO T_ANON_TOY_POOL (CATEGORY, VALUE) VALUES (...)`.

---

## 4. Package API

### `p_analyze(i_schema, i_table_name)`

Dry-run validation. No DML is performed against target tables.

Checks performed before and during the column loop:

**Privilege pre-flight** (once per call):
- `SELECT ANY DICTIONARY` — required for `DBA_TABLES`, `DBA_TAB_COLUMNS`, `DBA_CONSTRAINTS`.
- `ALTER ANY TABLE` — required to `DISABLE`/`ENABLE` FK constraints.
- `UPDATE ANY TABLE` — required for correlated `UPDATE` in `p_apply_map`.
- `SELECT ANY TABLE` — required to read target schema tables.

**Per table**:
- Table exists in `DBA_TABLES`; logs `WARN` if not found.
- Column exists in `DBA_TAB_COLUMNS`; logs `WARN` if not found.

**Per column**:
- `KEEP` columns: logged as informational; skipped.
- `POOL` columns: verifies the category has at least one pool entry; logs a
  `WARN` if empty (the `SCRAMBLE` fallback will be used at runtime).
- `CUSTOM` columns: executes `CUSTOM_EXPR` via `EXECUTE IMMEDIATE` inside a
  `SAVEPOINT` to verify it returns a value without errors; logs `WARN` on failure.
- `MAP_SOURCE` columns:
  - Logs `WARN` if only some of the three `MAP_SOURCE_*` fields are populated
    (partial configuration will cause a runtime error).
  - Logs `WARN` if `MAP_SOURCE` is set alongside `ANON_METHOD = 'NULL_VAL'`
    (`NULL_VAL` takes precedence and `MAP_SOURCE` is silently ignored).
  - Checks whether MAP entries already exist for the source column; logs `WARN`
    if none (parent must be built before child).
  - Checks `LVL` ordering: logs `WARN` if parent `LVL` ≥ child `LVL`
    (parent must be processed first to avoid `ORA-02291` at apply time).
  - Logs `INFO` if `ANON_METHOD` is set to a non-`NULL_VAL` value alongside
    `MAP_SOURCE` (the method is ignored for `MAP_SOURCE` columns; the parent's
    mapping is used instead).

Both parameters default to NULL, meaning all CMD rows with `IF_ANONYMIZE = 1`
are checked.

### `p_build_map(i_schema, i_table_name)`

Phase 1: generate old→new value mappings.

For each configured column (except `NULL_VAL` and `MAP_SOURCE` columns):
1. Fetches all distinct non-NULL values from the target table.
2. For each value not already in `T_ANON_TOY_MAP`: calls the appropriate
   value-generation function and inserts the new MAP entry.
3. Existing MAP entries are never overwritten (the PK prevents it), so
   re-running `p_build_map` is safe and will only add mappings for any new
   values that have appeared since the last run.

`SHIFT` method stores one sentinel MAP entry per column
(`OLD_VALUE = '__SHIFT_OFFSET__'`) to ensure the same offset is used
consistently across multiple runs.

### `p_apply_map(i_schema, i_table_name)`

Phase 2: apply mappings to target tables.

For each table:
1. FK constraints that reference this table are collected via `DBA_CONSTRAINTS`
   and `DISABLE`d.
2. For each configured column:
   - `NULL_VAL`: straightforward `UPDATE … SET col = NULL`.
   - All other methods: correlated subquery `UPDATE … SET col = (SELECT NEW_VALUE FROM T_ANON_TOY_MAP …)`.
   - DATE and TIMESTAMP columns are cast via `TO_CHAR` / `TO_DATE` /
     `TO_TIMESTAMP` using the format `'YYYY-MM-DD HH24:MI:SS'`.
   - NUMBER columns are cast via `TO_CHAR` / `TO_NUMBER`.
3. FK constraints are re-enabled with `ENABLE NOVALIDATE` (future DML is
   constrained; historical rows are not re-validated). Any that fail to
   re-enable are logged as `WARN` and processing continues. To fully re-validate
   historical data, issue `ALTER TABLE … ENABLE VALIDATE CONSTRAINT …` manually
   after the run.
4. On success, `IF_ANONYMIZE` is set to 0 for this table.

A per-table `BEGIN … EXCEPTION … END` block catches errors, rolls back the
table's changes, and logs an `ERROR` entry so that other tables in the same run
are not affected.

### `p_run(i_schema, i_table_name)`

Convenience wrapper that calls `p_build_map` then `p_apply_map` with the same
scope parameters.

### `p_enable(i_schema, i_table_name)` / `p_enable_all`

Reset `IF_ANONYMIZE = 1` for one table or all tables, allowing them to be
processed on the next `p_run` call.

### `p_clear_map(i_schema, i_table_name)` / `p_clear_map_all`

Delete MAP entries for one table or for all tables. Use before a fresh
anonymization run when you want entirely new replacement values rather than
reusing the existing ones.

---

## 5. Anonymization methods

### POOL

Selects a random row from `T_ANON_TOY_POOL WHERE CATEGORY = DATA_CATEGORY`.

The same old value always produces the same new value because the MAP entry is
looked up before a new pool value is drawn. If two different old values happen
to draw the same pool value, the second draw is accepted — pool uniqueness is
not enforced (the pool is for plausibility, not uniqueness guarantees).

If no pool entries exist for the given category, the method falls back silently
to SCRAMBLE. `p_analyze` will warn about empty categories in advance.

### SCRAMBLE

Generates a random replacement of the same Oracle data type:

- **VARCHAR2 / CHAR**: random alphanumeric string of the same length.
- **NUMBER**: random integer within the same order of magnitude. Negative
  originals produce negative replacements.
- **DATE / TIMESTAMP**: random date between 1970-01-01 and 2030-12-31.
- **Other types**: random 8-character alphanumeric fallback.

### SHIFT

Applies a fixed per-column random offset:

- **DATE / TIMESTAMP**: offset in days, range `[-SHIFT_RANGE, +SHIFT_RANGE]`.
- **NUMBER**: percentage offset, range `[-SHIFT_RANGE%, +SHIFT_RANGE%]`.
  Applied as `new = ROUND(old × (1 + offset/100), 10)`.

The offset is generated once and stored in the MAP table as a sentinel entry
(`OLD_VALUE = '__SHIFT_OFFSET__'`). Subsequent `p_build_map` calls reuse the
same offset, so relative ordering within the column is preserved across runs.

### FORMAT

Replaces every character position while preserving the structural pattern:

- Digits (0–9) → random digit.
- Uppercase letters (A–Z) → random uppercase letter.
- Lowercase letters (a–z) → random lowercase letter.
- All other characters (hyphens, spaces, dots, etc.) → kept as-is.

Example: `+36 20 123-4567` → `+83 71 904-2361`.

### NULL_VAL

Sets every non-NULL value to NULL with a single `UPDATE … SET col = NULL WHERE col IS NOT NULL`. No MAP entry is created.

### CUSTOM

Executes `CUSTOM_EXPR` via `EXECUTE IMMEDIATE … INTO` for every row. The
expression must be a complete SQL statement returning exactly one scalar value,
for example:

```sql
SELECT SYS_GUID() FROM DUAL
```

`p_analyze` test-executes the expression before any real data is touched.

### KEEP

Records the column in the configuration but takes no action at runtime.
`p_analyze` logs it as informational. Use it to document a deliberate decision
not to anonymize a column.

---

## 6. FK cascade strategy

### FK constraint disable/enable (auto-discovered)

Before updating each table, `p_apply_map` queries `DBA_CONSTRAINTS` to find
every FK constraint in any schema that references that table, and `DISABLE`s
them. This allows the parent table's columns (including PK columns) to be
updated without triggering FK violation errors in child tables. After the
update, each constraint is `ENABLE NOVALIDATE`d (future DML is constrained;
historical rows are not re-validated).

`IS_PK_SOURCE = 'Y'` documents that a column is a PK/UK source for FK
references elsewhere. It does **not** automatically generate UPDATE statements
for child FK columns. Child FK column values are never updated automatically —
they must always be configured explicitly (see below).

### Cascading the mapping to FK columns

To update child FK columns with the same replacement values as their parent PK,
add a row to `T_ANON_TOY_COLS` for each child FK column and set its
`MAP_SOURCE_*` to point at the parent column. This applies to both DB-enforced
FKs and application-logic relationships.

When a parent-child relationship is enforced only in application code (no DB
constraint), set the three `MAP_SOURCE_*` columns in `T_ANON_TOY_COLS` for the
child column:

```sql
-- Example: ORDERS.CUSTOMER_KEY references CUSTOMERS.CUST_ID,
-- but there is no DB-level FK constraint.
INSERT INTO T_ANON_TOY_COLS (
    TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ANON_METHOD,
    MAP_SOURCE_SCHEMA, MAP_SOURCE_TABLE, MAP_SOURCE_COLUMN,
    CONSTRAINT_TYPE
) VALUES (
    'MYAPP', 'ORDERS', 'CUSTOMER_KEY', 'SCRAMBLE',
    'MYAPP', 'CUSTOMERS', 'CUST_ID',
    'FK'
);
```

`p_build_map` skips own-mapping generation for this column.
`p_apply_map` updates `ORDERS.CUSTOMER_KEY` by looking up the MAP entries that
were built for `CUSTOMERS.CUST_ID`.

---

## 7. Logging

Every public procedure writes `START` and `END` entries to `T_ANON_TOY_LOG`.
Intermediate entries record per-table and per-column progress.

Status prefix conventions:

| Prefix | Meaning |
|--------|---------|
| _(none)_ | Normal progress |
| `INFO:` | Non-critical notice |
| `WARN:` | Potential problem; review before proceeding |
| `ERROR:` | Per-table failure; the table was rolled back but the run continues to the next table |
| `FATAL:` | Failure at the run level; the procedure returned early |

All log writes use `PRAGMA AUTONOMOUS_TRANSACTION`, so each entry is committed
immediately and independently. Log entries persist even if the calling
transaction rolls back.

Useful queries:

```sql
-- All entries from the most recent run
SELECT * FROM T_ANON_TOY_LOG
WHERE  RUN_ID = (SELECT MAX(RUN_ID) FROM T_ANON_TOY_LOG)
ORDER BY ID;

-- Warnings and errors only
SELECT * FROM T_ANON_TOY_LOG
WHERE  STATUS_INFO LIKE 'WARN%'
   OR  STATUS_INFO LIKE 'ERROR%'
   OR  STATUS_INFO LIKE 'FATAL%'
ORDER BY ID DESC;

-- All entries for a specific table
SELECT * FROM T_ANON_TOY_LOG
WHERE  TABLE_NAME = 'MYSCHEMA.MYTABLE'
ORDER BY ID;
```

---

## 8. Typical workflow

```
1. p_analyze          ← check config, test CUSTOM_EXPR, review WARNs
2. p_build_map        ← generate all old→new mappings (no target table DML)
3. review T_ANON_TOY_MAP  ← spot-check a sample of new values
4. p_apply_map        ← apply mappings, disable/enable FKs
5. review T_ANON_TOY_LOG  ← confirm no ERRORs or FATALs
6. validate results   ← query a few target tables to verify data quality
```

To re-run with the same mappings (e.g. after restoring from backup):

```sql
EXEC PKG_ANON_TOY.p_enable_all;
EXEC PKG_ANON_TOY.p_apply_map;
```

To re-run with completely fresh mappings:

```sql
EXEC PKG_ANON_TOY.p_enable_all;
EXEC PKG_ANON_TOY.p_clear_map_all;
EXEC PKG_ANON_TOY.p_run;
```

---

## 9. Known limitations

- **LOB columns** (`CLOB`, `BLOB`, `NCLOB`) are not supported. Configure them
  with `ANON_METHOD = 'NULL_VAL'` or `KEEP`.
- **Composite PKs**: auto-FK-cascade via `DBA_CONSTRAINTS` is tested only for
  single-column PKs. Composite-PK cascade should use explicit `MAP_SOURCE_*`
  entries instead.
- **COMMIT_EVERY**: the current implementation commits once per column rather
  than every N rows. For tables with very large row counts, set
  `COMMIT_EVERY` conservatively and monitor UNDO tablespace usage.
- **SHIFT DATE precision**: dates are cast to/from `'YYYY-MM-DD HH24:MI:SS'`.
  Sub-second precision on TIMESTAMP columns is lost.
- **Pool uniqueness**: the POOL method does not guarantee that two different old
  values map to two different new values. If strict uniqueness is required,
  use SCRAMBLE or a CUSTOM expression with `SYS_GUID()`.
- **Concurrent runs**: running `p_build_map` and `p_apply_map` concurrently on
  the same tables is not supported and may produce inconsistent results.
