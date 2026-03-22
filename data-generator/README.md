# PKG_DATGEN_TOY — Oracle Random Data Generator

## Origin

This tool started as a practical necessity. A few years ago I was working on archivation testing for complex, multi-level table structures — parent-child hierarchies with foreign keys several levels deep — and I needed a fast way to fill those tables with realistic-volume random data so I could measure actual runtimes in a safe sandbox environment. No production data, no hand-crafted fixtures, just enough rows to make the database work hard.

The original script was a single ~330-line PL/SQL file. It did the job: it could fill tables, it respected basic data types, and it gave me the row counts I needed to benchmark the archivation logic. The LOB population path was never properly implemented — that part was left as a known gap — and a handful of column types were not covered. But for the use case at the time, it worked well enough.

The script sat untouched for a few years.

A few weeks ago I rediscovered it, and decided it was worth doing properly. The old issues (LOB path, missing types, NLS-unsafe numeric literals, raw byte handling) were fixed, the API was expanded, and the code was commented extensively so that anyone picking it up — including future me — can understand exactly what each part does and why. AI assistance was used throughout: to identify problems in the original code, to reason through edge cases, and to write the inline documentation that now accompanies every function, cursor, and design decision.

After that first revision worked well on all my test cases, I installed a large set of skills and tools into my Claude setup — Oracle DBA guides, PL/SQL best-practice packs, security hardening rules, and more. A follow-up analysis with those tools available suggested seven further improvements: a missing `PRAGMA AUTONOMOUS_TRANSACTION` on the log function, a broken `TIMESTAMP WITH TIME ZONE` literal, SQL injection risk on table name identifiers, code duplication in the column-collection logic, incomplete error backtraces, a clunky discard-variable pattern for log calls, and no pre-run validation of custom expressions. I applied most of those changes. The previous version worked correctly on my test cases, but this one should be better — safer, cleaner, and with earlier, more useful error messages.

The result is what you see here.

---

## What it does

`PKG_DATGEN_TOY` fills Oracle tables with randomly generated data. You tell it which tables to populate, how many rows you want, and how to handle any columns that need special values (foreign keys, unique constraints, custom expressions). It takes care of everything else: primary key generation, data-type-appropriate random values, LOB content, direct-path inserts, intermediate commits, and per-run logging.

It is intentionally a **sandbox tool** — useful for performance testing, archivation testing, stress testing, and any situation where you need realistic data volume without using production data.

---

## Files

| File | Purpose |
|------|---------|
| `01_cleanup.sql` | Drops all `DATGEN_TOY` objects in dependency-safe order. Run before re-deploying. |
| `02_T_DATGEN_TOY_CMD.sql` | Command table — one row per target table, controls row counts, date ranges, LOB sizes, and the APPEND hint. |
| `03_T_DATGEN_TOY_COLS.sql` | Column overrides — supplies custom SQL expressions for FK columns, unique-constrained columns, and custom PKs. |
| `04_T_DATGEN_TOY_LOG.sql` | Log table and sequence — every step of every run is recorded here, grouped by `RUN_ID`. |
| `05_PKG_DATGEN_TOY.sql` | The package: specification and body. Public API plus all helper functions. |
| `06_DATGEN_TOY_test_structure.sql` | Test tables covering all supported column types, with matching CMD and COLS configuration. |

Deploy in numbered order. `01_cleanup.sql` can be re-run at any time to start fresh.

---

## Quick start

```sql
-- 1. Deploy all objects (run in order: 01 → 05)

-- 2. Register a target table
INSERT INTO T_DATGEN_TOY_CMD (TABLE_NAME, LVL, ROW_AMOUNT, MAX_ROWS)
VALUES ('MY_TABLE', 10, 1000, 10000);

-- 3. Add column overrides for any FK or unique columns
INSERT INTO T_DATGEN_TOY_COLS (TABLE_NAME, COLUMN_NAME, FILLING_STRING, CONSTRAINT_TYPE)
VALUES ('MY_TABLE', 'PARENT_ID',
        'SELECT ID FROM PARENT_TABLE ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROW ONLY',
        'FK');

COMMIT;

-- 4. Run
BEGIN PKG_DATGEN_TOY.p_run; END;
/

-- 5. Check the log
SELECT * FROM T_DATGEN_TOY_LOG ORDER BY ID;
```

See [MANUAL.md](MANUAL.md) for full configuration reference, all public API calls, supported column types, and worked examples.

---

## Requirements

- Oracle Database 12c or later (uses `FETCH FIRST N ROWS ONLY` in COLS expressions)
- Execute privilege on `DBMS_RANDOM`, `DBMS_LOB`, `DBMS_ASSERT`, `DBMS_UTILITY`, `SYS_GUID`
- A writable user tablespace (LOB content can grow quickly — size accordingly)
- All objects are created in the current schema (`USER_TABLES`, `USER_TAB_COLS`, etc.)
