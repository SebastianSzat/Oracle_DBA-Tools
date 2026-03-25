# ANON_TOY — Oracle Data Anonymizer

> ## ⚠ Work in Progress
>
> **This project is not production-ready and has not been thoroughly tested.**
>
> ANON_TOY is a personal side project developed in my spare time. While the design
> is intentional and the core logic has been reviewed, the package has not been
> validated against a broad range of real-world schemas, data types, or edge cases.
> Known and unknown bugs are likely present.
>
> **Use this as a reference, a starting point, or a learning resource — not as a
> drop-in solution for sensitive data.** If you run it against a real database,
> treat the results with caution: unexpected behaviour, incorrect mappings, or
> incomplete anonymization may occur without any visible error.
>
> Contributions, bug reports, and suggestions are welcome. Progress on testing and
> stabilisation will continue as time allows.

A PL/SQL package that anonymizes sensitive data in staging Oracle databases while
preserving referential integrity across parent-child table relationships.

## The story

Years ago I built an anonymizer for a staging database we rebuilt yearly from a
live copy. The source contained real stock-exchange and banking data, so we could
not use it directly for stress-testing and pre-deployment validation. The original
package worked well but was tightly coupled to a specific schema and data model, and could
not be shared publicly due to NDA obligations.

This is a clean, improved rebuild. It keeps the same core idea — a command table
drives which tables to process, a columns table defines how each column is
anonymized, and a mapping table records every old→new translation so that
referential integrity is maintained across any number of child tables. The new
version adds a pool table of pre-populated realistic-looking fake data (fantasy
names, IBANs, phone numbers, healthcare codes), more anonymization methods,
auto-discovery of FK constraints, and comprehensive logging.

Two features are planned for a later stage: an auto-mapper that identifies columns likely to contain sensitive data, and post-run map cleanup that removes the old→new translation table after a successful run — ensuring the anonymized dataset cannot be partially reversed by joining back to the mapping table.

## Requirements

| Requirement | Notes |
|---|---|
| Oracle Database | 11gR2 or later |
| `DBMS_RANDOM` | Granted to ANON_TOY schema (see `02_create_schema.sql`) |
| `DBMS_ASSERT` | Granted to ANON_TOY schema |
| `DBMS_UTILITY` | Granted to ANON_TOY schema |
| DBA-level ANY privileges | `SELECT/UPDATE/ALTER ANY TABLE`, `SELECT ANY DICTIONARY` — granted in `02_create_schema.sql` |
| SYSDBA access | Required to run `02_create_schema.sql` |

## Installation order

Run the scripts in sequence as the user indicated in each file header:

| # | File | Run as | Purpose |
|---|------|--------|---------|
| 01 | `01_cleanup.sql` | ANON_TOY or SYSDBA | Drop all existing ANON_TOY objects |
| 02 | `02_create_schema.sql` | SYSDBA | Create ANON_TOY user and grant privileges |
| 03 | `03_T_ANON_TOY_CMD.sql` | ANON_TOY | Command table |
| 04 | `04_T_ANON_TOY_COLS.sql` | ANON_TOY | Column definitions |
| 05 | `05_T_ANON_TOY_MAP.sql` | ANON_TOY | Value mapping table |
| 06 | `06_T_ANON_TOY_LOG.sql` | ANON_TOY | Log table and sequence |
| 07 | `07_T_ANON_TOY_POOL.sql` | ANON_TOY | Pool table and pre-populated data |
| 08 | `08_PKG_ANON_TOY.sql` | ANON_TOY | Package specification and body |
| 09 | `09_ANON_TOY_test_structure.sql` | ANON_TOY | Test tables and sample configuration |

## Quick start

```sql
-- 1. Populate T_ANON_TOY_CMD and T_ANON_TOY_COLS for your target schema.
--    See 09_ANON_TOY_test_structure.sql for a working example.

-- 2. Dry-run to validate configuration:
EXEC PKG_ANON_TOY.p_analyze;

-- 3. Full run (build mappings, then apply):
EXEC PKG_ANON_TOY.p_run;

-- 4. Review the log:
SELECT * FROM T_ANON_TOY_LOG ORDER BY ID DESC;
```

## Anonymization methods

| Method | Description |
|--------|-------------|
| `POOL` | Pick a random value from `T_ANON_TOY_POOL` for the specified `DATA_CATEGORY`. Falls back to SCRAMBLE if the category is empty. |
| `SCRAMBLE` | Generate a random value of the same type and approximate length. VARCHAR2 → random alphanumeric; NUMBER → same order of magnitude; DATE → random date 1970–2030. |
| `SHIFT` | Add a fixed per-column random offset. DATE/TIMESTAMP: shift by up to `SHIFT_RANGE` days. NUMBER: shift by up to `SHIFT_RANGE`% of the original value. Preserves relative ordering. |
| `FORMAT` | Preserve the structural pattern: digits replaced with random digits, letters with random letters (same case), separators kept. Useful for phone numbers, IBANs, account numbers. |
| `NULL_VAL` | Set every value to NULL. |
| `CUSTOM` | Execute `CUSTOM_EXPR` (a full `SELECT … FROM DUAL` statement) via `EXECUTE IMMEDIATE`. Tested by `p_analyze` before any real data is touched. |
| `KEEP` | Explicitly mark a column as not requiring anonymization. Logged by `p_analyze` but not processed. |

## FK cascade

The package handles referential integrity in two ways:

1. **FK constraint disable/enable**: Before updating a table, the package
   queries `DBA_CONSTRAINTS` to find every FK constraint in any schema that
   references that table, and `DISABLE`s them. This allows parent PK columns to
   be updated without triggering FK violations in child tables. The constraints
   are `ENABLE NOVALIDATE`d after the update. `IS_PK_SOURCE = 'Y'` documents
   that a column is a PK/UK source, but **does not** automatically update child
   FK column values — that always requires explicit configuration (see below).

2. **Cascading the mapping to FK columns**: To update a child FK column with
   the same replacement values as its parent PK, add a row to `T_ANON_TOY_COLS`
   for the child column and set `MAP_SOURCE_SCHEMA`, `MAP_SOURCE_TABLE`, and
   `MAP_SOURCE_COLUMN` to point to the parent column. The package will reuse the
   parent's MAP entries rather than generating new values. This applies to both
   DB-constraint FKs and application-logic-only relationships.

FK constraints are `DISABLE`d before each table's updates and `ENABLE NOVALIDATE`d
afterwards (future DML is constrained; historical rows are not re-checked). Any
constraint that cannot be re-enabled is logged as a `WARN`. To fully re-validate
historical data after a run, issue `ALTER TABLE … ENABLE VALIDATE CONSTRAINT …`
manually for each affected constraint.

## Pool data categories

The following categories are pre-populated in `T_ANON_TOY_POOL`:

`FIRST_NAME` · `LAST_NAME` · `FULL_NAME` · `COMPANY_NAME` · `CITY` ·
`STREET_ADDRESS` · `PHONE_HU` · `PHONE_INTL` · `BANK_ACCOUNT_HU3` ·
`BANK_ACCOUNT_HU2` · `IBAN` · `EMAIL` · `DEPARTMENT` · `NATIONAL_ID_HU` ·
`NATIONAL_ID_EU` · `TAX_NUMBER` · `DIAGNOSIS_CODE` · `MEDICATION` · `DOCTOR_NAME`

Add your own rows to `T_ANON_TOY_POOL` with any `CATEGORY` value you define in
`T_ANON_TOY_COLS.DATA_CATEGORY`.

## Security notes

- The package uses `AUTHID DEFINER`. No `EXECUTE` privilege is granted to other
  users or `PUBLIC`, so it cannot be run accidentally from another session.
- All dynamic SQL identifiers are wrapped with `DBMS_ASSERT.ENQUOTE_NAME` to
  prevent SQL injection.
- Run `01_cleanup.sql` (Section B) as SYSDBA to fully decommission the schema.
