-- =============================================================================
-- 02_T_DATGEN_TOY_CMD.sql
-- Command / configuration table.
-- Each row defines one target table to be filled with generated data.
-- The package reads this table on every call and processes only rows where
-- IF_GENERATE = 1, in ascending LVL order.
--
-- Objects created:
--   T_DATGEN_TOY_CMD          table
--   PK_DATGEN_TOY_CMD         primary key constraint
--   TRG_DATGEN_TOY_CMD        before-insert/update trigger (maintains MODIFIED)
--   COMMENT ON TABLE / COLUMN metadata
-- =============================================================================

CREATE TABLE T_DATGEN_TOY_CMD (

    -- Name of the target table to fill.
    -- Must match the object name in USER_TABLES exactly (uppercase).
    -- Primary key — duplicate entries for the same table are not allowed.
    TABLE_NAME   VARCHAR2(64)   NOT NULL,

    -- Execution order for this table within a single run.
    -- Tables are processed in ascending order.
    -- Set lower values for parent tables (those referenced by FK constraints)
    -- so they are populated before the child tables that depend on them.
    LVL          NUMBER(10)     NOT NULL,

    -- Number of rows to attempt to insert in a single run.
    -- If (current row count + ROW_AMOUNT) would exceed MAX_ROWS, the run
    -- inserts only as many rows as needed to reach the MAX_ROWS ceiling.
    ROW_AMOUNT   NUMBER(10)     NOT NULL,

    -- Hard ceiling on the number of rows this table should contain.
    -- Processing is skipped entirely if the current count already meets
    -- or exceeds this value.
    MAX_ROWS     NUMBER(10)     NOT NULL,

    -- Run flag.
    -- 1 = process this table on the next call to p_run / f_fill_tables.
    -- 0 = skip (either manually disabled or automatically set after a
    --     successful run).
    -- Reset to 1 using p_enable(table_name) or p_enable_all to re-enable.
    IF_GENERATE  NUMBER(1)      DEFAULT 1     NOT NULL,

    -- Number of rows inserted between intermediate COMMIT calls.
    -- Recommended: 1000 for tables without LOB columns (default).
    --              100  for tables that contain BLOB or CLOB columns,
    --                   because LOB content inflates each UNDO entry.
    COMMIT_EVERY NUMBER(10)     DEFAULT 1000  NOT NULL,

    -- Target size in kilobytes for each generated BLOB or CLOB value.
    -- The package calculates the number of ~2000-byte cycles needed.
    -- Irrelevant if the table has no LOB columns.
    -- Each table can have a different LOB size; tune per use case.
    LOB_SIZE_KB  NUMBER(10)     DEFAULT 10    NOT NULL,

    -- Earliest date (inclusive) used when generating random DATE and
    -- TIMESTAMP values for this table.
    -- Applies to all DATE/TIMESTAMP columns handled automatically.
    DATE_FROM    DATE           DEFAULT DATE '2000-01-01'  NOT NULL,

    -- Latest date (inclusive) used when generating random DATE and
    -- TIMESTAMP values for this table.
    DATE_TO      DATE           DEFAULT DATE '2030-12-31'  NOT NULL,

    -- Direct-path insert flag.
    -- 1 = add /*+ APPEND */ hint to INSERT statements; writes directly
    --     to new extents, bypassing the buffer cache.  Significantly
    --     faster for large ROW_AMOUNT values.
    -- 0 = standard INSERT (default).
    -- NOTE: APPEND is automatically disabled for tables that contain
    --       BLOB or CLOB columns, because direct-path inserts are
    --       incompatible with the RETURNING ROWID pattern used to
    --       populate LOB content.
    USE_APPEND   NUMBER(1)      DEFAULT 0     NOT NULL,

    -- Timestamp of the last INSERT or UPDATE on this row.
    -- Maintained automatically by trigger TRG_DATGEN_TOY_CMD.
    MODIFIED     TIMESTAMP      NOT NULL,

    CONSTRAINT PK_DATGEN_TOY_CMD PRIMARY KEY (TABLE_NAME)
);


-- -----------------------------------------------------------------------------
-- Trigger: keep MODIFIED current on every write.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER TRG_DATGEN_TOY_CMD
BEFORE INSERT OR UPDATE ON T_DATGEN_TOY_CMD
FOR EACH ROW
BEGIN
    :NEW.MODIFIED := SYSTIMESTAMP;
END TRG_DATGEN_TOY_CMD;
/


-- -----------------------------------------------------------------------------
-- Oracle data dictionary comments
-- -----------------------------------------------------------------------------
COMMENT ON TABLE T_DATGEN_TOY_CMD IS
    'Command table for PKG_DATGEN_TOY. Each row defines one target table to be filled with generated test data. Rows with IF_GENERATE=1 are processed on the next p_run / f_fill_tables call; the flag is automatically reset to 0 after successful completion. Use p_enable(table_name) or p_enable_all to re-enable.';

COMMENT ON COLUMN T_DATGEN_TOY_CMD.TABLE_NAME IS
    'Name of the target table to fill. Must match the object name in USER_TABLES exactly (uppercase). Primary key.';

COMMENT ON COLUMN T_DATGEN_TOY_CMD.LVL IS
    'Execution order. Tables are processed in ascending LVL order within a single run. Assign lower values to parent tables (those referenced by FK constraints) to guarantee they are populated before child tables.';

COMMENT ON COLUMN T_DATGEN_TOY_CMD.ROW_AMOUNT IS
    'Number of rows to insert in one run. Capped automatically if the total would exceed MAX_ROWS.';

COMMENT ON COLUMN T_DATGEN_TOY_CMD.MAX_ROWS IS
    'Maximum row count allowed in this table. Processing is skipped when the current count is already at or above this value.';

COMMENT ON COLUMN T_DATGEN_TOY_CMD.IF_GENERATE IS
    'Run flag. 1 = enabled (process on next run). 0 = disabled (skip). Automatically set to 0 after a successful run. Use p_enable(table_name) or p_enable_all to reset.';

COMMENT ON COLUMN T_DATGEN_TOY_CMD.COMMIT_EVERY IS
    'Rows inserted between intermediate commits. Default: 1000. Use ~100 for tables with BLOB or CLOB columns to limit UNDO growth per transaction.';

COMMENT ON COLUMN T_DATGEN_TOY_CMD.LOB_SIZE_KB IS
    'Target size in KB for each generated BLOB or CLOB value. Each table can specify a different size. Default: 10 KB. Has no effect on tables with no LOB columns.';

COMMENT ON COLUMN T_DATGEN_TOY_CMD.DATE_FROM IS
    'Earliest date (inclusive) for randomly generated DATE and TIMESTAMP values in this table. Default: 2000-01-01.';

COMMENT ON COLUMN T_DATGEN_TOY_CMD.DATE_TO IS
    'Latest date (inclusive) for randomly generated DATE and TIMESTAMP values in this table. Default: 2030-12-31.';

COMMENT ON COLUMN T_DATGEN_TOY_CMD.USE_APPEND IS
    'Direct-path insert flag. 1 = use /*+ APPEND */ hint (faster for large inserts; bypasses buffer cache). 0 = standard INSERT. Automatically overridden to 0 for tables with BLOB or CLOB columns, as direct-path inserts are incompatible with the RETURNING ROWID pattern used for LOB population.';

COMMENT ON COLUMN T_DATGEN_TOY_CMD.MODIFIED IS
    'Timestamp of the last INSERT or UPDATE on this row. Set automatically by trigger TRG_DATGEN_TOY_CMD.';
