-- =============================================================================
-- 03_T_ANON_TOY_CMD.sql
-- Command table — one row per target table to be anonymized.
-- Controls processing order, run flag, and commit frequency.
--
-- Run as ANON_TOY after 02_create_schema.sql.
-- =============================================================================

CREATE TABLE T_ANON_TOY_CMD (

    -- -------------------------------------------------------------------------
    -- Identity
    -- -------------------------------------------------------------------------
    TABLE_SCHEMA    VARCHAR2(64)   NOT NULL,
    -- Schema (owner) of the target table.  Must match DBA_TABLES.OWNER exactly
    -- (uppercase).  The anonymizer uses DBA-level ANY privileges to access it.

    TABLE_NAME      VARCHAR2(64)   NOT NULL,
    -- Target table name.  Must match DBA_TABLES.TABLE_NAME exactly (uppercase).

    -- -------------------------------------------------------------------------
    -- Processing order
    -- -------------------------------------------------------------------------
    LVL             NUMBER(10)     NOT NULL,
    -- Processing order within a run.  Lower values are processed first.
    -- Set parent tables to a lower LVL than their dependant child tables so
    -- the parent mapping is built before child FK columns are resolved.

    -- -------------------------------------------------------------------------
    -- Run control
    -- -------------------------------------------------------------------------
    IF_ANONYMIZE    NUMBER(1)      DEFAULT 1  NOT NULL,
    -- Run flag.  1 = process on next run.  0 = skip.
    -- Set to 0 automatically after a successful run for this table.
    -- Reset with p_enable(schema, table_name) or p_enable_all.

    -- -------------------------------------------------------------------------
    -- Performance
    -- -------------------------------------------------------------------------
    COMMIT_EVERY    NUMBER(10)     DEFAULT 1000 NOT NULL,
    -- Number of MAP entries inserted between intermediate commits during
    -- the build phase (p_build_map_core), counted per column.
    -- Has no effect on the apply phase — p_apply_map commits once per
    -- column regardless of this setting.
    -- Limits UNDO tablespace pressure when building large mapping tables.
    -- A value of 1 commits after every MAP entry (safest but slowest).

    -- -------------------------------------------------------------------------
    -- Audit
    -- -------------------------------------------------------------------------
    MODIFIED        DATE           DEFAULT SYSDATE NOT NULL,

    CONSTRAINT PK_ANON_TOY_CMD PRIMARY KEY (TABLE_SCHEMA, TABLE_NAME),

    CONSTRAINT CK_ANON_TOY_CMD_FLAG
        CHECK (IF_ANONYMIZE IN (0, 1)),

    CONSTRAINT CK_ANON_TOY_CMD_COMMIT
        CHECK (COMMIT_EVERY >= 1)

);

COMMENT ON TABLE  T_ANON_TOY_CMD IS
    'PKG_ANON_TOY command table. One row per target table. Controls anonymization order and run behaviour.';
COMMENT ON COLUMN T_ANON_TOY_CMD.TABLE_SCHEMA  IS 'Schema (owner) of the target table. Uppercase.';
COMMENT ON COLUMN T_ANON_TOY_CMD.TABLE_NAME    IS 'Target table name. Uppercase.';
COMMENT ON COLUMN T_ANON_TOY_CMD.LVL           IS 'Processing order. Lower values run first. Set parents lower than dependant children.';
COMMENT ON COLUMN T_ANON_TOY_CMD.IF_ANONYMIZE  IS '1 = process on next run. 0 = skip. Reset with p_enable or p_enable_all.';
COMMENT ON COLUMN T_ANON_TOY_CMD.COMMIT_EVERY  IS 'MAP entries inserted between intermediate commits during p_build_map (per column). No effect on p_apply_map. Default 1000.';
COMMENT ON COLUMN T_ANON_TOY_CMD.MODIFIED      IS 'Last modification timestamp (set by trigger).';


-- Trigger: keep MODIFIED current
CREATE OR REPLACE TRIGGER TRG_ANON_TOY_CMD_MOD
    BEFORE INSERT OR UPDATE ON T_ANON_TOY_CMD
    FOR EACH ROW
BEGIN
    :NEW.MODIFIED := SYSDATE;
END;
/
