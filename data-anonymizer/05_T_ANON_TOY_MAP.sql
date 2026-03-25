-- =============================================================================
-- 05_T_ANON_TOY_MAP.sql
-- Mapping table — stores the old_value → new_value translation for every
-- anonymized column value.
--
-- Purpose:
--   1. Guarantees consistency: the same old value always produces the same
--      new value, both within a run and across multiple runs.
--   2. Enables FK cascade: child tables referencing a parent PK column look
--      up the replacement value here rather than generating a new one.
--   3. Provides an audit trail: after a run you can query what was changed.
--
-- Lifecycle:
--   - Populated during the build-map phase (p_build_map).
--   - Read during the apply phase (p_apply_map).
--   - Persists between runs by default (allows re-running apply without
--     rebuilding).  Clear manually or via p_clear_map when starting fresh.
--
-- Run as ANON_TOY after 04_T_ANON_TOY_COLS.sql.
-- =============================================================================

CREATE TABLE T_ANON_TOY_MAP (

    -- -------------------------------------------------------------------------
    -- Source identity (PK of the mapping)
    -- -------------------------------------------------------------------------
    SOURCE_SCHEMA   VARCHAR2(64)    NOT NULL,
    -- Schema of the table where this column lives.

    SOURCE_TABLE    VARCHAR2(64)    NOT NULL,
    -- Table where this column lives.

    SOURCE_COLUMN   VARCHAR2(64)    NOT NULL,
    -- Column whose value is being replaced.

    OLD_VALUE       VARCHAR2(4000)  NOT NULL,
    -- The original value as a string.  All data types are cast to VARCHAR2
    -- using TO_CHAR with a canonical format before storage.

    -- -------------------------------------------------------------------------
    -- Replacement value
    -- -------------------------------------------------------------------------
    NEW_VALUE       VARCHAR2(4000),
    -- The anonymized replacement value as a string.
    -- NULL_VAL columns produce no MAP entry at all (they are applied via a
    -- direct UPDATE SET col = NULL without a MAP lookup).  NULL_VAL rows
    -- therefore never appear in this table.

    -- -------------------------------------------------------------------------
    -- Metadata
    -- -------------------------------------------------------------------------
    ANON_METHOD     VARCHAR2(16)    NOT NULL,
    -- The method that produced NEW_VALUE.  Stored for audit purposes.

    RUN_ID          NUMBER(12)      NOT NULL,
    -- The RUN_ID of the build-map run that created this entry.

    CREATED         DATE            DEFAULT SYSDATE NOT NULL,

    CONSTRAINT PK_ANON_TOY_MAP
        PRIMARY KEY (SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, OLD_VALUE)

);

COMMENT ON TABLE  T_ANON_TOY_MAP IS
    'PKG_ANON_TOY value mapping table. Stores old->new translations for all anonymized column values.';
COMMENT ON COLUMN T_ANON_TOY_MAP.SOURCE_SCHEMA  IS 'Schema of the table whose column is being mapped.';
COMMENT ON COLUMN T_ANON_TOY_MAP.SOURCE_TABLE   IS 'Table whose column is being mapped.';
COMMENT ON COLUMN T_ANON_TOY_MAP.SOURCE_COLUMN  IS 'Column whose values are being mapped.';
COMMENT ON COLUMN T_ANON_TOY_MAP.OLD_VALUE      IS 'Original value, cast to VARCHAR2 using a canonical TO_CHAR format.';
COMMENT ON COLUMN T_ANON_TOY_MAP.NEW_VALUE      IS 'Anonymized replacement value. NULL_VAL columns produce no MAP entry; this column is always non-NULL in practice.';
COMMENT ON COLUMN T_ANON_TOY_MAP.ANON_METHOD    IS 'Method used to produce NEW_VALUE (for audit).';
COMMENT ON COLUMN T_ANON_TOY_MAP.RUN_ID         IS 'RUN_ID of the p_build_map run that created this entry.';
COMMENT ON COLUMN T_ANON_TOY_MAP.CREATED        IS 'Timestamp when this mapping entry was inserted.';

-- Support fast lookup by table (used during apply phase per-table)
CREATE INDEX IDX_ANON_TOY_MAP_SRC
    ON T_ANON_TOY_MAP (SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN);
