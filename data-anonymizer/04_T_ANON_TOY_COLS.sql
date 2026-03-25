-- =============================================================================
-- 04_T_ANON_TOY_COLS.sql
-- Column definitions — one row per column that needs to be anonymized.
-- Controls how each column's values are replaced and whether its mapping
-- should be cascaded to FK columns in other tables.
--
-- Run as ANON_TOY after 03_T_ANON_TOY_CMD.sql.
-- =============================================================================

CREATE TABLE T_ANON_TOY_COLS (

    -- -------------------------------------------------------------------------
    -- Identity
    -- -------------------------------------------------------------------------
    TABLE_SCHEMA    VARCHAR2(64)   NOT NULL,
    TABLE_NAME      VARCHAR2(64)   NOT NULL,
    COLUMN_NAME     VARCHAR2(64)   NOT NULL,

    -- -------------------------------------------------------------------------
    -- Anonymization method
    -- -------------------------------------------------------------------------
    ANON_METHOD     VARCHAR2(16)   NOT NULL,
    -- How to replace the column's values.  One of:
    --
    --   POOL      Pick a random value from T_ANON_TOY_POOL for DATA_CATEGORY.
    --             The same old value always maps to the same new value via the
    --             MAP table (consistent across the run and across runs).
    --
    --   SCRAMBLE  Generate a random value of the same Oracle data type and
    --             approximate length.  For VARCHAR2: random alphanumeric string.
    --             For NUMBER: random number within the same order of magnitude.
    --             For DATE/TIMESTAMP: random value within a fixed range.
    --
    --   SHIFT     Add a fixed per-column random offset stored in the MAP table.
    --             For DATE/TIMESTAMP: offset in days (range: -SHIFT_RANGE to +SHIFT_RANGE).
    --             For NUMBER: offset as a percentage of the original value
    --             (range: -SHIFT_RANGE% to +SHIFT_RANGE%).
    --             Preserves relative ordering within the column.
    --
    --   FORMAT    Preserve the structural pattern of the value — digit positions
    --             replaced with random digits, letter positions with random
    --             letters (same case), separators kept unchanged.  Useful for
    --             phone numbers, account numbers, IBANs, ID numbers.
    --
    --   NULL_VAL  Set every value to NULL.
    --
    --   CUSTOM    Execute CUSTOM_EXPR as a SQL statement via EXECUTE IMMEDIATE
    --             and use the returned value.  Same semantics as FILLING_STRING
    --             in the data generator.
    --
    --   KEEP      Explicitly mark this column as not requiring anonymization.
    --             Logged by p_analyze but not processed at run time.

    DATA_CATEGORY   VARCHAR2(64),
    -- Used by POOL method to select the right T_ANON_TOY_POOL category.
    -- Also used for p_analyze reporting for all methods.
    -- Common values: FIRST_NAME, LAST_NAME, FULL_NAME, COMPANY_NAME,
    -- CITY, STREET_ADDRESS, PHONE, EMAIL, BANK_ACCOUNT, IBAN,
    -- NATIONAL_ID, DEPARTMENT, DIAGNOSIS_CODE, MEDICATION,
    -- CUSTOM (free-text).

    -- -------------------------------------------------------------------------
    -- SHIFT parameters
    -- -------------------------------------------------------------------------
    SHIFT_RANGE     NUMBER(10)     DEFAULT 180,
    -- Maximum shift for SHIFT method.
    -- DATE/TIMESTAMP: maximum days to shift (default 180).
    -- NUMBER: maximum percentage to shift (default 180 means ±180%).

    -- -------------------------------------------------------------------------
    -- FK cascade control
    -- -------------------------------------------------------------------------
    IS_PK_SOURCE    VARCHAR2(1)    DEFAULT 'N' NOT NULL,
    -- Informational flag.  'Y' documents that this column is a primary key
    -- (or unique key) whose values appear as FK references in other tables.
    -- This flag is used for documentation and p_analyze output only.
    -- The package does NOT auto-discover or auto-update FK columns when
    -- IS_PK_SOURCE = 'Y'.  To cascade mappings to FK columns, configure
    -- each FK column explicitly using MAP_SOURCE_* below.

    MAP_SOURCE_SCHEMA   VARCHAR2(64),
    MAP_SOURCE_TABLE    VARCHAR2(64),
    MAP_SOURCE_COLUMN   VARCHAR2(64),
    -- For FK columns whose relationship is enforced by application logic only
    -- (no DB-level constraint).  Tells the package: "when anonymizing this
    -- column, look up the mapping built for MAP_SOURCE_SCHEMA.MAP_SOURCE_TABLE
    -- .MAP_SOURCE_COLUMN rather than generating a new value."
    -- Leave NULL for non-FK columns that do not share a parent's mapping.

    -- -------------------------------------------------------------------------
    -- CUSTOM expression
    -- -------------------------------------------------------------------------
    CUSTOM_EXPR     VARCHAR2(4000),
    -- A complete SELECT statement (including FROM clause) that returns exactly
    -- one value.  Executed via EXECUTE IMMEDIATE … INTO at anonymization time.
    -- Example: 'SELECT CHR(39)||SYS_GUID()||CHR(39) FROM DUAL'
    -- Tested by p_analyze before any DML is run.

    -- -------------------------------------------------------------------------
    -- Informational
    -- -------------------------------------------------------------------------
    CONSTRAINT_TYPE VARCHAR2(16),
    -- Category label for documentation and p_analyze output.
    -- Suggested values: PK, FK, UI (unique index), DATA, LOB.
    -- Does not affect how the column is processed.

    -- -------------------------------------------------------------------------
    -- Audit
    -- -------------------------------------------------------------------------
    MODIFIED        DATE           DEFAULT SYSDATE NOT NULL,

    CONSTRAINT PK_ANON_TOY_COLS PRIMARY KEY (TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME),

    CONSTRAINT FK_ANON_TOY_COLS_CMD
        FOREIGN KEY (TABLE_SCHEMA, TABLE_NAME)
        REFERENCES T_ANON_TOY_CMD (TABLE_SCHEMA, TABLE_NAME),

    CONSTRAINT CK_ANON_TOY_COLS_METHOD
        CHECK (ANON_METHOD IN ('POOL','SCRAMBLE','SHIFT','FORMAT','NULL_VAL','CUSTOM','KEEP')),

    CONSTRAINT CK_ANON_TOY_COLS_ISPK
        CHECK (IS_PK_SOURCE IN ('Y','N'))

);

COMMENT ON TABLE  T_ANON_TOY_COLS IS
    'PKG_ANON_TOY column definitions. One row per column to anonymize. Controls method and MAP_SOURCE-based FK value cascade.';
COMMENT ON COLUMN T_ANON_TOY_COLS.TABLE_SCHEMA       IS 'Schema of the target table. Must match T_ANON_TOY_CMD.';
COMMENT ON COLUMN T_ANON_TOY_COLS.TABLE_NAME         IS 'Target table name. Must match T_ANON_TOY_CMD.';
COMMENT ON COLUMN T_ANON_TOY_COLS.COLUMN_NAME        IS 'Column to anonymize within the target table.';
COMMENT ON COLUMN T_ANON_TOY_COLS.ANON_METHOD        IS 'Anonymization method: POOL, SCRAMBLE, SHIFT, FORMAT, NULL_VAL, CUSTOM, KEEP.';
COMMENT ON COLUMN T_ANON_TOY_COLS.DATA_CATEGORY      IS 'Pool category or data description. Used by POOL method and p_analyze.';
COMMENT ON COLUMN T_ANON_TOY_COLS.SHIFT_RANGE        IS 'Max shift for SHIFT method. Days for DATE; percentage for NUMBER. Default 180.';
COMMENT ON COLUMN T_ANON_TOY_COLS.IS_PK_SOURCE       IS 'Informational. Y if this column is a PK/UK referenced by FK columns. Does not trigger auto-cascade; configure MAP_SOURCE_* on each FK column explicitly.';
COMMENT ON COLUMN T_ANON_TOY_COLS.MAP_SOURCE_SCHEMA  IS 'For application-logic FKs: source schema whose mapping to reuse.';
COMMENT ON COLUMN T_ANON_TOY_COLS.MAP_SOURCE_TABLE   IS 'For application-logic FKs: source table whose mapping to reuse.';
COMMENT ON COLUMN T_ANON_TOY_COLS.MAP_SOURCE_COLUMN  IS 'For application-logic FKs: source column whose mapping to reuse.';
COMMENT ON COLUMN T_ANON_TOY_COLS.CUSTOM_EXPR        IS 'Full SELECT statement for CUSTOM method. Executed via EXECUTE IMMEDIATE INTO.';
COMMENT ON COLUMN T_ANON_TOY_COLS.CONSTRAINT_TYPE    IS 'Informational label: PK, FK, UI, DATA, LOB.';
COMMENT ON COLUMN T_ANON_TOY_COLS.MODIFIED           IS 'Last modification timestamp (set by trigger).';

CREATE INDEX IDX_ANON_TOY_COLS_TBL
    ON T_ANON_TOY_COLS (TABLE_SCHEMA, TABLE_NAME);

CREATE OR REPLACE TRIGGER TRG_ANON_TOY_COLS_MOD
    BEFORE INSERT OR UPDATE ON T_ANON_TOY_COLS
    FOR EACH ROW
BEGIN
    :NEW.MODIFIED := SYSDATE;
END;
/
