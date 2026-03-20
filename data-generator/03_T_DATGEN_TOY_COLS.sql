-- =============================================================================
-- 03_T_DATGEN_TOY_COLS.sql
-- Special column configuration table.
-- Defines how to generate values for columns that cannot be filled with
-- unconstrained random data: foreign keys, unique-constrained columns, custom
-- primary keys (overriding auto-detection), and application-specific LOB
-- content.
--
-- Columns NOT listed here are handled automatically by the package:
--   - PK columns are auto-detected from the data dictionary and filled
--     using type-appropriate defaults (NUMBER: MAX+N, VARCHAR2/CHAR: SYS_GUID,
--     RAW: SYS_GUID). Add a row here to override that default for a PK column.
--   - All other columns are filled by data type (string, number, date, etc.).
--
-- Objects created:
--   T_DATGEN_TOY_COLS         table
--   PK_DATGEN_TOY_COLS        primary key constraint
--   FK_DATGEN_TOY_COLS        foreign key to T_DATGEN_TOY_CMD
--   TRG_DATGEN_TOY_COLS       before-insert/update trigger (maintains MODIFIED)
--   COMMENT ON TABLE / COLUMN metadata
-- =============================================================================

CREATE TABLE T_DATGEN_TOY_COLS (

    -- Name of the target table this column belongs to.
    -- Must have a matching entry in T_DATGEN_TOY_CMD.
    -- Part of the composite primary key.
    TABLE_NAME       VARCHAR2(64)    NOT NULL,

    -- Name of the column within the target table that requires special handling.
    -- Part of the composite primary key.
    COLUMN_NAME      VARCHAR2(64)    NOT NULL,

    -- A complete, executable SELECT statement that returns exactly one value.
    -- Executed at row-generation time via EXECUTE IMMEDIATE ... INTO.
    -- The returned value is embedded directly into the VALUES clause of the
    -- INSERT statement, so:
    --   * NUMBER values need no quoting.
    --     Example: 'SELECT seq_orders.nextval FROM dual'
    --   * VARCHAR2 / CHAR values must include their own surrounding quotes.
    --     Example: 'SELECT chr(39)||SYS_GUID()||chr(39) FROM dual'
    --   * FK values should select a randomly chosen valid parent key.
    --     Example: 'SELECT customer_id FROM customers
    --               ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROW ONLY'
    --     (quote the result as above if the column type is VARCHAR2)
    FILLING_STRING   VARCHAR2(4000)  NOT NULL,

    -- Category of the constraint or special case driving this configuration.
    -- Informational; used for documentation and future filtering.
    -- Valid values:
    --   PK  — primary key (custom expression overriding auto-detection)
    --   FK  — foreign key (must return a value present in the parent table)
    --   UI  — unique index or unique constraint
    --   LOB — large object requiring application-specific content
    CONSTRAINT_TYPE  VARCHAR2(16)    NOT NULL,

    -- Timestamp of the last INSERT or UPDATE on this row.
    -- Maintained automatically by trigger TRG_DATGEN_TOY_COLS.
    MODIFIED         TIMESTAMP       NOT NULL,

    CONSTRAINT PK_DATGEN_TOY_COLS PRIMARY KEY (TABLE_NAME, COLUMN_NAME),

    CONSTRAINT FK_DATGEN_TOY_COLS
        FOREIGN KEY (TABLE_NAME)
        REFERENCES  T_DATGEN_TOY_CMD (TABLE_NAME)
);


-- -----------------------------------------------------------------------------
-- Trigger: keep MODIFIED current on every write.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER TRG_DATGEN_TOY_COLS
BEFORE INSERT OR UPDATE ON T_DATGEN_TOY_COLS
FOR EACH ROW
BEGIN
    :NEW.MODIFIED := SYSTIMESTAMP;
END TRG_DATGEN_TOY_COLS;
/


-- -----------------------------------------------------------------------------
-- Oracle data dictionary comments
-- -----------------------------------------------------------------------------
COMMENT ON TABLE T_DATGEN_TOY_COLS IS
    'Special column configuration for PKG_DATGEN_TOY. Each row supplies a custom SQL expression for one column in a target table. Use this for FK columns (must reference a valid parent row), unique-constrained columns, custom PK expressions (overrides auto-detection), and LOBs with specific content. Columns not listed here are handled automatically by the package based on their data type.';

COMMENT ON COLUMN T_DATGEN_TOY_COLS.TABLE_NAME IS
    'Target table name. Must match an entry in T_DATGEN_TOY_CMD.TABLE_NAME. Part of the composite primary key. Foreign key to the command table.';

COMMENT ON COLUMN T_DATGEN_TOY_COLS.COLUMN_NAME IS
    'Column name within the target table that requires a custom fill expression. Combined with TABLE_NAME to form the primary key of this table.';

COMMENT ON COLUMN T_DATGEN_TOY_COLS.FILLING_STRING IS
    'A complete SELECT statement (including FROM clause) returning exactly one value. Executed via EXECUTE IMMEDIATE at row-generation time. The result is embedded directly into the INSERT VALUES clause — number values need no quoting; string values must include their own surrounding single quotes (use chr(39) to avoid escape complexity). Example for a NUMBER FK: ''SELECT id FROM parent ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROW ONLY''. Example for a VARCHAR2 FK: ''SELECT chr(39)||code||chr(39) FROM ref_table ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROW ONLY''.';

COMMENT ON COLUMN T_DATGEN_TOY_COLS.CONSTRAINT_TYPE IS
    'Category of the special handling. Valid values: PK (primary key — overrides auto-detection), FK (foreign key — expression must return a value present in the parent table), UI (unique index or constraint), LOB (large object with specific content requirements).';

COMMENT ON COLUMN T_DATGEN_TOY_COLS.MODIFIED IS
    'Timestamp of the last INSERT or UPDATE on this configuration row. Set automatically by trigger TRG_DATGEN_TOY_COLS — do not populate manually.';
