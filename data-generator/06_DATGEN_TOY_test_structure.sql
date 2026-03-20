-- =============================================================================
-- 06_DATGEN_TOY_test_structure.sql
-- Test tables and PKG_DATGEN_TOY configuration for functional validation.
--
-- Creates two tables that together cover every column type handled by the
-- package, linked by a foreign key to exercise the COLS-configured FK path:
--
--   T_DATGEN_TOY_PARENT  — all non-LOB types; NUMBER PK  (MAX+N strategy)
--   T_DATGEN_TOY_CHILD   — LOB types + FK;    VARCHAR2(32) PK (SYS_GUID strategy)
--
-- Both object names match the LIKE '%DATGEN_TOY%' pattern used by 01_cleanup.sql,
-- so they are removed automatically when the full object set is torn down.
--
-- PK generation strategies exercised:
--   NUMBER             → MAX(col) + row_index          (T_DATGEN_TOY_PARENT.ID)
--   VARCHAR2 >= 32     → RAWTOHEX(SYS_GUID())          (T_DATGEN_TOY_CHILD.ID)
--   VARCHAR2 < 32 (PK) → sequential numeric string     (not covered here; add a
--                         table with a VARCHAR2(<32) PK to test this path)
--   RAW        (PK)    → SYS_GUID() as raw bytes       (not covered here; add a
--                         table with a RAW primary key to test this path)
--
-- Data-type notes:
--   INTEGER, INT, SMALLINT — Oracle stores these as data_type = 'NUMBER' in
--     USER_TAB_COLS (they are numeric subtypes, not separate types).  Columns
--     declared with these names are therefore handled by the 'NUMBER' branch in
--     f_fill_tables, never by the 'INTEGER', 'INT', or 'SMALLINT' branches.
--     They are included here for completeness.
--   FLOAT — stored as data_type = 'FLOAT' in USER_TAB_COLS; handled by the
--     'FLOAT' branch (same f_random_number path, different ELSIF label).
--   TIMESTAMP WITH TIME ZONE / WITH LOCAL TIME ZONE — matched by the
--     LIKE 'TIMESTAMP%' branch; TO_TIMESTAMP result is accepted by Oracle with
--     the session timezone applied implicitly.
--
-- Objects created:
--   T_DATGEN_TOY_PARENT           table
--   PK_DATGEN_TOY_PARENT          primary key constraint
--   T_DATGEN_TOY_CHILD            table
--   PK_DATGEN_TOY_CHILD           primary key constraint
--   FK_DATGEN_TOY_CHILD_PARENT    foreign key constraint
--
-- T_DATGEN_TOY_CMD rows:
--   T_DATGEN_TOY_PARENT  LVL=10  (populated first — child FK references its rows)
--   T_DATGEN_TOY_CHILD   LVL=20  (populated second)
--
-- T_DATGEN_TOY_COLS rows:
--   T_DATGEN_TOY_CHILD.PARENT_ID  FK → random ID from T_DATGEN_TOY_PARENT
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Parent table: all non-LOB column types handled by f_fill_tables.
-- Primary key: NUMBER → auto-handled by the MAX+N strategy.
-- -----------------------------------------------------------------------------
CREATE TABLE T_DATGEN_TOY_PARENT (

    -- Primary key — NUMBER type; f_fill_tables uses MAX(ID) + row_index.
    ID                    NUMBER(10)                    NOT NULL,

    -- -------------------------------------------------------------------------
    -- String types
    -- -------------------------------------------------------------------------

    -- VARCHAR2 with length >= 32: f_random_string generates a random-length
    -- uppercase alphanumeric string between 1 and DATA_LENGTH characters.
    COL_VARCHAR2_LONG     VARCHAR2(200),

    -- VARCHAR2 with length < 32: same random-string path, shorter capacity.
    -- Note: if this were the PK column, the package would use sequential numeric
    -- strings instead of SYS_GUID() (which would exceed the column width).
    COL_VARCHAR2_SHORT    VARCHAR2(20),

    COL_CHAR              CHAR(10),
    COL_NVARCHAR2         NVARCHAR2(100),
    COL_NCHAR             NCHAR(5)                      NOT NULL,

    -- -------------------------------------------------------------------------
    -- Exact numeric types
    -- -------------------------------------------------------------------------

    -- NUMBER with explicit precision and scale.
    COL_NUMBER            NUMBER(12, 2)                 NOT NULL,

    -- INTEGER, INT, SMALLINT are Oracle subtypes of NUMBER.
    -- USER_TAB_COLS reports data_type = 'NUMBER' for all three; the package
    -- handles them via the 'NUMBER' branch (precision=38, scale=0).
    COL_INTEGER           INTEGER,
    COL_SMALLINT          SMALLINT,

    -- FLOAT is stored as data_type = 'FLOAT' in USER_TAB_COLS and is handled
    -- by the explicit 'FLOAT' ELSIF branch in f_fill_tables.
    COL_FLOAT             FLOAT(24),

    -- -------------------------------------------------------------------------
    -- IEEE 754 binary floating-point types
    -- -------------------------------------------------------------------------
    COL_BINARY_FLOAT      BINARY_FLOAT                  NOT NULL,
    COL_BINARY_DOUBLE     BINARY_DOUBLE,

    -- -------------------------------------------------------------------------
    -- Date and timestamp types
    -- -------------------------------------------------------------------------
    COL_DATE              DATE                          NOT NULL,

    -- Plain TIMESTAMP (no time zone).
    COL_TIMESTAMP         TIMESTAMP(6),

    -- TIMESTAMP WITH TIME ZONE — matched by LIKE 'TIMESTAMP%'; Oracle applies
    -- the session timezone to the TO_TIMESTAMP literal during insert.
    COL_TIMESTAMP_TZ      TIMESTAMP(3) WITH TIME ZONE,

    -- TIMESTAMP WITH LOCAL TIME ZONE — same LIKE 'TIMESTAMP%' branch; stored
    -- normalised to the database timezone, displayed in session timezone.
    COL_TIMESTAMP_LTZ     TIMESTAMP(3) WITH LOCAL TIME ZONE,

    -- -------------------------------------------------------------------------
    -- Binary type
    -- -------------------------------------------------------------------------
    COL_RAW               RAW(100),

    CONSTRAINT PK_DATGEN_TOY_PARENT PRIMARY KEY (ID)
);


-- -----------------------------------------------------------------------------
-- Child table: LOB types + FK back to the parent table.
-- Primary key: VARCHAR2(32) → auto-handled by RAWTOHEX(SYS_GUID()) strategy.
-- PARENT_ID: FK column — value supplied via T_DATGEN_TOY_COLS (see below).
-- LOB_SIZE_KB controls the approximate size of each BLOB/CLOB/NCLOB value;
-- the package inserts EMPTY_BLOB()/EMPTY_CLOB() placeholders and then
-- populates content via RETURNING ROWID + UPDATE :bind_var.
-- -----------------------------------------------------------------------------
CREATE TABLE T_DATGEN_TOY_CHILD (

    -- Primary key — VARCHAR2(32); f_fill_tables uses RAWTOHEX(SYS_GUID()).
    ID            VARCHAR2(32)                          NOT NULL,

    -- Foreign key to parent — value supplied by T_DATGEN_TOY_COLS FILLING_STRING.
    PARENT_ID     NUMBER(10)                            NOT NULL,

    -- -------------------------------------------------------------------------
    -- LOB types
    -- -------------------------------------------------------------------------
    COL_BLOB      BLOB,
    COL_CLOB      CLOB,
    COL_NCLOB     NCLOB,

    -- -------------------------------------------------------------------------
    -- Additional regular columns for a realistic mixed-type child row
    -- -------------------------------------------------------------------------
    COL_VARCHAR2  VARCHAR2(200),
    COL_NUMBER    NUMBER(8),
    COL_DATE      DATE,

    CONSTRAINT PK_DATGEN_TOY_CHILD
        PRIMARY KEY (ID),

    CONSTRAINT FK_DATGEN_TOY_CHILD_PARENT
        FOREIGN KEY (PARENT_ID)
        REFERENCES T_DATGEN_TOY_PARENT (ID)
);


-- =============================================================================
-- PKG_DATGEN_TOY command configuration
-- =============================================================================

-- Parent table — processed first (LVL=10) so its rows exist when the child run
-- tries to pick a random PARENT_ID via the FK FILLING_STRING.
-- USE_APPEND=1: no LOB columns, so direct-path insert is active.  The package
-- commits after every row when APPEND is on (ORA-12839 prevention), which
-- effectively makes COMMIT_EVERY irrelevant for this table but causes no harm.
INSERT INTO T_DATGEN_TOY_CMD (
    TABLE_NAME, LVL, ROW_AMOUNT, MAX_ROWS,
    COMMIT_EVERY, LOB_SIZE_KB,
    DATE_FROM,       DATE_TO,
    USE_APPEND
) VALUES (
    'T_DATGEN_TOY_PARENT', 10, 100, 1000,
    1000, 10,
    DATE '2010-01-01', DATE '2024-12-31',
    1
);

-- Child table — processed second (LVL=20).
-- COMMIT_EVERY=100: LOB content inflates each UNDO entry; lower batch size
-- keeps UNDO tablespace pressure manageable.
-- LOB_SIZE_KB=5: small LOBs speed up the test run.
-- USE_APPEND=0: the package would override it to 0 anyway because the table
-- has LOB columns (direct-path inserts are incompatible with RETURNING ROWID).
INSERT INTO T_DATGEN_TOY_CMD (
    TABLE_NAME, LVL, ROW_AMOUNT, MAX_ROWS,
    COMMIT_EVERY, LOB_SIZE_KB,
    DATE_FROM,       DATE_TO,
    USE_APPEND
) VALUES (
    'T_DATGEN_TOY_CHILD', 20, 100, 1000,
    100, 5,
    DATE '2010-01-01', DATE '2024-12-31',
    0
);


-- =============================================================================
-- PKG_DATGEN_TOY column configuration
-- =============================================================================

-- PARENT_ID is a foreign key: the package must insert a value that already
-- exists in T_DATGEN_TOY_PARENT.ID.  The FILLING_STRING selects a random
-- parent ID at row-generation time.  The result is a NUMBER value so no
-- quoting is needed in the VALUES clause.
INSERT INTO T_DATGEN_TOY_COLS (
    TABLE_NAME,
    COLUMN_NAME,
    FILLING_STRING,
    CONSTRAINT_TYPE
) VALUES (
    'T_DATGEN_TOY_CHILD',
    'PARENT_ID',
    'SELECT ID FROM T_DATGEN_TOY_PARENT ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROW ONLY',
    'FK'
);

COMMIT;




-- =============================================================================
-- PKG_DATGEN_TOY test run, and Selects to check result
-- =============================================================================


begin 
    PKG_DATGEN_TOY.p_run;
end;
/
select * from T_DATGEN_TOY_PARENT;
select * from T_DATGEN_TOY_CHILD;
select * from T_DATGEN_TOY_CMD;
select * from T_DATGEN_TOY_COLS;
select * from T_DATGEN_TOY_LOG order by 1 desc;


