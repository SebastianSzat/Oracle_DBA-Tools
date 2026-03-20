-- =============================================================================
-- 05_PKG_DATGEN_TOY.sql
-- Data-generation package: specification and body.
--
-- Public entry points:
--   PKG_DATGEN_TOY.p_run                        -- process all enabled tables
--   PKG_DATGEN_TOY.p_run(in_table_name)         -- process one specific table
--   PKG_DATGEN_TOY.f_fill_tables(in_table_name) -- same as p_run; returns row count
--   PKG_DATGEN_TOY.p_enable(in_table_name)      -- set IF_GENERATE = 1 for one table
--   PKG_DATGEN_TOY.p_enable_all                 -- set IF_GENERATE = 1 for all tables
--   PKG_DATGEN_TOY.p_reset(in_table_name)       -- alias for p_enable (reset for re-run)
--   PKG_DATGEN_TOY.p_analyze(in_table_name)     -- dry-run analysis; logs findings only
--
-- The package reads T_DATGEN_TOY_CMD for all rows where IF_GENERATE = 1,
-- processes each target table in ascending LVL order, inserts the configured
-- number of rows of random data, and sets IF_GENERATE = 0 on completion.
-- All activity is logged to T_DATGEN_TOY_LOG.
--
-- Random-value helper functions are also available for direct use.
-- =============================================================================


-- =============================================================================
-- PACKAGE SPECIFICATION
-- =============================================================================
CREATE OR REPLACE PACKAGE PKG_DATGEN_TOY AS

    /* -------------------------------------------------------------------------
       f_log
       Write one entry to T_DATGEN_TOY_LOG and return the message text.
       The COMMIT inside f_log ensures each entry survives a caller rollback.
       Assign the return value to v_log_discard when the caller does not need
       the text but still wants the log entry committed.
    ------------------------------------------------------------------------- */
    FUNCTION f_log (
        i_run_id     NUMBER,
        i_table_name VARCHAR2,
        i_step       VARCHAR2,
        i_info       VARCHAR2
    ) RETURN VARCHAR2;

    /* -------------------------------------------------------------------------
       f_random_string
       Return a random uppercase alphanumeric string.  Actual length is chosen
       randomly between 1 and i_length to produce realistic mixed-length data
       rather than always filling the column to capacity.  Capped at 4000 chars.
    ------------------------------------------------------------------------- */
    FUNCTION f_random_string (i_length NUMBER) RETURN VARCHAR2;

    /* -------------------------------------------------------------------------
       f_random_number
       Return a random NUMBER with i_precision total digits and i_scale
       fractional digits.  NULL inputs default to precision=10, scale=0.
    ------------------------------------------------------------------------- */
    FUNCTION f_random_number (i_precision NUMBER, i_scale NUMBER) RETURN NUMBER;

    /* -------------------------------------------------------------------------
       f_random_date
       Return a random DATE uniformly distributed between i_from and i_to.
    ------------------------------------------------------------------------- */
    FUNCTION f_random_date (
        i_from DATE DEFAULT DATE '2000-01-01',
        i_to   DATE DEFAULT DATE '2030-12-31'
    ) RETURN DATE;

    /* -------------------------------------------------------------------------
       f_random_timestamp
       Return a random TIMESTAMP uniformly distributed between i_from and i_to.
    ------------------------------------------------------------------------- */
    FUNCTION f_random_timestamp (
        i_from DATE DEFAULT DATE '2000-01-01',
        i_to   DATE DEFAULT DATE '2030-12-31'
    ) RETURN TIMESTAMP;

    /* -------------------------------------------------------------------------
       f_random_raw
       Return a RAW value of i_length bytes (capped at 2000) filled with random
       content.  Uses SYS_GUID() internally — DBMS_RANDOM.STRING('X') is
       intentionally avoided because it produces G-Z characters that are not
       valid hex digits and cause ORA-06502 in HEXTORAW.
    ------------------------------------------------------------------------- */
    FUNCTION f_random_raw (i_length NUMBER) RETURN RAW;

    /* -------------------------------------------------------------------------
       f_random_binary_float
       Return a random BINARY_FLOAT value in the range [0, 1).
    ------------------------------------------------------------------------- */
    FUNCTION f_random_binary_float RETURN BINARY_FLOAT;

    /* -------------------------------------------------------------------------
       f_random_binary_double
       Return a random BINARY_DOUBLE value in the range [0, 1).
    ------------------------------------------------------------------------- */
    FUNCTION f_random_binary_double RETURN BINARY_DOUBLE;

    /* -------------------------------------------------------------------------
       f_random_clob
       Return a temporary CLOB of approximately (i_times * 2000) characters of
       random uppercase alphanumeric content.  Caller is responsible for freeing
       the temporary LOB when done via DBMS_LOB.FREETEMPORARY.
    ------------------------------------------------------------------------- */
    FUNCTION f_random_clob (i_times NUMBER) RETURN CLOB;

    /* -------------------------------------------------------------------------
       f_random_blob
       Return a temporary BLOB of approximately (i_times * 2000) bytes of
       random content, converted from a CLOB via DBMS_LOB.CONVERTTOBLOB.
       Caller is responsible for freeing the temporary LOB when done.
    ------------------------------------------------------------------------- */
    FUNCTION f_random_blob (i_times NUMBER) RETURN BLOB;

    /* -------------------------------------------------------------------------
       f_fill_tables
       Main entry point.  Reads T_DATGEN_TOY_CMD (IF_GENERATE = 1), fills each
       target table with random data, then sets IF_GENERATE = 0 on success.
       in_table_name : when provided, process only that table; NULL = all enabled.
       Returns total rows inserted across all processed tables; 0 on fatal error.
    ------------------------------------------------------------------------- */
    FUNCTION f_fill_tables (in_table_name VARCHAR2 DEFAULT NULL) RETURN NUMBER;

    /* -------------------------------------------------------------------------
       p_run  (no arguments — process all enabled tables)
       Wrapper around f_fill_tables that echoes progress to DBMS_OUTPUT.
    ------------------------------------------------------------------------- */
    PROCEDURE p_run;

    /* -------------------------------------------------------------------------
       p_run  (one argument — process a single named table)
    ------------------------------------------------------------------------- */
    PROCEDURE p_run (in_table_name VARCHAR2);

    /* -------------------------------------------------------------------------
       p_enable_all
       Sets IF_GENERATE = 1 for every row in T_DATGEN_TOY_CMD so all tables
       will be processed on the next p_run / f_fill_tables call.
    ------------------------------------------------------------------------- */
    PROCEDURE p_enable_all;

    /* -------------------------------------------------------------------------
       p_enable
       Sets IF_GENERATE = 1 for a single named table.
    ------------------------------------------------------------------------- */
    PROCEDURE p_enable (in_table_name VARCHAR2);

    /* -------------------------------------------------------------------------
       p_reset
       Alias for p_enable — re-enables a previously completed table for another
       generation pass after IF_GENERATE was set to 0 by a successful run.
    ------------------------------------------------------------------------- */
    PROCEDURE p_reset (in_table_name VARCHAR2);

    /* -------------------------------------------------------------------------
       p_analyze
       Dry-run pre-analysis for one table.  Logs what f_fill_tables would do —
       PK columns and their auto-detection strategy, COLS overrides, regular
       columns by type, NOT NULL / unsupported-type warnings, composite PK
       notices — without inserting any data.  Useful for validating configuration
       before committing to a real run.
    ------------------------------------------------------------------------- */
    PROCEDURE p_analyze (in_table_name VARCHAR2);

END PKG_DATGEN_TOY;
/


-- =============================================================================
-- PACKAGE BODY
-- =============================================================================
CREATE OR REPLACE PACKAGE BODY PKG_DATGEN_TOY AS

    /* -------------------------------------------------------------------------
       Package-level variables — kept to the absolute minimum.
       Only state that helper functions need to share with f_fill_tables is
       declared here; everything else is local to f_fill_tables to prevent
       stale state between calls in the same session.
    ------------------------------------------------------------------------- */

    -- Identifier for the current f_fill_tables / p_analyze invocation.
    -- Set once at run start; used by helper-function exception handlers so
    -- they can log errors without receiving run_id as a parameter.
    v_run_id      NUMBER(12);

    -- Discard variable for f_log return values when the caller does not need
    -- the text but must capture the return to satisfy PL/SQL assignment rules.
    -- Intentionally never read after assignment.
    v_log_discard VARCHAR2(4000);


    /* =========================================================================
       f_log
    ========================================================================= */
    FUNCTION f_log (
        i_run_id     NUMBER,
        i_table_name VARCHAR2,
        i_step       VARCHAR2,
        i_info       VARCHAR2
    ) RETURN VARCHAR2 IS
    BEGIN
        INSERT INTO T_DATGEN_TOY_LOG (run_id, table_name, step, status_info)
        VALUES (i_run_id, i_table_name, i_step, i_info);
        -- Immediate commit: this entry survives even if the caller rolls back.
        COMMIT;
        RETURN i_info;
    END f_log;


    /* =========================================================================
       f_random_string
    ========================================================================= */
    FUNCTION f_random_string (i_length NUMBER) RETURN VARCHAR2 IS
        v_cap     PLS_INTEGER := LEAST(NVL(i_length, 1), 4000);
        -- Random length between 1 and v_cap for realistic mixed-length test data.
        v_actual  PLS_INTEGER := TRUNC(DBMS_RANDOM.VALUE(1, v_cap + 1));
        v_result  VARCHAR2(4000);
    BEGIN
        v_result := DBMS_RANDOM.STRING('X', v_actual);
        RETURN v_result;
    EXCEPTION
        WHEN OTHERS THEN
            v_log_discard := f_log(v_run_id, NULL, 'f_random_string',
                                   'Unhandled error: ' || SQLERRM);
            RETURN NULL;
    END f_random_string;


    /* =========================================================================
       f_random_number
    ========================================================================= */
    FUNCTION f_random_number (i_precision NUMBER, i_scale NUMBER) RETURN NUMBER IS
        v_precision  NUMBER := NVL(i_precision, 10);
        v_scale      NUMBER := NVL(i_scale, 0);
        v_upper      NUMBER;
        v_result     NUMBER;
    BEGIN
        -- Upper bound = 10 ^ (integer digits); TRUNC gives the right scale shape.
        v_upper  := POWER(10, v_precision - v_scale);
        v_result := TRUNC(DBMS_RANDOM.VALUE(0, v_upper), v_scale);
        RETURN v_result;
    EXCEPTION
        WHEN OTHERS THEN
            v_log_discard := f_log(v_run_id, NULL, 'f_random_number',
                                   'Unhandled error: ' || SQLERRM);
            RETURN NULL;
    END f_random_number;


    /* =========================================================================
       f_random_date
    ========================================================================= */
    FUNCTION f_random_date (
        i_from DATE DEFAULT DATE '2000-01-01',
        i_to   DATE DEFAULT DATE '2030-12-31'
    ) RETURN DATE IS
        v_j_min  NUMBER := TO_NUMBER(TO_CHAR(NVL(i_from, DATE '2000-01-01'), 'J'));
        -- +1 so the upper bound is inclusive (DBMS_RANDOM.VALUE upper is exclusive).
        v_j_max  NUMBER := TO_NUMBER(TO_CHAR(NVL(i_to,   DATE '2030-12-31'), 'J')) + 1;
        v_result DATE;
    BEGIN
        v_result := TO_DATE(TRUNC(DBMS_RANDOM.VALUE(v_j_min, v_j_max)), 'J');
        RETURN v_result;
    EXCEPTION
        WHEN OTHERS THEN
            v_log_discard := f_log(v_run_id, NULL, 'f_random_date',
                                   'Unhandled error: ' || SQLERRM);
            RETURN NULL;
    END f_random_date;


    /* =========================================================================
       f_random_timestamp
    ========================================================================= */
    FUNCTION f_random_timestamp (
        i_from DATE DEFAULT DATE '2000-01-01',
        i_to   DATE DEFAULT DATE '2030-12-31'
    ) RETURN TIMESTAMP IS
        v_from        DATE      := NVL(i_from, DATE '2000-01-01');
        v_to          DATE      := NVL(i_to,   DATE '2030-12-31');
        -- +1 day in seconds so the full final day is included in the range.
        v_range_secs  NUMBER    := (v_to - v_from + 1) * 86400;
        v_result      TIMESTAMP;
    BEGIN
        v_result := CAST(v_from AS TIMESTAMP)
                    + NUMTODSINTERVAL(DBMS_RANDOM.VALUE(0, v_range_secs), 'SECOND');
        RETURN v_result;
    EXCEPTION
        WHEN OTHERS THEN
            v_log_discard := f_log(v_run_id, NULL, 'f_random_timestamp',
                                   'Unhandled error: ' || SQLERRM);
            RETURN NULL;
    END f_random_timestamp;


    /* =========================================================================
       f_random_raw
       Builds a hex string from SYS_GUID() calls and converts via HEXTORAW.
       Each SYS_GUID() yields 16 bytes = 32 hex characters.
       DBMS_RANDOM.STRING('X') would include G-Z (not valid hex) and cause
       ORA-06502 when passed to HEXTORAW — that approach is not used here.
    ========================================================================= */
    FUNCTION f_random_raw (i_length NUMBER) RETURN RAW IS
        v_target  PLS_INTEGER  := LEAST(NVL(i_length, 1), 2000);
        v_hex     VARCHAR2(4001);
    BEGIN
        -- NVL guards against Oracle treating an uninitialised VARCHAR2 as NULL,
        -- which would make LENGTH(v_hex) = NULL and the WHILE condition never TRUE.
        WHILE NVL(LENGTH(v_hex), 0) < v_target * 2 LOOP
            v_hex := v_hex || RAWTOHEX(SYS_GUID());
        END LOOP;
        RETURN HEXTORAW(SUBSTR(v_hex, 1, v_target * 2));
    EXCEPTION
        WHEN OTHERS THEN
            v_log_discard := f_log(v_run_id, NULL, 'f_random_raw',
                                   'Unhandled error: ' || SQLERRM);
            RETURN NULL;
    END f_random_raw;


    /* =========================================================================
       f_random_binary_float
    ========================================================================= */
    FUNCTION f_random_binary_float RETURN BINARY_FLOAT IS
    BEGIN
        RETURN TO_BINARY_FLOAT(DBMS_RANDOM.VALUE);
    EXCEPTION
        WHEN OTHERS THEN
            v_log_discard := f_log(v_run_id, NULL, 'f_random_binary_float',
                                   'Unhandled error: ' || SQLERRM);
            RETURN NULL;
    END f_random_binary_float;


    /* =========================================================================
       f_random_binary_double
    ========================================================================= */
    FUNCTION f_random_binary_double RETURN BINARY_DOUBLE IS
    BEGIN
        RETURN TO_BINARY_DOUBLE(DBMS_RANDOM.VALUE);
    EXCEPTION
        WHEN OTHERS THEN
            v_log_discard := f_log(v_run_id, NULL, 'f_random_binary_double',
                                   'Unhandled error: ' || SQLERRM);
            RETURN NULL;
    END f_random_binary_double;


    /* =========================================================================
       f_random_clob
    ========================================================================= */
    FUNCTION f_random_clob (i_times NUMBER) RETURN CLOB IS
        v_result  CLOB;
        v_chunk   VARCHAR2(2000);
    BEGIN
        DBMS_LOB.CREATETEMPORARY(v_result, FALSE);
        FOR idx IN 1..i_times LOOP
            v_chunk := DBMS_RANDOM.STRING('X', 2000);
            DBMS_LOB.WRITEAPPEND(v_result, LENGTH(v_chunk), v_chunk);
        END LOOP;
        RETURN v_result;
    EXCEPTION
        WHEN OTHERS THEN
            IF v_result IS NOT NULL THEN
                DBMS_LOB.FREETEMPORARY(v_result);
            END IF;
            v_log_discard := f_log(v_run_id, NULL, 'f_random_clob',
                                   'Unhandled error: ' || SQLERRM);
            RETURN NULL;
    END f_random_clob;


    /* =========================================================================
       f_random_blob
       Builds content via f_random_clob (CREATETEMPORARY + WRITEAPPEND pattern),
       then converts to BLOB via DBMS_LOB.CONVERTTOBLOB.  The intermediate
       temporary CLOB is freed immediately after conversion.
       The old approach of concatenating v_clob || DBMS_RANDOM.STRING(...) in a
       loop caused implicit PGA conversions on every iteration and never freed
       the intermediate CLOB — that approach is not used here.
    ========================================================================= */
    FUNCTION f_random_blob (i_times NUMBER) RETURN BLOB IS
        v_clob        CLOB;
        v_result      BLOB;
        v_dest_offset INTEGER := 1;
        v_src_offset  INTEGER := 1;
        v_warn        INTEGER;
        v_ctx         INTEGER := DBMS_LOB.DEFAULT_LANG_CTX;
    BEGIN
        v_clob := f_random_clob(i_times);
        DBMS_LOB.CREATETEMPORARY(v_result, FALSE);
        DBMS_LOB.CONVERTTOBLOB(
            v_result,
            v_clob,
            DBMS_LOB.LOBMAXSIZE,
            v_dest_offset,
            v_src_offset,
            DBMS_LOB.DEFAULT_CSID,
            v_ctx,
            v_warn
        );
        -- Free the intermediate CLOB; the returned BLOB belongs to the caller.
        DBMS_LOB.FREETEMPORARY(v_clob);
        RETURN v_result;
    EXCEPTION
        WHEN OTHERS THEN
            -- Free both temporaries if they were created before the error.
            IF v_clob   IS NOT NULL THEN DBMS_LOB.FREETEMPORARY(v_clob);   END IF;
            IF v_result IS NOT NULL THEN DBMS_LOB.FREETEMPORARY(v_result); END IF;
            v_log_discard := f_log(v_run_id, NULL, 'f_random_blob',
                                   'Unhandled error: ' || SQLERRM);
            RETURN NULL;
    END f_random_blob;


    /* =========================================================================
       f_fill_tables
    ========================================================================= */
    FUNCTION f_fill_tables (in_table_name VARCHAR2 DEFAULT NULL) RETURN NUMBER IS

        /* -----------------------------------------------------------------
           Row counters
        ----------------------------------------------------------------- */
        v_actual_rows   NUMBER(10);        -- Current row count of the target table
        v_rows_needed   NUMBER(10);        -- Rows to insert in this run
        v_all_rows      NUMBER(10) := 0;   -- Cumulative total across all tables

        /* -----------------------------------------------------------------
           Dynamic SQL string buffers
           VARCHAR2(32767) is the PL/SQL maximum; avoids truncation for wide
           tables with many columns.
        ----------------------------------------------------------------- */
        v_stmt          VARCHAR2(32767);   -- Full INSERT statement
        v_columns       VARCHAR2(32767);   -- Column list  (comma-separated)
        v_values        VARCHAR2(32767);   -- Values list  (comma-separated)

        /* -----------------------------------------------------------------
           Staging variables for generated values
        ----------------------------------------------------------------- */
        v_random_str    VARCHAR2(4000);    -- Generated string value
        v_random_num    NUMBER;            -- Generated number value
        v_random_dt     DATE;              -- Generated date value
        v_random_ts     TIMESTAMP;         -- Generated timestamp value
        v_filling_str   VARCHAR2(4000);    -- Value returned by a FILLING_STRING query

        /* -----------------------------------------------------------------
           LOB handling
           BLOB and CLOB values cannot be embedded as literals in dynamic SQL.
           Strategy:
             1. INSERT the row with EMPTY_BLOB() / EMPTY_CLOB() placeholders.
             2. Capture the new row's ROWID via the RETURNING clause.
             3. UPDATE each LOB column separately using a bind variable (:1)
                so Oracle handles the LOB locator correctly.
             4. Free each temporary LOB immediately after the UPDATE.
        ----------------------------------------------------------------- */
        TYPE t_name_list IS TABLE OF VARCHAR2(64) INDEX BY PLS_INTEGER;
        v_blob_cols     t_name_list;       -- BLOB column names for this row
        v_clob_cols     t_name_list;       -- CLOB/NCLOB column names for this row
        v_blob_count    PLS_INTEGER;       -- Number of BLOB columns in current row
        v_clob_count    PLS_INTEGER;       -- Number of CLOB/NCLOB columns in current row
        v_row_has_lob   BOOLEAN;           -- TRUE if the current row has any LOB
        v_table_has_lob BOOLEAN;           -- TRUE if the table has any LOB (pre-analysis)
        v_rowid         ROWID;             -- ROWID of the just-inserted row
        v_tmp_blob      BLOB;              -- Staging variable for LOB UPDATE + FREETEMPORARY
        v_tmp_clob      CLOB;              -- Staging variable for LOB UPDATE + FREETEMPORARY

        /* -----------------------------------------------------------------
           LOB size
        ----------------------------------------------------------------- */
        v_lob_cycle     NUMBER(10);        -- Iteration count for f_random_blob/clob
                                           -- Each iteration appends ~2000 bytes/chars

        /* -----------------------------------------------------------------
           Direct-path insert
        ----------------------------------------------------------------- */
        v_use_append    BOOLEAN;           -- TRUE when APPEND hint is active
                                           -- Auto-disabled when table has LOB columns

        /* -----------------------------------------------------------------
           PK auto-detection
           PK columns not listed in T_DATGEN_TOY_COLS are handled automatically:
             NUMBER                              → MAX(col) + row_index
             VARCHAR2/CHAR >= 32 chars           → SYS_GUID() as hex string
             VARCHAR2/CHAR <  32 chars           → COUNT(*) offset + row_index
             RAW                                 → SYS_GUID() as raw bytes
             Other                               → NULL (warned before insert loop)
           The numeric base (MAX or COUNT) is queried once and cached here.
        ----------------------------------------------------------------- */
        TYPE t_pk_max_tab IS TABLE OF NUMBER INDEX BY VARCHAR2(64);
        v_pk_max_tab    t_pk_max_tab;      -- Numeric base cache; keyed by column name
        v_pk_max        NUMBER;            -- Temporary for the MAX / COUNT query
        v_pk_col_count  NUMBER(10);        -- Total PK columns (for composite notice)

        /* -----------------------------------------------------------------
           Pre-analysis column caches
           All three column sets (auto-PK, COLS-configured, regular) are
           collected into arrays once per table before the insert loop to
           avoid re-executing the same cursor queries on every row iteration.
        ----------------------------------------------------------------- */

        -- Auto-handled PK columns (not overridden in T_DATGEN_TOY_COLS)
        TYPE t_pk_rec IS RECORD (
            col_name    VARCHAR2(64),
            data_type   VARCHAR2(64),
            data_length NUMBER
        );
        TYPE t_pk_tab IS TABLE OF t_pk_rec INDEX BY PLS_INTEGER;
        v_auto_pk_cols  t_pk_tab;
        v_auto_pk_cnt   PLS_INTEGER;

        -- COLS-configured columns (FK, UI, custom PK, LOB expressions)
        TYPE t_cfg_rec IS RECORD (
            col_name    VARCHAR2(64),
            fill_str    VARCHAR2(4000)
        );
        TYPE t_cfg_tab IS TABLE OF t_cfg_rec INDEX BY PLS_INTEGER;
        v_cfg_cols      t_cfg_tab;
        v_cfg_cnt       PLS_INTEGER;

        -- Regular columns (auto-detected by data type; not PK, not in COLS)
        TYPE t_reg_rec IS RECORD (
            col_name    VARCHAR2(64),
            data_type   VARCHAR2(64),
            data_length NUMBER,
            data_prec   NUMBER,
            data_scale  NUMBER,
            nullable    VARCHAR2(1)    -- 'Y' = nullable, 'N' = NOT NULL
        );
        TYPE t_reg_tab IS TABLE OF t_reg_rec INDEX BY PLS_INTEGER;
        v_reg_cols      t_reg_tab;
        v_reg_cnt       PLS_INTEGER;

        /* -----------------------------------------------------------------
           Miscellaneous
        ----------------------------------------------------------------- */
        v_log           VARCHAR2(4000);    -- Return buffer from f_log

    BEGIN

        /* ------------------------------------------------------------------
           Assign a new RUN_ID.  All log entries from this invocation share it:
             SELECT * FROM T_DATGEN_TOY_LOG WHERE run_id = <value> ORDER BY id;
        ------------------------------------------------------------------ */
        SELECT SEQ_DATGEN_TOY_LOG_ID.NEXTVAL INTO v_run_id FROM DUAL;
        v_log := f_log(v_run_id, NULL, 'f_fill_tables', 'Run started.');

        /* ==================================================================
           OUTER LOOP — one iteration per enabled target table.
           IF_GENERATE = 1 means "process on this run".
           When in_table_name is supplied, only that table is processed.
           Tables are ordered by LVL ascending so parent tables (holding the
           rows that child FK columns reference) are populated first.
        ================================================================== */
        FOR cmd IN (
            SELECT *
            FROM   T_DATGEN_TOY_CMD
            WHERE  IF_GENERATE = 1
              AND  (in_table_name IS NULL OR TABLE_NAME = UPPER(in_table_name))
            ORDER  BY LVL ASC
        ) LOOP

            /* ----------------------------------------------------------------
               Per-table error isolation.
               A failure on one table is caught here: uncommitted work for that
               table is rolled back, the error is logged, and the outer loop
               continues to the next table.  Other tables that already completed
               in this run are not affected.
            ---------------------------------------------------------------- */
            BEGIN -- per-table block

                v_log := f_log(v_run_id, cmd.TABLE_NAME, 'f_fill_tables',
                               'Processing table: ' || cmd.TABLE_NAME);

                /* ------------------------------------------------------------
                   Reset per-table caches.
                ------------------------------------------------------------ */
                v_auto_pk_cnt   := 0;
                v_cfg_cnt       := 0;
                v_reg_cnt       := 0;
                v_table_has_lob := FALSE;
                v_pk_max_tab.DELETE;
                v_auto_pk_cols.DELETE;
                v_cfg_cols.DELETE;
                v_reg_cols.DELETE;

                /* ------------------------------------------------------------
                   LOB cycle count from the per-table CMD setting.
                   Each f_random_clob / f_random_blob cycle appends ~2000 bytes.
                ------------------------------------------------------------ */
                v_lob_cycle := GREATEST(1, ROUND(cmd.LOB_SIZE_KB * 1024 / 2000));

                /* ------------------------------------------------------------
                   Determine how many rows to insert this run.
                ------------------------------------------------------------ */
                EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || cmd.TABLE_NAME
                    INTO v_actual_rows;

                IF v_actual_rows >= cmd.MAX_ROWS THEN
                    v_rows_needed := 0;
                    v_log := f_log(v_run_id, cmd.TABLE_NAME, 'f_fill_tables',
                                   'Table is already at or above MAX_ROWS ('
                                   || cmd.MAX_ROWS || '). Skipping.');
                ELSIF (v_actual_rows + cmd.ROW_AMOUNT) > cmd.MAX_ROWS THEN
                    v_rows_needed := cmd.MAX_ROWS - v_actual_rows;
                    v_log := f_log(v_run_id, cmd.TABLE_NAME, 'f_fill_tables',
                                   'ROW_AMOUNT would exceed MAX_ROWS; '
                                   || 'capping insert at ' || v_rows_needed || ' rows.');
                ELSE
                    v_rows_needed := cmd.ROW_AMOUNT;
                    v_log := f_log(v_run_id, cmd.TABLE_NAME, 'f_fill_tables',
                                   'Inserting ' || v_rows_needed || ' rows.');
                END IF;

                /* ------------------------------------------------------------
                   Skip all pre-analysis and inserts when nothing is to be done.
                   This avoids expensive MAX / COUNT queries on tables that are
                   already at their MAX_ROWS ceiling.
                ------------------------------------------------------------ */
                IF v_rows_needed > 0 THEN

                /* ------------------------------------------------------------
                   PK PRE-ANALYSIS
                   1. Count PK columns; log a notice for composite PKs.
                   2. Collect auto-handled PK columns (no COLS override) into cache.
                   3. Query MAX(col) for NUMBER PKs; COUNT(*) for short VARCHAR2 PKs.
                ------------------------------------------------------------ */
                SELECT COUNT(*)
                INTO   v_pk_col_count
                FROM   user_cons_columns ucc
                JOIN   user_constraints  uc ON ucc.constraint_name = uc.constraint_name
                WHERE  uc.constraint_type = 'P'
                  AND  uc.table_name      = cmd.TABLE_NAME;

                IF v_pk_col_count > 1 THEN
                    v_log := f_log(v_run_id, cmd.TABLE_NAME, 'f_fill_tables',
                                   'INFO: Composite PK detected ('
                                   || v_pk_col_count
                                   || ' columns). Each column is handled '
                                   || 'independently by its data type.');
                END IF;

                FOR pk_col IN (
                    SELECT ucc.column_name,
                           utc.data_type,
                           -- NVARCHAR2/NCHAR: DATA_LENGTH is the byte limit
                           -- (e.g. 200 for NVARCHAR2(100) with AL16UTF16).
                           -- CHAR_LENGTH holds the declared character limit,
                           -- which is what f_random_string and the < 32 check
                           -- must use to avoid ORA-12899.
                           CASE WHEN utc.data_type IN ('NVARCHAR2','NCHAR')
                                THEN utc.char_length
                                ELSE utc.data_length
                           END AS data_length
                    FROM   user_cons_columns ucc
                    JOIN   user_constraints  uc
                           ON  ucc.constraint_name = uc.constraint_name
                    JOIN   user_tab_cols     utc
                           ON  utc.table_name  = ucc.table_name
                           AND utc.column_name = ucc.column_name
                    WHERE  uc.constraint_type = 'P'
                      AND  uc.table_name      = cmd.TABLE_NAME
                      AND  NOT EXISTS (
                               SELECT 1 FROM T_DATGEN_TOY_COLS c
                               WHERE  c.table_name  = ucc.table_name
                                 AND  c.column_name = ucc.column_name
                           )
                ) LOOP
                    v_auto_pk_cnt := v_auto_pk_cnt + 1;
                    v_auto_pk_cols(v_auto_pk_cnt).col_name    := pk_col.column_name;
                    v_auto_pk_cols(v_auto_pk_cnt).data_type   := pk_col.data_type;
                    v_auto_pk_cols(v_auto_pk_cnt).data_length := pk_col.data_length;

                    IF pk_col.data_type = 'NUMBER' THEN
                        -- Cache MAX so the insert loop can use MAX + idx without
                        -- re-querying the table on every row.
                        EXECUTE IMMEDIATE
                            'SELECT NVL(MAX(' || pk_col.column_name || '), 0) FROM '
                            || cmd.TABLE_NAME
                            INTO v_pk_max;
                        v_pk_max_tab(pk_col.column_name) := v_pk_max;

                    ELSIF pk_col.data_type IN ('VARCHAR2','CHAR','NVARCHAR2','NCHAR')
                      AND pk_col.data_length < 32
                    THEN
                        -- Column is too short for a 32-char SYS_GUID().
                        -- Reuse the COUNT(*) already captured in v_actual_rows —
                        -- no rows have been inserted yet, so the values are identical.
                        v_pk_max_tab(pk_col.column_name) := v_actual_rows;
                        v_log := f_log(v_run_id, cmd.TABLE_NAME, 'f_fill_tables',
                                       'INFO: PK column ' || pk_col.column_name
                                       || ' length ' || pk_col.data_length
                                       || ' < 32 — SYS_GUID() too long; using '
                                       || 'sequential numeric string. Add a COLS '
                                       || 'entry to supply a custom expression.');

                    ELSIF pk_col.data_type NOT IN ('VARCHAR2','CHAR','NVARCHAR2','NCHAR','RAW')
                    THEN
                        -- Unsupported PK type — warn here in pre-analysis so the
                        -- problem is visible before any insert is attempted and so
                        -- p_analyze surfaces it correctly.  NULL will be inserted,
                        -- which will fail with ORA-01400 if the column is NOT NULL.
                        v_log := f_log(v_run_id, cmd.TABLE_NAME, 'f_fill_tables',
                                       'WARN: Auto-PK column ' || pk_col.column_name
                                       || ' has unsupported type ' || pk_col.data_type
                                       || '. NULL will be inserted — add a COLS entry '
                                       || 'to supply a custom expression.');
                    END IF;
                END LOOP;

                /* ------------------------------------------------------------
                   COLS PRE-ANALYSIS
                ------------------------------------------------------------ */
                FOR cfg_col IN (
                    SELECT column_name, filling_string
                    FROM   T_DATGEN_TOY_COLS
                    WHERE  table_name = cmd.TABLE_NAME
                ) LOOP
                    v_cfg_cnt := v_cfg_cnt + 1;
                    v_cfg_cols(v_cfg_cnt).col_name := cfg_col.column_name;
                    v_cfg_cols(v_cfg_cnt).fill_str := cfg_col.filling_string;
                END LOOP;

                /* ------------------------------------------------------------
                   REGULAR COLUMN PRE-ANALYSIS
                   Collect columns that are neither PK nor COLS-configured.
                   Virtual and hidden columns are excluded — they are not
                   insertable and would raise ORA-54013.
                   NOT NULL columns with unsupported types are flagged here
                   before the insert loop so the operator is warned early;
                   inserting NULL into them would cause ORA-01400 at runtime.
                ------------------------------------------------------------ */
                FOR reg_col IN (
                    SELECT utc.column_name,
                           utc.data_type,
                           -- NVARCHAR2/NCHAR: DATA_LENGTH is the byte limit;
                           -- CHAR_LENGTH is the declared character limit.
                           -- Use CHAR_LENGTH for N-types so f_random_string does
                           -- not generate more characters than the column holds.
                           CASE WHEN utc.data_type IN ('NVARCHAR2','NCHAR')
                                THEN utc.char_length
                                ELSE utc.data_length
                           END AS data_length,
                           utc.data_precision,
                           utc.data_scale,
                           utc.nullable
                    FROM   user_tab_cols utc
                    WHERE  utc.table_name      = cmd.TABLE_NAME
                      -- Exclude virtual and hidden columns (not insertable)
                      AND  utc.virtual_column  = 'NO'
                      AND  utc.hidden_column   = 'NO'
                      -- Exclude COLS-configured columns
                      AND  utc.column_name NOT IN (
                               SELECT c.column_name FROM T_DATGEN_TOY_COLS c
                               WHERE  c.table_name = cmd.TABLE_NAME
                           )
                      -- Exclude PK columns (handled in Step 1)
                      AND  utc.column_name NOT IN (
                               SELECT ucc.column_name
                               FROM   user_cons_columns ucc
                               JOIN   user_constraints  uc
                                      ON ucc.constraint_name = uc.constraint_name
                               WHERE  uc.constraint_type = 'P'
                                 AND  uc.table_name      = cmd.TABLE_NAME
                           )
                ) LOOP
                    v_reg_cnt := v_reg_cnt + 1;
                    v_reg_cols(v_reg_cnt).col_name   := reg_col.column_name;
                    v_reg_cols(v_reg_cnt).data_type  := reg_col.data_type;
                    v_reg_cols(v_reg_cnt).data_length:= reg_col.data_length;
                    v_reg_cols(v_reg_cnt).data_prec  := reg_col.data_precision;
                    v_reg_cols(v_reg_cnt).data_scale := reg_col.data_scale;
                    v_reg_cols(v_reg_cnt).nullable   := reg_col.nullable;

                    -- Set table-level LOB flag for the APPEND hint decision below.
                    IF reg_col.data_type IN ('BLOB','CLOB','NCLOB') THEN
                        v_table_has_lob := TRUE;
                    END IF;

                    -- Warn about unsupported-type columns before any inserts are
                    -- attempted.  NOT NULL columns will fail with ORA-01400; nullable
                    -- ones receive NULL silently.  Both are surfaced here so p_analyze
                    -- and the pre-run log show the full picture before the first row.
                    IF reg_col.data_type NOT IN (
                               'VARCHAR2','CHAR','NVARCHAR2','NCHAR',
                               'NUMBER','INTEGER','INT','SMALLINT','FLOAT',
                               'BINARY_FLOAT','BINARY_DOUBLE',
                               'DATE','RAW','BLOB','CLOB','NCLOB'
                           )
                       AND reg_col.data_type NOT LIKE 'TIMESTAMP%'
                    THEN
                        IF reg_col.nullable = 'N' THEN
                            v_log := f_log(v_run_id, cmd.TABLE_NAME, 'f_fill_tables',
                                           'WARN: Column ' || reg_col.column_name
                                           || ' type ' || reg_col.data_type
                                           || ' is NOT NULL and not supported — NULL will '
                                           || 'be inserted, causing ORA-01400. Add a COLS '
                                           || 'entry to supply a custom expression.');
                        ELSE
                            v_log := f_log(v_run_id, cmd.TABLE_NAME, 'f_fill_tables',
                                           'INFO: Column ' || reg_col.column_name
                                           || ' type ' || reg_col.data_type
                                           || ' is not supported — NULL will be inserted. '
                                           || 'Add a COLS entry if a specific value '
                                           || 'is needed.');
                        END IF;
                    END IF;
                END LOOP;

                /* ------------------------------------------------------------
                   APPEND hint decision.
                   USE_APPEND = 1 enables direct-path inserts using the APPEND
                   hint, which bypass the buffer cache and are significantly
                   faster for large ROW_AMOUNT values.  Direct-path inserts are
                   incompatible with the RETURNING ROWID pattern used for LOB
                   population, so APPEND is automatically overridden to 0 when
                   the table has any LOB column.
                ------------------------------------------------------------ */
                IF cmd.USE_APPEND = 1 THEN
                    IF v_table_has_lob THEN
                        v_use_append := FALSE;
                        v_log := f_log(v_run_id, cmd.TABLE_NAME, 'f_fill_tables',
                                       'INFO: USE_APPEND=1 overridden to 0 — '
                                       || 'table contains LOB columns; direct-path '
                                       || 'inserts are incompatible with the '
                                       || 'RETURNING ROWID pattern for LOB content.');
                    ELSE
                        v_use_append := TRUE;
                    END IF;
                ELSE
                    v_use_append := FALSE;
                END IF;

                /* ==============================================================
                   INNER LOOP — insert one row per iteration.
                ============================================================== */
                FOR idx IN 1..v_rows_needed LOOP

                    -- Reset per-row state.
                    v_columns     := NULL;
                    v_values      := NULL;
                    v_row_has_lob := FALSE;
                    v_blob_count  := 0;
                    v_clob_count  := 0;
                    v_blob_cols.DELETE;
                    v_clob_cols.DELETE;

                    /* ----------------------------------------------------------
                       STEP 1 — Auto-handled PK columns
                    ---------------------------------------------------------- */
                    FOR i IN 1..v_auto_pk_cnt LOOP
                        v_columns := v_columns || ',' || v_auto_pk_cols(i).col_name;

                        IF v_auto_pk_cols(i).data_type = 'NUMBER' THEN
                            -- Cached MAX + current row index = unique, collision-free.
                            v_values := v_values || ','
                                        || TO_CHAR(v_pk_max_tab(v_auto_pk_cols(i).col_name) + idx);

                        ELSIF v_auto_pk_cols(i).data_type
                                  IN ('VARCHAR2','CHAR','NVARCHAR2','NCHAR') THEN
                            IF v_auto_pk_cols(i).data_length >= 32 THEN
                                -- 32-char uppercase hex string; globally unique.
                                v_values := v_values
                                            || ',''' || RAWTOHEX(SYS_GUID()) || '''';
                            ELSE
                                -- Column too short for SYS_GUID(); use sequential
                                -- numeric string based on the cached COUNT(*) offset.
                                v_values := v_values || ','''
                                            || TO_CHAR(
                                                   v_pk_max_tab(v_auto_pk_cols(i).col_name) + idx)
                                            || '''';
                            END IF;

                        ELSIF v_auto_pk_cols(i).data_type = 'RAW' THEN
                            v_values := v_values
                                        || ',HEXTORAW(''' || RAWTOHEX(SYS_GUID()) || ''')';

                        ELSE
                            -- Unsupported PK type — insert NULL.
                            -- Warning was already logged in pre-analysis.
                            v_values := v_values || ',NULL';
                        END IF;
                    END LOOP; -- auto-PK

                    /* ----------------------------------------------------------
                       STEP 2 — COLS-configured columns
                       FILLING_STRING is executed as a SELECT returning one value.
                       The result is embedded directly into the VALUES clause:
                         - NUMBER values: no quoting needed (e.g. 42)
                         - VARCHAR2 values: must include surrounding quotes
                           (use chr(39) in the expression to avoid nesting)
                    ---------------------------------------------------------- */
                    FOR i IN 1..v_cfg_cnt LOOP
                        EXECUTE IMMEDIATE v_cfg_cols(i).fill_str INTO v_filling_str;
                        v_columns := v_columns || ',' || v_cfg_cols(i).col_name;
                        v_values  := v_values  || ',' || v_filling_str;
                    END LOOP; -- COLS-configured

                    /* ----------------------------------------------------------
                       STEP 3 — Regular columns (auto-detected by data type)
                    ---------------------------------------------------------- */
                    FOR i IN 1..v_reg_cnt LOOP
                        v_columns := v_columns || ',' || v_reg_cols(i).col_name;

                        /* ---- String types ---------------------------------- */
                        IF v_reg_cols(i).data_type
                               IN ('VARCHAR2','CHAR','NVARCHAR2','NCHAR')
                        THEN
                            -- f_random_string already applies a random length
                            -- between 1 and i_length for realistic data variance.
                            v_random_str := f_random_string(v_reg_cols(i).data_length);
                            v_values := v_values || ',''' || v_random_str || '''';

                        /* ---- Exact numeric types --------------------------- */
                        ELSIF v_reg_cols(i).data_type
                                  IN ('NUMBER','INTEGER','INT','SMALLINT','FLOAT')
                        THEN
                            v_random_num := f_random_number(
                                                NVL(v_reg_cols(i).data_prec,  10),
                                                NVL(v_reg_cols(i).data_scale,  0));
                            -- 'NLS_NUMERIC_CHARACTERS' override ensures '.' as decimal
                            -- separator regardless of session NLS settings, preventing
                            -- malformed SQL literals on European-locale databases.
                            v_values := v_values || ','
                                        || TO_CHAR(v_random_num, 'TM9',
                                                   'NLS_NUMERIC_CHARACTERS=''.,''');

                        /* ---- IEEE 754 binary float ------------------------- */
                        ELSIF v_reg_cols(i).data_type = 'BINARY_FLOAT' THEN
                            -- Call the dedicated function, not f_random_number.
                            -- NLS override prevents comma decimal on European locales.
                            v_values := v_values
                                        || ',TO_BINARY_FLOAT('
                                        || TO_CHAR(f_random_binary_float, 'TM9',
                                                   'NLS_NUMERIC_CHARACTERS=''.,''') || ')';

                        /* ---- IEEE 754 binary double ------------------------ */
                        ELSIF v_reg_cols(i).data_type = 'BINARY_DOUBLE' THEN
                            -- NLS override prevents comma decimal on European locales.
                            v_values := v_values
                                        || ',TO_BINARY_DOUBLE('
                                        || TO_CHAR(f_random_binary_double, 'TM9',
                                                   'NLS_NUMERIC_CHARACTERS=''.,''') || ')';

                        /* ---- Date ------------------------------------------ */
                        ELSIF v_reg_cols(i).data_type = 'DATE' THEN
                            -- Date range comes from the per-table CMD configuration.
                            v_random_dt := f_random_date(cmd.DATE_FROM, cmd.DATE_TO);
                            v_values := v_values
                                        || ',TO_DATE('''
                                        || TO_CHAR(v_random_dt, 'YYYY-MM-DD HH24:MI:SS')
                                        || ''',''YYYY-MM-DD HH24:MI:SS'')';

                        /* ---- Timestamp (all variants) ---------------------- */
                        ELSIF v_reg_cols(i).data_type LIKE 'TIMESTAMP%' THEN
                            -- Covers TIMESTAMP, TIMESTAMP WITH TIME ZONE, and
                            -- TIMESTAMP WITH LOCAL TIME ZONE.
                            v_random_ts := f_random_timestamp(cmd.DATE_FROM, cmd.DATE_TO);
                            v_values := v_values
                                        || ',TO_TIMESTAMP('''
                                        || TO_CHAR(v_random_ts, 'YYYY-MM-DD HH24:MI:SS.FF6')
                                        || ''',''YYYY-MM-DD HH24:MI:SS.FF6'')';

                        /* ---- RAW ------------------------------------------- */
                        ELSIF v_reg_cols(i).data_type = 'RAW' THEN
                            -- f_random_raw() uses SYS_GUID() (pure hex, safe for HEXTORAW).
                            v_values := v_values
                                        || ',HEXTORAW('''
                                        || RAWTOHEX(f_random_raw(v_reg_cols(i).data_length))
                                        || ''')';

                        /* ---- BLOB ------------------------------------------ */
                        ELSIF v_reg_cols(i).data_type = 'BLOB' THEN
                            -- Insert an empty locator placeholder.
                            -- Content is written via RETURNING ROWID in Step 4.
                            v_values      := v_values || ',EMPTY_BLOB()';
                            v_row_has_lob := TRUE;
                            v_blob_count  := v_blob_count + 1;
                            v_blob_cols(v_blob_count) := v_reg_cols(i).col_name;

                        /* ---- CLOB / NCLOB ---------------------------------- */
                        ELSIF v_reg_cols(i).data_type IN ('CLOB','NCLOB') THEN
                            v_values      := v_values || ',EMPTY_CLOB()';
                            v_row_has_lob := TRUE;
                            v_clob_count  := v_clob_count + 1;
                            v_clob_cols(v_clob_count) := v_reg_cols(i).col_name;

                        /* ---- Unsupported types ----------------------------- */
                        ELSE
                            -- XMLTYPE, INTERVAL, SDO_GEOMETRY, etc.
                            -- Warning was already logged in pre-analysis; just insert NULL.
                            v_values := v_values || ',NULL';
                        END IF;

                    END LOOP; -- regular columns

                    /* ----------------------------------------------------------
                       STEP 4 — Build and execute the INSERT statement.

                       Non-LOB path: single INSERT (with optional APPEND hint).

                       LOB path: INSERT with EMPTY_BLOB/EMPTY_CLOB placeholders
                       (APPEND is auto-disabled for LOB tables), capture ROWID via
                       RETURNING, then UPDATE each LOB column with a bind variable.
                       Each temporary LOB is freed immediately after the UPDATE to
                       prevent temp LOB cache growth during long-running inserts.
                    ---------------------------------------------------------- */
                    IF v_use_append THEN
                        v_stmt := 'INSERT /*+ APPEND */ INTO ' || cmd.TABLE_NAME
                                  || ' (' || TRIM(',' FROM v_columns) || ')'
                                  || ' VALUES (' || TRIM(',' FROM v_values) || ')';
                    ELSE
                        v_stmt := 'INSERT INTO ' || cmd.TABLE_NAME
                                  || ' (' || TRIM(',' FROM v_columns) || ')'
                                  || ' VALUES (' || TRIM(',' FROM v_values) || ')';
                    END IF;

                    IF v_row_has_lob THEN
                        -- Insert empty locators; capture the new row's ROWID.
                        EXECUTE IMMEDIATE v_stmt || ' RETURNING ROWID INTO :1'
                            RETURNING INTO v_rowid;

                        -- Populate each BLOB column and free the temporary LOB.
                        FOR i IN 1..v_blob_count LOOP
                            v_tmp_blob := f_random_blob(v_lob_cycle);
                            EXECUTE IMMEDIATE
                                'UPDATE ' || cmd.TABLE_NAME
                                || ' SET '   || v_blob_cols(i) || ' = :1'
                                || ' WHERE ROWID = :2'
                                USING v_tmp_blob, v_rowid;
                            DBMS_LOB.FREETEMPORARY(v_tmp_blob);
                        END LOOP;

                        -- Populate each CLOB/NCLOB column and free the temporary LOB.
                        FOR i IN 1..v_clob_count LOOP
                            v_tmp_clob := f_random_clob(v_lob_cycle);
                            EXECUTE IMMEDIATE
                                'UPDATE ' || cmd.TABLE_NAME
                                || ' SET '   || v_clob_cols(i) || ' = :1'
                                || ' WHERE ROWID = :2'
                                USING v_tmp_clob, v_rowid;
                            DBMS_LOB.FREETEMPORARY(v_tmp_clob);
                        END LOOP;

                    ELSE
                        EXECUTE IMMEDIATE v_stmt;
                    END IF;

                    /* ----------------------------------------------------------
                       Intermediate commit.

                       Standard path (no APPEND): commit every COMMIT_EVERY rows
                       to limit UNDO tablespace pressure during large inserts.

                       APPEND path: Oracle requires a COMMIT after every direct-
                       path INSERT before the same table can be accessed again in
                       the same transaction (ORA-12839 otherwise).  COMMIT_EVERY
                       is therefore ignored when APPEND is active and every row
                       is committed individually.
                    ---------------------------------------------------------- */
                    IF v_use_append OR MOD(idx, cmd.COMMIT_EVERY) = 0 THEN
                        COMMIT;
                    END IF;

                END LOOP; -- inner row loop

                -- Final commit for any rows since the last intermediate commit.
                COMMIT;

                END IF; -- v_rows_needed > 0

                v_log := f_log(v_run_id, cmd.TABLE_NAME, 'f_fill_tables',
                               'Completed. Rows inserted: ' || v_rows_needed);
                v_all_rows := v_all_rows + v_rows_needed;

                -- Mark table as processed; IF_GENERATE = 0 means "done / skip".
                -- Use p_enable(table_name) or p_enable_all to re-enable.
                UPDATE T_DATGEN_TOY_CMD
                SET    IF_GENERATE = 0
                WHERE  TABLE_NAME = cmd.TABLE_NAME;
                COMMIT;

            EXCEPTION
                WHEN OTHERS THEN
                    -- Roll back this table's uncommitted work only.
                    ROLLBACK;
                    v_log_discard := f_log(v_run_id, cmd.TABLE_NAME, 'f_fill_tables',
                                           'ERROR: ' || SQLERRM
                                           || ' — work since last commit rolled back. '
                                           || 'Continuing to next table.');
            END; -- per-table block

        END LOOP; -- outer table loop

        v_log := f_log(v_run_id, NULL, 'f_fill_tables',
                       'Run complete. Total rows inserted: ' || v_all_rows);
        RETURN v_all_rows;

    EXCEPTION
        WHEN OTHERS THEN
            -- Catch errors outside the per-table block (e.g. run-start failure).
            ROLLBACK;
            v_log_discard := f_log(v_run_id, NULL, 'f_fill_tables',
                                   'FATAL: ' || SQLERRM || ' — run aborted.');
            RETURN 0;
    END f_fill_tables;


    /* =========================================================================
       p_run  (no arguments)
    ========================================================================= */
    PROCEDURE p_run IS
        v_rows  NUMBER;
    BEGIN
        v_rows := f_fill_tables(in_table_name => NULL);
        DBMS_OUTPUT.PUT_LINE('Run complete. Total rows inserted: ' || v_rows);
    END p_run;


    /* =========================================================================
       p_run  (one argument)
    ========================================================================= */
    PROCEDURE p_run (in_table_name VARCHAR2) IS
        v_rows  NUMBER;
    BEGIN
        v_rows := f_fill_tables(in_table_name => in_table_name);
        DBMS_OUTPUT.PUT_LINE('Run complete for ' || UPPER(in_table_name)
                             || '. Rows inserted: ' || v_rows);
    END p_run;


    /* =========================================================================
       p_enable_all
    ========================================================================= */
    PROCEDURE p_enable_all IS
        v_count  PLS_INTEGER;
    BEGIN
        UPDATE T_DATGEN_TOY_CMD
        SET    IF_GENERATE = 1;
        v_count := SQL%ROWCOUNT;
        COMMIT;
        IF v_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('WARN: No rows in T_DATGEN_TOY_CMD — nothing enabled.');
        ELSE
            DBMS_OUTPUT.PUT_LINE(v_count || ' table(s) enabled (IF_GENERATE = 1).');
        END IF;
    END p_enable_all;


    /* =========================================================================
       p_enable
    ========================================================================= */
    PROCEDURE p_enable (in_table_name VARCHAR2) IS
        v_count  PLS_INTEGER;
    BEGIN
        UPDATE T_DATGEN_TOY_CMD
        SET    IF_GENERATE = 1
        WHERE  TABLE_NAME = UPPER(in_table_name);
        -- Capture before COMMIT — COMMIT resets SQL%ROWCOUNT to 0.
        v_count := SQL%ROWCOUNT;
        COMMIT;
        IF v_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('WARN: ' || UPPER(in_table_name)
                                 || ' not found in T_DATGEN_TOY_CMD — nothing changed.');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Table ' || UPPER(in_table_name)
                                 || ' enabled (IF_GENERATE = 1).');
        END IF;
    END p_enable;


    /* =========================================================================
       p_reset  (alias for p_enable)
    ========================================================================= */
    PROCEDURE p_reset (in_table_name VARCHAR2) IS
    BEGIN
        p_enable(in_table_name);
    END p_reset;


    /* =========================================================================
       p_analyze
       Dry-run pre-analysis for one table.  Logs what f_fill_tables would do
       without inserting any data.  Call this to validate configuration, check
       PK auto-detection strategy, and surface warnings before a real run.
    ========================================================================= */
    PROCEDURE p_analyze (in_table_name VARCHAR2) IS
        v_tbl           VARCHAR2(64) := UPPER(in_table_name);
        v_pk_col_count  NUMBER;
        v_cmd_count     NUMBER;
        v_tbl_count     NUMBER;
        v_log           VARCHAR2(4000);
    BEGIN
        SELECT SEQ_DATGEN_TOY_LOG_ID.NEXTVAL INTO v_run_id FROM DUAL;
        v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                       'Analysis started for table: ' || v_tbl);

        -- Verify the table has an entry in the command table.
        SELECT COUNT(*) INTO v_cmd_count
        FROM   T_DATGEN_TOY_CMD
        WHERE  TABLE_NAME = v_tbl;

        IF v_cmd_count = 0 THEN
            v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                           'ERROR: ' || v_tbl
                           || ' not found in T_DATGEN_TOY_CMD.');
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || v_tbl
                                 || ' not found in T_DATGEN_TOY_CMD.');
            RETURN;
        END IF;

        -- Verify the physical table actually exists in the schema.
        -- If it doesn't, user_tab_cols returns 0 rows silently, giving a
        -- false impression that the configuration is clean.
        SELECT COUNT(*) INTO v_tbl_count
        FROM   user_tables
        WHERE  table_name = v_tbl;

        IF v_tbl_count = 0 THEN
            v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                           'WARN: ' || v_tbl
                           || ' is in T_DATGEN_TOY_CMD but does not exist in '
                           || 'USER_TABLES. f_fill_tables will fail for this table.');
            DBMS_OUTPUT.PUT_LINE('WARN: ' || v_tbl
                                 || ' not found in USER_TABLES.');
            -- Continue analysis — report 0 columns rather than aborting silently.
        END IF;

        -- PK summary
        SELECT COUNT(*) INTO v_pk_col_count
        FROM   user_cons_columns ucc
        JOIN   user_constraints  uc ON ucc.constraint_name = uc.constraint_name
        WHERE  uc.constraint_type = 'P'
          AND  uc.table_name      = v_tbl;

        IF v_pk_col_count = 0 THEN
            v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                           'INFO: No primary key detected.');
        ELSIF v_pk_col_count > 1 THEN
            v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                           'INFO: Composite PK (' || v_pk_col_count || ' columns).');
        ELSE
            v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                           'INFO: Single-column PK detected.');
        END IF;

        -- Log each auto-handled PK column and its generation strategy.
        FOR pk_col IN (
            SELECT ucc.column_name,
                   utc.data_type,
                   CASE WHEN utc.data_type IN ('NVARCHAR2','NCHAR')
                        THEN utc.char_length
                        ELSE utc.data_length
                   END AS data_length
            FROM   user_cons_columns ucc
            JOIN   user_constraints  uc  ON ucc.constraint_name = uc.constraint_name
            JOIN   user_tab_cols     utc ON utc.table_name       = ucc.table_name
                                       AND utc.column_name       = ucc.column_name
            WHERE  uc.constraint_type = 'P'
              AND  uc.table_name      = v_tbl
              AND  NOT EXISTS (
                       SELECT 1 FROM T_DATGEN_TOY_COLS c
                       WHERE  c.table_name  = ucc.table_name
                         AND  c.column_name = ucc.column_name
                   )
        ) LOOP
            IF pk_col.data_type = 'NUMBER' THEN
                v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                               '  PK ' || pk_col.column_name
                               || ' (' || pk_col.data_type || ')'
                               || ' → auto: MAX+N');
            ELSIF pk_col.data_type IN ('VARCHAR2','CHAR','NVARCHAR2','NCHAR') THEN
                IF pk_col.data_length >= 32 THEN
                    v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                                   '  PK ' || pk_col.column_name
                                   || ' (' || pk_col.data_type
                                   || '(' || pk_col.data_length || '))'
                                   || ' → auto: SYS_GUID()');
                ELSE
                    v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                                   '  PK ' || pk_col.column_name
                                   || ' (' || pk_col.data_type
                                   || '(' || pk_col.data_length || '))'
                                   || ' → auto: sequential string '
                                   || '(column too short for SYS_GUID)');
                END IF;
            ELSIF pk_col.data_type = 'RAW' THEN
                v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                               '  PK ' || pk_col.column_name
                               || ' (RAW) → auto: SYS_GUID()');
            ELSE
                v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                               '  PK ' || pk_col.column_name
                               || ' (' || pk_col.data_type
                               || ') UNSUPPORTED → NULL will be inserted');
            END IF;
        END LOOP;

        -- Log COLS-configured columns.
        FOR cfg_col IN (
            SELECT column_name, constraint_type, filling_string
            FROM   T_DATGEN_TOY_COLS
            WHERE  table_name = v_tbl
        ) LOOP
            v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                           '  COLS ' || cfg_col.column_name
                           || ' [' || cfg_col.constraint_type || ']'
                           || ' → custom: '
                           || SUBSTR(cfg_col.filling_string, 1, 80));
        END LOOP;

        -- Log regular columns; flag NOT NULL unsupported-type problems.
        FOR reg_col IN (
            SELECT utc.column_name,
                   utc.data_type,
                   CASE WHEN utc.data_type IN ('NVARCHAR2','NCHAR')
                        THEN utc.char_length
                        ELSE utc.data_length
                   END AS data_length,
                   utc.nullable
            FROM   user_tab_cols utc
            WHERE  utc.table_name     = v_tbl
              AND  utc.virtual_column = 'NO'
              AND  utc.hidden_column  = 'NO'
              AND  utc.column_name NOT IN (
                       SELECT c.column_name FROM T_DATGEN_TOY_COLS c
                       WHERE  c.table_name = v_tbl
                   )
              AND  utc.column_name NOT IN (
                       SELECT ucc.column_name
                       FROM   user_cons_columns ucc
                       JOIN   user_constraints  uc ON ucc.constraint_name = uc.constraint_name
                       WHERE  uc.constraint_type = 'P' AND uc.table_name = v_tbl
                   )
        ) LOOP
            IF reg_col.data_type NOT IN (
                   'VARCHAR2','CHAR','NVARCHAR2','NCHAR',
                   'NUMBER','INTEGER','INT','SMALLINT','FLOAT',
                   'BINARY_FLOAT','BINARY_DOUBLE',
                   'DATE','RAW','BLOB','CLOB','NCLOB'
               )
               AND reg_col.data_type NOT LIKE 'TIMESTAMP%'
            THEN
                IF reg_col.nullable = 'N' THEN
                    v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                                   '  REG ' || reg_col.column_name
                                   || ' (' || reg_col.data_type
                                   || ') NOT NULL UNSUPPORTED'
                                   || ' → WILL FAIL (ORA-01400)');
                ELSE
                    v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                                   '  REG ' || reg_col.column_name
                                   || ' (' || reg_col.data_type
                                   || ') unsupported → NULL will be inserted');
                END IF;
            ELSE
                v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                               '  REG ' || reg_col.column_name
                               || ' (' || reg_col.data_type || ')'
                               || CASE WHEN reg_col.nullable = 'N'
                                       THEN ' NOT NULL' ELSE '' END
                               || ' → auto');
            END IF;
        END LOOP;

        v_log := f_log(v_run_id, v_tbl, 'p_analyze',
                       'Analysis complete. To review: SELECT * FROM '
                       || 'T_DATGEN_TOY_LOG WHERE run_id = '
                       || v_run_id || ' ORDER BY id;');
        DBMS_OUTPUT.PUT_LINE('Analysis logged. RUN_ID = ' || v_run_id);

    EXCEPTION
        WHEN OTHERS THEN
            v_log_discard := f_log(v_run_id, v_tbl, 'p_analyze',
                                   'ERROR: ' || SQLERRM);
            DBMS_OUTPUT.PUT_LINE('Analysis error: ' || SQLERRM);
    END p_analyze;


-- =============================================================================
-- Package initialisation block — nothing to initialise.
-- =============================================================================
BEGIN
    NULL;
END PKG_DATGEN_TOY;
/
