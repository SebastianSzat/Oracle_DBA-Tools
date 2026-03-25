-- =============================================================================
-- 08_PKG_ANON_TOY.sql
-- Package specification and body for the ANON_TOY data anonymizer.
--
-- Run as ANON_TOY after 07_T_ANON_TOY_POOL.sql.
--
-- Public API
-- ----------
--   p_analyze    (i_schema, i_table_name)  Dry-run: check config, test CUSTOM_EXPR
--   p_build_map  (i_schema, i_table_name)  Phase 1: generate old→new mappings
--   p_apply_map  (i_schema, i_table_name)  Phase 2: disable FKs, apply, re-enable
--   p_run        (i_schema, i_table_name)  Calls p_build_map then p_apply_map
--   p_enable     (i_schema, i_table_name)  Set IF_ANONYMIZE=1; i_table_name DEFAULT NULL
--                                          enables all tables in the schema
--   p_enable_all                           Reset IF_ANONYMIZE to 1 for all tables
--   p_clear_map  (i_schema, i_table_name)  Delete MAP entries; i_table_name DEFAULT NULL
--                                          clears all MAP entries for the schema
--   p_clear_map_all                        Delete all MAP entries
--
-- All i_schema / i_table_name parameters accept NULL to mean "process all
-- rows in T_ANON_TOY_CMD with IF_ANONYMIZE = 1" (ordered by LVL).
-- p_enable and p_clear_map accept i_table_name DEFAULT NULL to mean "all
-- tables within i_schema" (schema-scope operation).
--
-- FK cascade strategy
-- -------------------
-- When a parent column is anonymized its referenced FK constraints are
-- DISABLE'd before the UPDATE and re-enabled with ENABLE NOVALIDATE afterwards.
-- NOVALIDATE skips the full-table historical check (which would fail because
-- child rows still hold the old values) while still enforcing future DML.
-- By the time all tables are processed in LVL order, all child FK columns
-- have been updated to match the new parent values.  A DBA can then run
-- ALTER TABLE ... ENABLE VALIDATE CONSTRAINT ... to fully validate if needed.
-- =============================================================================

CREATE OR REPLACE PACKAGE PKG_ANON_TOY
AUTHID DEFINER
AS

    PROCEDURE p_analyze (
        i_schema     IN VARCHAR2 DEFAULT NULL,
        i_table_name IN VARCHAR2 DEFAULT NULL
    );

    PROCEDURE p_build_map (
        i_schema     IN VARCHAR2 DEFAULT NULL,
        i_table_name IN VARCHAR2 DEFAULT NULL
    );

    PROCEDURE p_apply_map (
        i_schema     IN VARCHAR2 DEFAULT NULL,
        i_table_name IN VARCHAR2 DEFAULT NULL
    );

    PROCEDURE p_run (
        i_schema     IN VARCHAR2 DEFAULT NULL,
        i_table_name IN VARCHAR2 DEFAULT NULL
    );

    PROCEDURE p_enable (
        i_schema     IN VARCHAR2,
        i_table_name IN VARCHAR2 DEFAULT NULL
    );

    PROCEDURE p_enable_all;

    PROCEDURE p_clear_map (
        i_schema     IN VARCHAR2,
        i_table_name IN VARCHAR2 DEFAULT NULL
    );

    PROCEDURE p_clear_map_all;

END PKG_ANON_TOY;
/


-- =============================================================================
-- Package body
-- =============================================================================

CREATE OR REPLACE PACKAGE BODY PKG_ANON_TOY
AS

    -- =========================================================================
    -- Package-level types
    -- =========================================================================

    TYPE t_cmd_rec IS RECORD (
        table_schema  VARCHAR2(64),
        table_name    VARCHAR2(64),
        commit_every  NUMBER(10)
    );
    TYPE t_cmd_tab IS TABLE OF t_cmd_rec INDEX BY PLS_INTEGER;

    TYPE t_col_rec IS RECORD (
        column_name        VARCHAR2(64),
        anon_method        VARCHAR2(16),
        data_category      VARCHAR2(64),
        shift_range        NUMBER(10),
        is_pk_source       VARCHAR2(1),    -- informational only; FK auto-discovery
                                           -- via DBA_CONSTRAINTS fires for all
                                           -- tables regardless of this flag
        map_source_schema  VARCHAR2(64),
        map_source_table   VARCHAR2(64),
        map_source_column  VARCHAR2(64),
        custom_expr        VARCHAR2(4000),
        constraint_type    VARCHAR2(16)    -- informational metadata ('PK','FK','DATA');
                                           -- not used in package logic
    );
    TYPE t_col_tab IS TABLE OF t_col_rec INDEX BY PLS_INTEGER;

    TYPE t_fk_rec IS RECORD (
        owner            VARCHAR2(64),
        table_name       VARCHAR2(64),
        constraint_name  VARCHAR2(64)
    );
    TYPE t_fk_tab IS TABLE OF t_fk_rec INDEX BY PLS_INTEGER;

    -- =========================================================================
    -- Package-level flag: set TRUE by core procedures when a FATAL error is
    -- caught internally.  p_run reads this after each core call and raises
    -- an application error so automation can detect failure.
    -- Reset to FALSE at the start of each p_run / p_build_map / p_apply_map call.
    --
    -- ORA-04068 note: if the package body is recompiled while a session has an
    -- active run (e.g. mid-loop in p_build_map_core), Oracle raises ORA-04068
    -- on the next package call from that session and discards all package state,
    -- resetting this flag to FALSE.  Consequence: p_run might skip its check
    -- after the core call if ORA-04068 fired between the FATAL handler and the
    -- check.  This race is extremely unlikely in production but is inherent to
    -- session-scoped package state and cannot be eliminated here.
    -- p_run re-raises any unhandled exception (including ORA-04068) from its
    -- WHEN OTHERS so automation will still see a failure signal.
    -- =========================================================================
    g_fatal_occurred BOOLEAN := FALSE;

    -- =========================================================================
    -- Private: f_next_run_id
    -- =========================================================================
    FUNCTION f_next_run_id RETURN NUMBER
    IS
        v_id NUMBER;
    BEGIN
        SELECT SEQ_ANON_TOY_LOG_ID.NEXTVAL INTO v_id FROM DUAL;
        RETURN v_id;
    END f_next_run_id;

    -- =========================================================================
    -- Private: f_log / p_log
    -- PRAGMA AUTONOMOUS_TRANSACTION ensures log commits never affect the
    -- caller's transaction.
    -- f_log is implemented as a FUNCTION (RETURN NUMBER) rather than a
    -- PROCEDURE because that makes it callable as a statement-level expression
    -- (v_dummy := f_log(...)) from within SQL contexts.  In practice all
    -- callers are in PL/SQL blocks; p_log is the preferred call site and
    -- hides the dummy-return boilerplate.
    -- NOTE: the return value (1 = logged, 0 = log failed) exists solely to
    -- satisfy the PL/SQL FUNCTION-in-expression idiom.  No caller should
    -- branch on it: a 0 return means the log write was silently lost, not
    -- that anything in the caller's processing failed.  Always discard via
    -- v_dummy.
    -- =========================================================================
    FUNCTION f_log (
        i_run_id      IN NUMBER,
        i_table_name  IN VARCHAR2,
        i_step        IN VARCHAR2,
        i_status_info IN VARCHAR2
    ) RETURN NUMBER
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO T_ANON_TOY_LOG (RUN_ID, TABLE_NAME, STEP, STATUS_INFO)
        VALUES (i_run_id, i_table_name, i_step, i_status_info);
        COMMIT;
        RETURN 1;
    EXCEPTION
        WHEN OTHERS THEN
            -- Logging must not replace the original exception in a WHEN OTHERS
            -- handler.  Silently return 0 so the caller's error chain is
            -- preserved.  The autonomous transaction rolls back automatically.
            RETURN 0;
    END f_log;

    PROCEDURE p_log (
        i_run_id      IN NUMBER,
        i_table_name  IN VARCHAR2,
        i_step        IN VARCHAR2,
        i_status_info IN VARCHAR2
    )
    IS
        v_dummy NUMBER;
    BEGIN
        v_dummy := f_log(i_run_id, i_table_name, i_step, i_status_info);
    END p_log;

    -- =========================================================================
    -- Private: get_cmd_rows
    -- Collects CMD rows into o_cmds / o_cnt, ordered by LVL.
    -- Three-branch scoping:
    --   Both non-null  → exact table match
    --   Schema only    → all tables in that schema (i_table_name IS NULL)
    --   Both null      → all tables with IF_ANONYMIZE = 1
    -- Previously the table-without-schema case (i_table_name IS NOT NULL,
    -- i_schema IS NULL) fell through to the global scope, silently processing
    -- all enabled tables instead of raising an error for this invalid input.
    -- =========================================================================
    PROCEDURE get_cmd_rows (
        i_schema     IN  VARCHAR2,
        i_table_name IN  VARCHAR2,
        o_cmds       OUT t_cmd_tab,
        o_cnt        OUT PLS_INTEGER
    )
    IS
    BEGIN
        -- Guard: a table name without a schema is a caller error.  The ELSE
        -- arm (global scope) would otherwise silently ignore the table name
        -- and process every enabled table across all schemas.
        IF i_schema IS NULL AND i_table_name IS NOT NULL THEN
            RAISE_APPLICATION_ERROR(-20000,
                'PKG_ANON_TOY: i_table_name requires i_schema to be specified');
        END IF;

        o_cnt := 0;
        IF i_schema IS NOT NULL AND i_table_name IS NOT NULL THEN
            -- Exact table match
            FOR r IN (
                SELECT TABLE_SCHEMA, TABLE_NAME, COMMIT_EVERY
                FROM   T_ANON_TOY_CMD
                WHERE  UPPER(TABLE_SCHEMA) = UPPER(i_schema)
                AND    UPPER(TABLE_NAME)   = UPPER(i_table_name)
                AND    IF_ANONYMIZE        = 1
                ORDER BY LVL
            ) LOOP
                o_cnt := o_cnt + 1;
                o_cmds(o_cnt).table_schema := r.TABLE_SCHEMA;
                o_cmds(o_cnt).table_name   := r.TABLE_NAME;
                o_cmds(o_cnt).commit_every := r.COMMIT_EVERY;
            END LOOP;
        ELSIF i_schema IS NOT NULL AND i_table_name IS NULL THEN
            -- Schema-scope: all enabled tables within the named schema
            FOR r IN (
                SELECT TABLE_SCHEMA, TABLE_NAME, COMMIT_EVERY
                FROM   T_ANON_TOY_CMD
                WHERE  UPPER(TABLE_SCHEMA) = UPPER(i_schema)
                AND    IF_ANONYMIZE        = 1
                ORDER BY LVL
            ) LOOP
                o_cnt := o_cnt + 1;
                o_cmds(o_cnt).table_schema := r.TABLE_SCHEMA;
                o_cmds(o_cnt).table_name   := r.TABLE_NAME;
                o_cmds(o_cnt).commit_every := r.COMMIT_EVERY;
            END LOOP;
        ELSE
            -- Global scope: all enabled tables across all schemas
            FOR r IN (
                SELECT TABLE_SCHEMA, TABLE_NAME, COMMIT_EVERY
                FROM   T_ANON_TOY_CMD
                WHERE  IF_ANONYMIZE = 1
                ORDER BY LVL
            ) LOOP
                o_cnt := o_cnt + 1;
                o_cmds(o_cnt).table_schema := r.TABLE_SCHEMA;
                o_cmds(o_cnt).table_name   := r.TABLE_NAME;
                o_cmds(o_cnt).commit_every := r.COMMIT_EVERY;
            END LOOP;
        END IF;
    END get_cmd_rows;

    -- =========================================================================
    -- Private: get_col_rows
    -- Collects COLS rows for one table (KEEP rows excluded).
    -- =========================================================================
    PROCEDURE get_col_rows (
        i_schema     IN  VARCHAR2,
        i_table_name IN  VARCHAR2,
        o_cols       OUT t_col_tab,
        o_cnt        OUT PLS_INTEGER
    )
    IS
    BEGIN
        o_cnt := 0;
        FOR r IN (
            SELECT COLUMN_NAME,
                   ANON_METHOD,
                   DATA_CATEGORY,
                   SHIFT_RANGE,
                   IS_PK_SOURCE,
                   MAP_SOURCE_SCHEMA,
                   MAP_SOURCE_TABLE,
                   MAP_SOURCE_COLUMN,
                   CUSTOM_EXPR,
                   CONSTRAINT_TYPE
            FROM   T_ANON_TOY_COLS
            WHERE  UPPER(TABLE_SCHEMA) = UPPER(i_schema)
            AND    UPPER(TABLE_NAME)   = UPPER(i_table_name)
            AND    ANON_METHOD        <> 'KEEP'
        ) LOOP
            o_cnt := o_cnt + 1;
            o_cols(o_cnt).column_name        := r.COLUMN_NAME;
            o_cols(o_cnt).anon_method        := r.ANON_METHOD;
            o_cols(o_cnt).data_category      := r.DATA_CATEGORY;
            o_cols(o_cnt).shift_range        := r.SHIFT_RANGE;
            o_cols(o_cnt).is_pk_source       := r.IS_PK_SOURCE;
            o_cols(o_cnt).map_source_schema  := r.MAP_SOURCE_SCHEMA;
            o_cols(o_cnt).map_source_table   := r.MAP_SOURCE_TABLE;
            o_cols(o_cnt).map_source_column  := r.MAP_SOURCE_COLUMN;
            o_cols(o_cnt).custom_expr        := r.CUSTOM_EXPR;
            o_cols(o_cnt).constraint_type    := r.CONSTRAINT_TYPE;
        END LOOP;
    END get_col_rows;

    -- =========================================================================
    -- Private: get_child_fk_constraints
    -- Collects all FK constraints whose referenced table is i_schema.i_table_name
    -- (i.e. FKs on child tables that point up to this parent table).
    -- =========================================================================
    PROCEDURE get_child_fk_constraints (
        i_schema     IN  VARCHAR2,
        i_table_name IN  VARCHAR2,
        o_fks        OUT t_fk_tab,
        o_cnt        OUT PLS_INTEGER
    )
    IS
    BEGIN
        o_cnt := 0;
        FOR r IN (
            SELECT c.OWNER, c.TABLE_NAME, c.CONSTRAINT_NAME
            FROM   DBA_CONSTRAINTS  c
            JOIN   DBA_CONSTRAINTS  p
                ON  p.CONSTRAINT_NAME = c.R_CONSTRAINT_NAME
                AND p.OWNER           = c.R_OWNER
            WHERE  c.CONSTRAINT_TYPE = 'R'
            AND    UPPER(p.OWNER)      = UPPER(i_schema)
            AND    UPPER(p.TABLE_NAME) = UPPER(i_table_name)
        ) LOOP
            o_cnt := o_cnt + 1;
            o_fks(o_cnt).owner           := r.OWNER;
            o_fks(o_cnt).table_name      := r.TABLE_NAME;
            o_fks(o_cnt).constraint_name := r.CONSTRAINT_NAME;
        END LOOP;
    END get_child_fk_constraints;

    -- =========================================================================
    -- Private: p_disable_fks / p_enable_fks
    -- p_enable_fks uses ENABLE NOVALIDATE deliberately:
    --   ENABLE VALIDATE would fail here because child rows still carry old
    --   parent values at the time the parent table is re-enabled.  NOVALIDATE
    --   skips the historical check while still enforcing future DML.
    --   After the full run (all tables processed in LVL order) all FK columns
    --   are consistent; a DBA may then run ENABLE VALIDATE manually if desired.
    --
    -- NOTE: each ALTER TABLE DDL statement issues an implicit COMMIT before and
    -- after execution.  Any open DML in the calling session at the moment
    -- p_disable_fks or p_enable_fks is entered will be committed implicitly.
    -- p_apply_map_core calls p_disable_fks only at clean transaction boundaries
    -- (all prior columns for the previous table have been committed), so this is
    -- safe within the package's own call path.
    -- =========================================================================
    PROCEDURE p_disable_fks (
        i_fks      IN  t_fk_tab,
        i_cnt      IN  PLS_INTEGER,
        i_run_id   IN  NUMBER,
        o_fail_cnt OUT PLS_INTEGER
    )
    IS
        v_sql VARCHAR2(512);
    BEGIN
        o_fail_cnt := 0;
        FOR i IN 1 .. i_cnt LOOP
            v_sql := 'ALTER TABLE '
                  || DBMS_ASSERT.ENQUOTE_NAME(i_fks(i).owner,      FALSE) || '.'
                  || DBMS_ASSERT.ENQUOTE_NAME(i_fks(i).table_name, FALSE)
                  || ' DISABLE CONSTRAINT '
                  || DBMS_ASSERT.ENQUOTE_NAME(i_fks(i).constraint_name, FALSE);
            BEGIN
                EXECUTE IMMEDIATE v_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    o_fail_cnt := o_fail_cnt + 1;
                    p_log(i_run_id,
                          i_fks(i).owner || '.' || i_fks(i).table_name,
                          'p_disable_fks',
                          'WARN: could not disable ' || i_fks(i).constraint_name
                          || ': ' || SQLERRM);
            END;
        END LOOP;
    END p_disable_fks;

    PROCEDURE p_enable_fks (
        i_fks    IN t_fk_tab,
        i_cnt    IN PLS_INTEGER,
        i_run_id IN NUMBER
    )
    IS
        v_sql VARCHAR2(512);
    BEGIN
        FOR i IN 1 .. i_cnt LOOP
            -- ENABLE NOVALIDATE: re-activates the constraint for future DML
            -- without performing a full-table historical validation.  Safe here
            -- because child rows will be updated in subsequent LVL steps.
            v_sql := 'ALTER TABLE '
                  || DBMS_ASSERT.ENQUOTE_NAME(i_fks(i).owner,      FALSE) || '.'
                  || DBMS_ASSERT.ENQUOTE_NAME(i_fks(i).table_name, FALSE)
                  || ' ENABLE NOVALIDATE CONSTRAINT '
                  || DBMS_ASSERT.ENQUOTE_NAME(i_fks(i).constraint_name, FALSE);
            BEGIN
                EXECUTE IMMEDIATE v_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    p_log(i_run_id,
                          i_fks(i).owner || '.' || i_fks(i).table_name,
                          'p_enable_fks',
                          'WARN: could not enable ' || i_fks(i).constraint_name
                          || ': ' || SQLERRM);
            END;
        END LOOP;
    END p_enable_fks;

    -- =========================================================================
    -- Private: f_pool_value
    -- Returns a random value from T_ANON_TOY_POOL for the given category.
    -- Returns NULL when the category has no entries.
    -- =========================================================================
    FUNCTION f_pool_value (i_category IN VARCHAR2)
    RETURN VARCHAR2
    IS
        v_val VARCHAR2(4000);
    BEGIN
        SELECT VALUE INTO v_val
        FROM (
            SELECT VALUE
            FROM   T_ANON_TOY_POOL
            WHERE  CATEGORY = i_category
            ORDER BY DBMS_RANDOM.VALUE
        )
        WHERE ROWNUM = 1;
        RETURN v_val;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN RETURN NULL;
    END f_pool_value;

    -- =========================================================================
    -- Private: f_scramble_value
    -- Generates a random replacement of the same type and approximate length.
    --
    -- DATE / TIMESTAMP: always returns 'YYYY-MM-DD HH24:MI:SS' format so that
    --   p_apply_map can safely cast the MAP value back to DATE or TIMESTAMP.
    -- NUMBER: random integer within the same order of magnitude.
    --   NOTE: ROUND with no precision argument always produces an integer,
    --   so decimal column structure (e.g. AMOUNT NUMBER(12,2)) is NOT
    --   preserved — replacements will be integers.  Use SHIFT to retain
    --   fractional structure via a percentage offset.
    -- VARCHAR2 / CHAR / other: random alphanumeric string of the same length.
    -- =========================================================================
    FUNCTION f_scramble_value (
        i_old_value IN VARCHAR2,
        i_data_type IN VARCHAR2
    ) RETURN VARCHAR2
    IS
        v_len    PLS_INTEGER;
        v_chars  VARCHAR2(62) := 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
        v_result VARCHAR2(4000) := '';
        v_num    NUMBER;
        v_mag    NUMBER;
    BEGIN
        IF i_old_value IS NULL THEN
            RETURN NULL;
        END IF;

        IF i_data_type = 'DATE' OR i_data_type LIKE 'TIMESTAMP%' THEN
            -- Random date in [1970-01-01, 2030-12-31] with random time component.
            -- Adding DBMS_RANDOM.VALUE(0,1) (a fraction of a day) randomises the
            -- HH24:MI:SS portion.  Sub-second precision (TIMESTAMP(6) fractional
            -- seconds) remains zero because the canonical MAP format 'YYYY-MM-DD
            -- HH24:MI:SS' carries no fractional component; this is a known
            -- limitation for TIMESTAMP(6) columns.
            RETURN TO_CHAR(
                       DATE '1970-01-01'
                       + TRUNC(DBMS_RANDOM.VALUE(0, 22280))
                       + DBMS_RANDOM.VALUE(0, 1),   -- random time within the day
                       'YYYY-MM-DD HH24:MI:SS');

        ELSIF i_data_type = 'NUMBER' THEN
            -- FM format with explicit '.' matches the NLS-independent mask used
            -- throughout the package.  Without a format mask, TO_NUMBER('3.14')
            -- raises ORA-01722 in EU sessions where the decimal separator is ','.
            v_num := TO_NUMBER(i_old_value,
                         'FM99999999999999999999999999990.999999999999');
            IF v_num = 0 THEN
                RETURN '0';
            END IF;
            v_mag    := POWER(10, FLOOR(LOG(10, ABS(v_num))));
            -- Upper bound is v_mag * 10 (not v_mag*10-1) so that fractional
            -- values (0 < ABS < 1) never produce an inverted range.
            -- Example: v_num=0.5 → v_mag=0.1 → VALUE(0.1, 1.0) is valid;
            -- the old formula gave VALUE(0.1, 0.0) → ORA-27337.
            v_result := TO_CHAR(ROUND(DBMS_RANDOM.VALUE(v_mag, v_mag * 10)));
            IF v_num < 0 THEN v_result := '-' || v_result; END IF;
            RETURN v_result;

        ELSE
            -- VARCHAR2, CHAR, and all other types: random alphanumeric string
            v_len := GREATEST(1, LEAST(4000, NVL(LENGTH(i_old_value), 8)));
            FOR i IN 1 .. v_len LOOP
                v_result := v_result
                    || SUBSTR(v_chars,
                               TRUNC(DBMS_RANDOM.VALUE(1, LENGTH(v_chars) + 1)),
                               1);
            END LOOP;
            RETURN v_result;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            -- Emergency fallback: return a type-safe value so p_apply_map can cast
            -- it back.  Log to DBMS_OUTPUT since no run_id is available here;
            -- the f_new_value WHEN OTHERS will also log to T_ANON_TOY_LOG.
            -- DBMS_OUTPUT is wrapped in its own handler: ORA-20000 (buffer
            -- overflow) inside a WHEN OTHERS block would otherwise escape the
            -- handler and cascade up to p_build_map_core, aborting the run.
            BEGIN
                DBMS_OUTPUT.PUT_LINE(
                    'WARN: f_scramble_value fallback for data_type='
                    || i_data_type || ' old_value=' || SUBSTR(i_old_value, 1, 50)
                    || ' error=' || SQLERRM);
            EXCEPTION
                WHEN OTHERS THEN NULL;  -- advisory only; buffer overflow must not abort the run
            END;
            IF i_data_type = 'NUMBER' THEN
                RETURN '0';
            ELSIF i_data_type = 'DATE' OR i_data_type LIKE 'TIMESTAMP%' THEN
                RETURN '2000-01-01 00:00:00';
            ELSE
                RETURN DBMS_RANDOM.STRING('X', 8);
            END IF;
    END f_scramble_value;

    -- =========================================================================
    -- Private: f_format_value
    -- Preserves the structural pattern of a value:
    --   digits (0-9)   → random digit
    --   uppercase (A-Z) → random uppercase letter
    --   lowercase (a-z) → random lowercase letter
    --   all other chars → kept unchanged (separators, spaces, +, /, etc.)
    -- =========================================================================
    FUNCTION f_format_value (i_old_value IN VARCHAR2) RETURN VARCHAR2
    IS
        v_result VARCHAR2(4000) := '';
        v_ch     CHAR(1);
        v_upper  VARCHAR2(26) := 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
        v_lower  VARCHAR2(26) := 'abcdefghijklmnopqrstuvwxyz';
    BEGIN
        IF i_old_value IS NULL THEN RETURN NULL; END IF;

        FOR i IN 1 .. LENGTH(i_old_value) LOOP
            v_ch := SUBSTR(i_old_value, i, 1);

            IF v_ch BETWEEN '0' AND '9' THEN
                v_result := v_result
                    || TO_CHAR(TRUNC(DBMS_RANDOM.VALUE(0, 10)));

            ELSIF v_ch BETWEEN 'A' AND 'Z' THEN
                v_result := v_result
                    || SUBSTR(v_upper, TRUNC(DBMS_RANDOM.VALUE(1, 27)), 1);

            ELSIF v_ch BETWEEN 'a' AND 'z' THEN
                v_result := v_result
                    || SUBSTR(v_lower, TRUNC(DBMS_RANDOM.VALUE(1, 27)), 1);

            ELSE
                v_result := v_result || v_ch;
            END IF;
        END LOOP;

        RETURN v_result;
    END f_format_value;

    -- =========================================================================
    -- Private: f_get_or_create_shift_offset
    -- Returns the per-column SHIFT offset, creating and persisting it on first
    -- call.  The offset is stored in T_ANON_TOY_MAP under
    -- OLD_VALUE = '__SHIFT_OFFSET__' so subsequent runs reuse the same offset,
    -- preserving relative ordering within the column across runs.
    --
    -- DATE / TIMESTAMP: integer days, range [-SHIFT_RANGE, +SHIFT_RANGE].
    -- NUMBER: decimal percentage, range [-SHIFT_RANGE%, +SHIFT_RANGE%].
    --
    -- NOTE: this function writes to T_ANON_TOY_MAP (the sentinel row) when the
    -- offset does not yet exist.  It is not autonomous — the insert is part of
    -- the caller's transaction and will be rolled back if the caller rolls back.
    -- =========================================================================
    FUNCTION f_get_or_create_shift_offset (
        i_schema      IN VARCHAR2,
        i_table       IN VARCHAR2,
        i_column      IN VARCHAR2,
        i_shift_range IN NUMBER,
        i_data_type   IN VARCHAR2,
        i_run_id      IN NUMBER
    ) RETURN VARCHAR2
    IS
        v_offset VARCHAR2(64);
        v_key    CONSTANT VARCHAR2(20) := '__SHIFT_OFFSET__';
    BEGIN
        -- NULL or non-positive SHIFT_RANGE means the column is misconfigured.
        -- Return '0' (a no-op shift) to avoid ORA-27337 from:
        --   NULL  → DBMS_RANDOM.VALUE(NULL, NULL)   — undefined
        --   0     → DBMS_RANDOM.VALUE(0, 0)         — upper = lower, invalid
        --   < 0   → DBMS_RANDOM.VALUE(n, -n)        — upper < lower, invalid
        -- Log a WARN so operators who skip p_analyze still see the problem.
        -- p_analyze also warns about this; the log here covers the case where
        -- p_build_map is called directly without a prior p_analyze run.
        IF i_shift_range IS NULL OR i_shift_range <= 0 THEN
            p_log(i_run_id, i_schema || '.' || i_table, 'f_get_or_create_shift_offset',
                  'WARN: column ' || i_column
                  || ' SHIFT_RANGE is '
                  || NVL(TO_CHAR(i_shift_range), 'NULL')
                  || ' (must be > 0); no shift will be applied — column values'
                  || ' will be left unchanged in MAP.');
            RETURN '0';
        END IF;

        BEGIN
            SELECT NEW_VALUE INTO v_offset
            FROM   T_ANON_TOY_MAP
            WHERE  SOURCE_SCHEMA = UPPER(i_schema)
            AND    SOURCE_TABLE  = UPPER(i_table)
            AND    SOURCE_COLUMN = UPPER(i_column)
            AND    OLD_VALUE     = v_key;
            RETURN v_offset;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
        END;

        -- Generate a new offset appropriate for the column's data type.
        -- TIMESTAMP columns in Oracle are reported as 'TIMESTAMP(n)' —
        -- the LIKE check correctly catches all timestamp variants.
        IF i_data_type = 'DATE' OR i_data_type LIKE 'TIMESTAMP%' THEN
            v_offset := TO_CHAR(ROUND(DBMS_RANDOM.VALUE(
                            -i_shift_range, i_shift_range)));
        ELSE
            -- NUMBER: percentage offset stored with two decimal places.
            -- FM format with explicit '.' keeps the sentinel NLS-independent
            -- so build and apply sessions may have different NLS settings.
            v_offset := TO_CHAR(ROUND(DBMS_RANDOM.VALUE(
                            -i_shift_range, i_shift_range), 2),
                            'FM99999999999999999999999999990.999999999999');
        END IF;

        BEGIN
            INSERT INTO T_ANON_TOY_MAP
                (SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, OLD_VALUE,
                 NEW_VALUE, ANON_METHOD, RUN_ID)
            VALUES
                (UPPER(i_schema), UPPER(i_table), UPPER(i_column), v_key,
                 v_offset, '__SENTINEL__', i_run_id);
                -- '__SENTINEL__' distinguishes this bookkeeping row from real
                -- SHIFT data mappings so audit queries on ANON_METHOD = 'SHIFT'
                -- return only genuine old→new value pairs, not offset metadata.
        EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN
                -- A concurrent session inserted the sentinel first; load its value
                -- so both sessions use the same offset for this column.
                BEGIN
                    SELECT NEW_VALUE INTO v_offset
                    FROM   T_ANON_TOY_MAP
                    WHERE  SOURCE_SCHEMA = UPPER(i_schema)
                    AND    SOURCE_TABLE  = UPPER(i_table)
                    AND    SOURCE_COLUMN = UPPER(i_column)
                    AND    OLD_VALUE     = v_key;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        -- Three-way race: the sentinel was deleted by a concurrent
                        -- p_clear_map between the DUP fire and this re-SELECT.
                        -- Generate a local offset using the same range and format
                        -- as the main path so DATE and NUMBER offsets remain
                        -- consistent in type, sign, and precision.
                        IF i_data_type = 'DATE' OR i_data_type LIKE 'TIMESTAMP%' THEN
                            v_offset := TO_CHAR(ROUND(DBMS_RANDOM.VALUE(
                                            -i_shift_range, i_shift_range)));
                        ELSE
                            v_offset := TO_CHAR(ROUND(DBMS_RANDOM.VALUE(
                                            -i_shift_range, i_shift_range), 2),
                                            'FM99999999999999999999999999990.999999999999');
                        END IF;
                END;
        END;

        RETURN v_offset;
    END f_get_or_create_shift_offset;

    -- =========================================================================
    -- Private: f_new_value
    -- Master value-generation function.  Returns the anonymized replacement
    -- string for one old_value + column configuration combination.
    --
    -- For most methods the caller writes the result to T_ANON_TOY_MAP.
    -- Exception: SHIFT also writes the per-column sentinel offset row to MAP
    -- via f_get_or_create_shift_offset (part of the same transaction).
    -- =========================================================================
    FUNCTION f_new_value (
        i_old_value  IN VARCHAR2,
        i_col        IN t_col_rec,
        i_schema     IN VARCHAR2,
        i_table      IN VARCHAR2,
        i_data_type  IN VARCHAR2,
        i_run_id     IN NUMBER
    ) RETURN VARCHAR2
    IS
        v_new    VARCHAR2(4000);
        v_offset NUMBER;
        v_num    NUMBER;
        v_date   DATE;
        v_test   VARCHAR2(4000);
    BEGIN
        IF i_old_value IS NULL THEN
            RETURN NULL;
        END IF;

        CASE i_col.anon_method

            WHEN 'NULL_VAL' THEN
                -- Dead code: p_build_map_core skips NULL_VAL columns with
                -- CONTINUE before calling f_new_value, so this branch is
                -- unreachable from the current caller.  Retained as a safety
                -- net for any future direct caller that does not pre-filter
                -- NULL_VAL — if reached it stores NULL in MAP, which would
                -- cause p_apply_map to SET the column to NULL for all matching
                -- rows.  Any future caller must handle this explicitly.
                RETURN NULL;

            WHEN 'POOL' THEN
                v_new := f_pool_value(i_col.data_category);
                RETURN NVL(v_new, f_scramble_value(i_old_value, i_data_type));

            WHEN 'SCRAMBLE' THEN
                RETURN f_scramble_value(i_old_value, i_data_type);

            WHEN 'FORMAT' THEN
                RETURN f_format_value(i_old_value);

            WHEN 'SHIFT' THEN
                -- FM format mask required: the sentinel is stored with the same
                -- mask (period decimal, NLS-independent).  Without a mask,
                -- TO_NUMBER('12.34') raises ORA-01722 in EU-locale sessions.
                -- DATE/TIMESTAMP offsets are integer strings so both masks work,
                -- but using the FM mask consistently is harmless and safe.
                v_offset := TO_NUMBER(
                    f_get_or_create_shift_offset(
                        i_schema, i_table, i_col.column_name,
                        i_col.shift_range, i_data_type, i_run_id),
                    'FM99999999999999999999999999990.999999999999');

                -- LIKE 'TIMESTAMP%' catches 'TIMESTAMP(6)', 'TIMESTAMP(3)', etc.
                IF i_data_type = 'DATE' OR i_data_type LIKE 'TIMESTAMP%' THEN
                    v_date := TO_DATE(i_old_value, 'YYYY-MM-DD HH24:MI:SS');
                    RETURN TO_CHAR(v_date + v_offset, 'YYYY-MM-DD HH24:MI:SS');
                ELSE
                    -- i_old_value is FM-formatted (period decimal); use the same
                    -- mask so this works in EU-locale sessions too.  Return value
                    -- uses FM format to match p_apply_map_core's TO_NUMBER cast.
                    v_num := TO_NUMBER(i_old_value,
                                 'FM99999999999999999999999999990.999999999999');
                    RETURN TO_CHAR(ROUND(v_num * (1 + v_offset / 100), 10),
                                   'FM99999999999999999999999999990.999999999999');
                END IF;

            WHEN 'CUSTOM' THEN
                EXECUTE IMMEDIATE i_col.custom_expr INTO v_test;
                RETURN v_test;

            ELSE
                -- Unknown ANON_METHOD: log a WARN so the operator can identify
                -- misconfigured rows.  p_analyze logs the same condition; this
                -- guard covers the case where p_build_map is called directly
                -- without a prior p_analyze run.
                p_log(i_run_id, i_schema || '.' || i_table, 'f_new_value',
                      'WARN: unknown anon_method=' || i_col.anon_method
                      || ' column=' || i_col.column_name
                      || ' — falling back to SCRAMBLE');
                RETURN f_scramble_value(i_old_value, i_data_type);
        END CASE;

    EXCEPTION
        WHEN OTHERS THEN
            -- Log the fallback to T_ANON_TOY_LOG before returning so systematic
            -- failures are visible.  Without this, every value for the column
            -- would silently receive the type-safe placeholder with no operator alert.
            p_log(i_run_id, i_schema || '.' || i_table, 'f_new_value',
                  'WARN: fallback fired for column ' || i_col.column_name
                  || ' old_value=' || SUBSTR(i_old_value, 1, 50)
                  || ' error=' || SQLERRM);
            RETURN f_scramble_value(i_old_value, i_data_type);
    END f_new_value;

    -- =========================================================================
    -- Public: p_enable / p_enable_all / p_clear_map / p_clear_map_all
    -- =========================================================================
    PROCEDURE p_enable (i_schema IN VARCHAR2, i_table_name IN VARCHAR2 DEFAULT NULL)
    -- NOTE: always issues an unconditional COMMIT.  Any open DML in the calling
    -- session will be committed when p_enable is called.
    IS
        v_rows PLS_INTEGER;
    BEGIN
        IF i_table_name IS NULL THEN
            -- Schema-scope: enable all tables in the schema
            UPDATE T_ANON_TOY_CMD
            SET    IF_ANONYMIZE = 1
            WHERE  UPPER(TABLE_SCHEMA) = UPPER(i_schema);
        ELSE
            UPDATE T_ANON_TOY_CMD
            SET    IF_ANONYMIZE = 1
            WHERE  UPPER(TABLE_SCHEMA) = UPPER(i_schema)
            AND    UPPER(TABLE_NAME)   = UPPER(i_table_name);
        END IF;
        v_rows := SQL%ROWCOUNT;
        COMMIT;  -- COMMIT before DBMS_OUTPUT: a buffer-overflow on the advisory
                 -- message must not prevent the UPDATE from being persisted
        IF v_rows = 0 THEN
            DBMS_OUTPUT.PUT_LINE(
                'WARNING: p_enable matched 0 rows for schema='
                || i_schema
                || CASE WHEN i_table_name IS NOT NULL
                        THEN ' table=' || i_table_name ELSE '' END
                || ' — verify the name(s) exist in T_ANON_TOY_CMD');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in p_enable: ' || SQLERRM);
            RAISE;
    END p_enable;

    PROCEDURE p_enable_all
    IS
    BEGIN
        UPDATE T_ANON_TOY_CMD SET IF_ANONYMIZE = 1;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in p_enable_all: ' || SQLERRM);
            RAISE;
    END p_enable_all;

    PROCEDURE p_clear_map (i_schema IN VARCHAR2, i_table_name IN VARCHAR2 DEFAULT NULL)
    -- NOTE: always issues an unconditional COMMIT.  Any open DML in the calling
    -- session will be committed when p_clear_map is called.
    IS
    -- WARNING — SHIFT sentinel rows:
    -- f_get_or_create_shift_offset stores a '__SHIFT_OFFSET__' sentinel in
    -- T_ANON_TOY_MAP (ANON_METHOD='__SENTINEL__') to persist the per-column
    -- SHIFT offset across runs.  p_clear_map deletes ALL rows for the
    -- schema/table, including this sentinel.  After p_clear_map + p_build_map,
    -- SHIFT columns will receive a NEW random offset.  Any data anonymized in
    -- an earlier run will have a different offset from data anonymized in the
    -- new run, silently breaking relative ordering across runs.
    -- If consistent SHIFT ordering across runs is required, do NOT call
    -- p_clear_map for tables that have SHIFT columns.
        v_rows PLS_INTEGER;
    BEGIN
        IF i_table_name IS NULL THEN
            -- Schema-scope: clear MAP entries for all tables in the schema
            DELETE FROM T_ANON_TOY_MAP
            WHERE  UPPER(SOURCE_SCHEMA) = UPPER(i_schema);
        ELSE
            DELETE FROM T_ANON_TOY_MAP
            WHERE  UPPER(SOURCE_SCHEMA) = UPPER(i_schema)
            AND    UPPER(SOURCE_TABLE)  = UPPER(i_table_name);
        END IF;
        v_rows := SQL%ROWCOUNT;
        COMMIT;  -- COMMIT before DBMS_OUTPUT: a buffer-overflow on the advisory
                 -- message must not prevent the DELETE from being persisted
        IF v_rows = 0 THEN
            DBMS_OUTPUT.PUT_LINE(
                'WARNING: p_clear_map deleted 0 rows for schema='
                || i_schema
                || CASE WHEN i_table_name IS NOT NULL
                        THEN ' table=' || i_table_name ELSE '' END
                || ' — MAP may already be empty or the name(s) may not match');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in p_clear_map: ' || SQLERRM);
            RAISE;
    END p_clear_map;

    PROCEDURE p_clear_map_all
    IS
    BEGIN
        DELETE FROM T_ANON_TOY_MAP;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in p_clear_map_all: ' || SQLERRM);
            RAISE;
    END p_clear_map_all;

    -- =========================================================================
    -- Public: p_analyze
    -- Dry-run: validates configuration and tests CUSTOM_EXPR expressions.
    -- No DML is performed against target tables.
    -- =========================================================================
    PROCEDURE p_analyze (
        i_schema     IN VARCHAR2 DEFAULT NULL,
        i_table_name IN VARCHAR2 DEFAULT NULL
    )
    IS
        v_run_id     NUMBER;
        v_cmds       t_cmd_tab;
        v_cmd_cnt    PLS_INTEGER;
        v_cols       t_col_tab;
        v_col_cnt    PLS_INTEGER;
        v_tbl        VARCHAR2(128);
        v_tbl_count  NUMBER;
        v_col_exists NUMBER;
        v_nullable   VARCHAR2(1);
        v_pool_cnt   NUMBER;
        v_map_cnt    NUMBER;
        v_test_val   VARCHAR2(4000);
        v_dummy      NUMBER;
    BEGIN
        BEGIN
            v_run_id := f_next_run_id();
        EXCEPTION
            WHEN OTHERS THEN
                -- Sequence failure before any run_id exists; cannot log to table.
                -- Emit to DBMS_OUTPUT and re-raise with context.
                DBMS_OUTPUT.PUT_LINE(
                    'FATAL: PKG_ANON_TOY.p_analyze sequence failure: ' || SQLERRM);
                RAISE;
        END;

        v_dummy := f_log(v_run_id, NULL, 'p_analyze', 'START');

        -- Privilege pre-flight: verify the definer holds the system privileges
        -- the package requires.  A missing privilege causes a mid-run FATAL with
        -- no upfront warning.  SESSION_PRIVS includes privileges granted via roles.
        DECLARE
            v_priv_cnt NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_priv_cnt
            FROM   SESSION_PRIVS
            WHERE  PRIVILEGE = 'SELECT ANY DICTIONARY';
            IF v_priv_cnt = 0 THEN
                v_dummy := f_log(v_run_id, NULL, 'p_analyze',
                    'WARN: SELECT ANY DICTIONARY not granted'
                    || ' — DBA_TABLES / DBA_TAB_COLUMNS / DBA_CONSTRAINTS'
                    || ' queries will fail with ORA-00942');
            END IF;

            SELECT COUNT(*) INTO v_priv_cnt
            FROM   SESSION_PRIVS
            WHERE  PRIVILEGE = 'ALTER ANY TABLE';
            IF v_priv_cnt = 0 THEN
                v_dummy := f_log(v_run_id, NULL, 'p_analyze',
                    'WARN: ALTER ANY TABLE not granted'
                    || ' — FK DISABLE / ENABLE NOVALIDATE will fail with ORA-01031');
            END IF;

            SELECT COUNT(*) INTO v_priv_cnt
            FROM   SESSION_PRIVS
            WHERE  PRIVILEGE = 'UPDATE ANY TABLE';
            IF v_priv_cnt = 0 THEN
                v_dummy := f_log(v_run_id, NULL, 'p_analyze',
                    'WARN: UPDATE ANY TABLE not granted'
                    || ' — correlated UPDATE in p_apply_map will fail with ORA-01031');
            END IF;

            SELECT COUNT(*) INTO v_priv_cnt
            FROM   SESSION_PRIVS
            WHERE  PRIVILEGE = 'SELECT ANY TABLE';
            IF v_priv_cnt = 0 THEN
                v_dummy := f_log(v_run_id, NULL, 'p_analyze',
                    'WARN: SELECT ANY TABLE not granted'
                    || ' — DISTINCT value fetch in p_build_map will fail with ORA-00942');
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                v_dummy := f_log(v_run_id, NULL, 'p_analyze',
                    'WARN: privilege pre-flight check failed: ' || SQLERRM
                    || ' — continuing; missing privileges may cause later errors');
        END;

        get_cmd_rows(i_schema, i_table_name, v_cmds, v_cmd_cnt);

        IF v_cmd_cnt = 0 THEN
            v_dummy := f_log(v_run_id, NULL, 'p_analyze',
                             'WARN: no CMD rows found with IF_ANONYMIZE=1');
            RETURN;
        END IF;

        FOR c IN 1 .. v_cmd_cnt LOOP
            v_tbl := v_cmds(c).table_schema || '.' || v_cmds(c).table_name;
            v_dummy := f_log(v_run_id, v_tbl, 'p_analyze', 'Checking table');

            -- Verify the table exists in DBA_TABLES
            BEGIN
                SELECT COUNT(*) INTO v_tbl_count
                FROM   DBA_TABLES
                WHERE  UPPER(OWNER)      = UPPER(v_cmds(c).table_schema)
                AND    UPPER(TABLE_NAME) = UPPER(v_cmds(c).table_name);

                IF v_tbl_count = 0 THEN
                    v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                     'WARN: table not found in DBA_TABLES');
                    CONTINUE;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                     'ERROR: DBA_TABLES check failed: ' || SQLERRM);
                    CONTINUE;
            END;

            get_col_rows(v_cmds(c).table_schema, v_cmds(c).table_name,
                         v_cols, v_col_cnt);

            IF v_col_cnt = 0 THEN
                v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                 'WARN: no COLS rows found (method <> KEEP)');
            ELSE
                v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                 'INFO: ' || v_col_cnt || ' column(s) configured');
            END IF;

            FOR i IN 1 .. v_col_cnt LOOP

                -- Verify the column exists; catches typos in COLUMN_NAME before
                -- p_build_map silently skips them with only a WARN entry.
                BEGIN
                    SELECT COUNT(*) INTO v_col_exists
                    FROM   DBA_TAB_COLUMNS
                    WHERE  UPPER(OWNER)       = UPPER(v_cmds(c).table_schema)
                    AND    UPPER(TABLE_NAME)  = UPPER(v_cmds(c).table_name)
                    AND    UPPER(COLUMN_NAME) = UPPER(v_cols(i).column_name);
                EXCEPTION
                    WHEN OTHERS THEN
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'ERROR: DBA_TAB_COLUMNS check failed for column '
                            || v_cols(i).column_name || ': ' || SQLERRM
                            || ' — column skipped');
                        CONTINUE;
                END;

                IF v_col_exists = 0 THEN
                    v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                        'WARN: column ' || v_cols(i).column_name
                        || ' not found in DBA_TAB_COLUMNS'
                        || ' — check for a typo in T_ANON_TOY_COLS');
                    CONTINUE;
                END IF;

                -- MAP_SOURCE (application-logic FK) check first: these columns
                -- derive their replacement from a parent column's MAP entries.
                IF v_cols(i).map_source_schema IS NOT NULL THEN

                    -- N-6: Partial MAP_SOURCE configuration check.
                    -- All three MAP_SOURCE_* columns must be set together.
                    -- A partial fill (e.g. only schema+table but no column) will
                    -- cause the apply phase to look up a MAP entry with a NULL
                    -- SOURCE_COLUMN, returning no rows and silently skipping the
                    -- column.
                    IF    v_cols(i).map_source_table  IS NULL
                       OR v_cols(i).map_source_column IS NULL
                    THEN
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'WARN: column ' || v_cols(i).column_name
                            || ' has a partial MAP_SOURCE configuration'
                            || ' (MAP_SOURCE_SCHEMA is set but MAP_SOURCE_TABLE'
                            || ' or MAP_SOURCE_COLUMN is NULL);'
                            || ' all three MAP_SOURCE_* columns must be populated'
                            || ' — column will be silently skipped at apply time');
                        CONTINUE;
                    END IF;

                    -- N-7: ANON_METHOD set alongside MAP_SOURCE (method is ignored).
                    -- For MAP_SOURCE columns the apply phase always looks up the
                    -- parent's MAP entries; ANON_METHOD is never evaluated.
                    -- Log an INFO to avoid silent user confusion.
                    IF v_cols(i).anon_method NOT IN ('NULL_VAL', 'KEEP') THEN
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'INFO: column ' || v_cols(i).column_name
                            || ' has ANON_METHOD=' || v_cols(i).anon_method
                            || ' but MAP_SOURCE is also set;'
                            || ' the method is ignored for MAP_SOURCE columns'
                            || ' — the parent column''s mapping is reused instead');
                    END IF;

                    -- Detect MAP_SOURCE + NULL_VAL conflict: at apply time,
                    -- p_apply_map_core checks NULL_VAL BEFORE MAP_SOURCE, so
                    -- NULL_VAL takes precedence and MAP_SOURCE is silently ignored.
                    -- Validating the MAP_SOURCE for a column that will be NULLed
                    -- wastes effort and can produce misleading WARN entries.
                    IF v_cols(i).anon_method = 'NULL_VAL' THEN
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'WARN: column ' || v_cols(i).column_name
                            || ' has both MAP_SOURCE and ANON_METHOD=NULL_VAL;'
                            || ' NULL_VAL takes precedence at apply time and'
                            || ' MAP_SOURCE will be ignored — remove MAP_SOURCE'
                            || ' or change ANON_METHOD');
                        CONTINUE;
                    END IF;

                    -- Check whether parent MAP entries already exist.
                    -- Exclude the SHIFT sentinel row (__SHIFT_OFFSET__) so the
                    -- count reflects actual data mappings, not internal bookkeeping.
                    BEGIN
                        SELECT COUNT(*) INTO v_map_cnt
                        FROM   T_ANON_TOY_MAP
                        WHERE  UPPER(SOURCE_SCHEMA) = UPPER(v_cols(i).map_source_schema)
                        AND    UPPER(SOURCE_TABLE)  = UPPER(v_cols(i).map_source_table)
                        AND    UPPER(SOURCE_COLUMN) = UPPER(v_cols(i).map_source_column)
                        AND    OLD_VALUE            <> '__SHIFT_OFFSET__';
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                'ERROR: T_ANON_TOY_MAP check failed for column '
                                || v_cols(i).column_name || ': ' || SQLERRM
                                || ' — column skipped');
                            CONTINUE;
                    END;

                    IF v_map_cnt = 0 THEN
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'WARN: column ' || v_cols(i).column_name
                            || ' MAP_SOURCE points to '
                            || v_cols(i).map_source_schema || '.'
                            || v_cols(i).map_source_table  || '.'
                            || v_cols(i).map_source_column
                            || ' but no MAP entries exist yet for that source;'
                            || ' run p_build_map for the parent table first');
                    ELSE
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'INFO: column ' || v_cols(i).column_name
                            || ' MAP_SOURCE OK ('
                            || v_map_cnt || ' mapping(s) available)');
                    END IF;

                    -- LVL ordering check: parent table must have a lower LVL
                    -- than this (child) table so it is processed first.  Wrong
                    -- ordering causes ORA-02291 when p_apply_map updates the
                    -- child FK column before the parent PK has been updated.
                    DECLARE
                        v_parent_lvl NUMBER;
                        v_child_lvl  NUMBER;
                    BEGIN
                        SELECT LVL INTO v_parent_lvl
                        FROM   T_ANON_TOY_CMD
                        WHERE  UPPER(TABLE_SCHEMA) = UPPER(v_cols(i).map_source_schema)
                        AND    UPPER(TABLE_NAME)   = UPPER(v_cols(i).map_source_table);

                        SELECT LVL INTO v_child_lvl
                        FROM   T_ANON_TOY_CMD
                        WHERE  UPPER(TABLE_SCHEMA) = UPPER(v_cmds(c).table_schema)
                        AND    UPPER(TABLE_NAME)   = UPPER(v_cmds(c).table_name);

                        IF v_parent_lvl >= v_child_lvl THEN
                            v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                'WARN: column ' || v_cols(i).column_name
                                || ': MAP_SOURCE parent '
                                || v_cols(i).map_source_schema || '.'
                                || v_cols(i).map_source_table
                                || ' has LVL ' || v_parent_lvl
                                || ' >= current table LVL ' || v_child_lvl
                                || ' — parent must be processed first;'
                                || ' ORA-02291 will occur at apply time unless'
                                || ' LVL values are corrected in T_ANON_TOY_CMD');
                        ELSE
                            v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                'INFO: column ' || v_cols(i).column_name
                                || ' LVL order OK (parent LVL ' || v_parent_lvl
                                || ' < child LVL ' || v_child_lvl || ')');
                        END IF;
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN NULL;
                        -- Parent or child not in T_ANON_TOY_CMD; the missing-
                        -- table check above will flag this separately.
                    END;

                ELSIF v_cols(i).anon_method = 'POOL' THEN
                    -- Verify category has entries; report actual count
                    BEGIN
                        SELECT COUNT(*) INTO v_pool_cnt
                        FROM   T_ANON_TOY_POOL
                        WHERE  CATEGORY = v_cols(i).data_category;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                'ERROR: T_ANON_TOY_POOL check failed for column '
                                || v_cols(i).column_name || ': ' || SQLERRM
                                || ' — column skipped');
                            CONTINUE;
                    END;

                    IF v_pool_cnt = 0 THEN
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'WARN: column ' || v_cols(i).column_name
                            || ' POOL category "' || v_cols(i).data_category
                            || '" has 0 entries; SCRAMBLE fallback will be used');
                    ELSE
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'INFO: column ' || v_cols(i).column_name
                            || ' POOL category "' || v_cols(i).data_category
                            || '" has ' || v_pool_cnt || ' entries');

                        -- Check that pool values fit within the target column's
                        -- declared width.  A pool value wider than DATA_LENGTH
                        -- causes ORA-12899 at apply time.
                        DECLARE
                            v_max_pool_len NUMBER;
                            v_col_data_len NUMBER;
                        BEGIN
                            SELECT MAX(LENGTH(VALUE)) INTO v_max_pool_len
                            FROM   T_ANON_TOY_POOL
                            WHERE  CATEGORY = v_cols(i).data_category;

                            SELECT DATA_LENGTH INTO v_col_data_len
                            FROM   DBA_TAB_COLUMNS
                            WHERE  UPPER(OWNER)       = UPPER(v_cmds(c).table_schema)
                            AND    UPPER(TABLE_NAME)  = UPPER(v_cmds(c).table_name)
                            AND    UPPER(COLUMN_NAME) = UPPER(v_cols(i).column_name);

                            IF v_max_pool_len > v_col_data_len THEN
                                v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                    'WARN: column ' || v_cols(i).column_name
                                    || ' POOL category "' || v_cols(i).data_category
                                    || '" max value length ' || v_max_pool_len
                                    || ' exceeds column DATA_LENGTH ' || v_col_data_len
                                    || ' — ORA-12899 will occur at apply time;'
                                    || ' trim pool values or widen the target column');
                            END IF;
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN NULL; -- missing column flagged above
                        END;
                    END IF;

                ELSIF v_cols(i).anon_method = 'CUSTOM' THEN
                    -- Test-execute the expression before any real data is touched
                    IF v_cols(i).custom_expr IS NULL THEN
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'WARN: column ' || v_cols(i).column_name
                            || ' CUSTOM method but CUSTOM_EXPR is NULL');
                    ELSE
                        -- SAVEPOINT / ROLLBACK ensures p_analyze is a genuine
                        -- dry-run: any DML side effects in the CUSTOM_EXPR
                        -- (e.g. an expression that writes an audit row) are
                        -- rolled back after the test.  f_log uses an autonomous
                        -- transaction and is unaffected by this ROLLBACK.
                        -- Savepoint name uses double underscores + package prefix
                        -- to avoid collision with any caller-owned savepoint.
                        SAVEPOINT __anon_pkg_cust_test__;
                        BEGIN
                            EXECUTE IMMEDIATE v_cols(i).custom_expr INTO v_test_val;
                            v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                'INFO: column ' || v_cols(i).column_name
                                || ' CUSTOM_EXPR test OK (sample: '
                                || SUBSTR(v_test_val, 1, 80) || ')');
                        EXCEPTION
                            WHEN OTHERS THEN
                                v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                    'WARN: column ' || v_cols(i).column_name
                                    || ' CUSTOM_EXPR test FAILED: ' || SQLERRM
                                    || ' | Expr: '
                                    || SUBSTR(v_cols(i).custom_expr, 1, 200));
                        END;
                        ROLLBACK TO SAVEPOINT __anon_pkg_cust_test__;
                    END IF;

                ELSIF v_cols(i).anon_method = 'SHIFT' THEN
                    IF v_cols(i).shift_range IS NULL OR v_cols(i).shift_range <= 0 THEN
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'WARN: column ' || v_cols(i).column_name
                            || ' SHIFT method but SHIFT_RANGE is '
                            || NVL(TO_CHAR(v_cols(i).shift_range), 'NULL')
                            || ' (must be > 0); column will be left unchanged.'
                            || ' Set a positive SHIFT_RANGE in T_ANON_TOY_COLS.');
                    ELSE
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'INFO: column ' || v_cols(i).column_name
                            || ' SHIFT SHIFT_RANGE = ' || v_cols(i).shift_range);
                    END IF;

                ELSIF v_cols(i).anon_method = 'SCRAMBLE' THEN
                    v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                        'INFO: column ' || v_cols(i).column_name || ' SCRAMBLE');

                ELSIF v_cols(i).anon_method = 'FORMAT' THEN
                    -- FORMAT is a character-level structure-preserving randomiser;
                    -- it is only safe for VARCHAR2/CHAR/NVARCHAR2/NCHAR columns.
                    -- On DATE columns it produces a date-shaped but invalid string
                    -- (e.g. '7381-23-95') causing ORA-01843 at apply time.
                    DECLARE
                        v_fmt_type VARCHAR2(64);
                    BEGIN
                        SELECT DATA_TYPE INTO v_fmt_type
                        FROM   DBA_TAB_COLUMNS
                        WHERE  UPPER(OWNER)       = UPPER(v_cmds(c).table_schema)
                        AND    UPPER(TABLE_NAME)  = UPPER(v_cmds(c).table_name)
                        AND    UPPER(COLUMN_NAME) = UPPER(v_cols(i).column_name);

                        IF v_fmt_type NOT IN ('VARCHAR2','CHAR','NVARCHAR2','NCHAR') THEN
                            v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                'WARN: column ' || v_cols(i).column_name
                                || ' FORMAT method on ' || v_fmt_type || ' column'
                                || ' — FORMAT is only safe for VARCHAR2/CHAR types;'
                                || ' use SCRAMBLE for ' || v_fmt_type || ' columns'
                                || ' to avoid ORA-01843 / ORA-01722 at apply time');
                        ELSE
                            v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                'INFO: column ' || v_cols(i).column_name || ' FORMAT');
                        END IF;
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN NULL; -- missing column flagged above
                    END;

                ELSIF v_cols(i).anon_method = 'NULL_VAL' THEN
                    -- Warn if the column has a NOT NULL constraint: NULL_VAL
                    -- would raise ORA-01407 at apply time.
                    BEGIN
                        SELECT NULLABLE INTO v_nullable
                        FROM   DBA_TAB_COLUMNS
                        WHERE  UPPER(OWNER)       = UPPER(v_cmds(c).table_schema)
                        AND    UPPER(TABLE_NAME)  = UPPER(v_cmds(c).table_name)
                        AND    UPPER(COLUMN_NAME) = UPPER(v_cols(i).column_name);
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN NULL; -- missing column flagged above
                        WHEN OTHERS THEN
                            v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                                'ERROR: DBA_TAB_COLUMNS nullable check failed for column '
                                || v_cols(i).column_name || ': ' || SQLERRM
                                || ' — column skipped');
                            CONTINUE;
                    END;

                    IF v_nullable = 'N' THEN
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'WARN: column ' || v_cols(i).column_name
                            || ' NULL_VAL on a NOT NULL column'
                            || ' — ORA-01407 will occur at apply time;'
                            || ' change the method or drop the NOT NULL constraint');
                    ELSE
                        v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                            'INFO: column ' || v_cols(i).column_name
                            || ' NULL_VAL — all non-null values will be set to NULL');
                    END IF;

                ELSE
                    -- Unrecognised method: warn before wasting a full run.
                    -- Known methods: POOL SCRAMBLE SHIFT FORMAT NULL_VAL CUSTOM KEEP.
                    -- KEEP is excluded by get_col_rows so it never reaches here.
                    v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                        'WARN: column ' || v_cols(i).column_name
                        || ' unknown ANON_METHOD "' || v_cols(i).anon_method
                        || '" — will fall through to SCRAMBLE at run time;'
                        || ' check for a typo in T_ANON_TOY_COLS');
                END IF;

            END LOOP; -- columns

        END LOOP; -- tables

        v_dummy := f_log(v_run_id, NULL, 'p_analyze', 'END');

    EXCEPTION
        WHEN OTHERS THEN
            g_fatal_occurred := TRUE;
            v_dummy := f_log(v_run_id, v_tbl, 'p_analyze',
                             'FATAL: ' || SQLERRM || ' | '
                             || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
            RAISE;
    END p_analyze;

    -- =========================================================================
    -- Private: p_build_map_core
    -- Phase 1 implementation.  Accepts an externally generated RUN_ID so that
    -- p_run can share one RUN_ID across both build and apply phases.
    -- =========================================================================
    PROCEDURE p_build_map_core (
        i_schema     IN VARCHAR2,
        i_table_name IN VARCHAR2,
        i_run_id     IN NUMBER
    )
    IS
        v_cmds       t_cmd_tab;
        v_cmd_cnt    PLS_INTEGER;
        v_cols       t_col_tab;
        v_col_cnt    PLS_INTEGER;
        v_tbl        VARCHAR2(128);
        v_safe_tbl   VARCHAR2(200);
        v_safe_col   VARCHAR2(200);
        v_sql        VARCHAR2(32767);
        v_old_val    VARCHAR2(4000);
        v_new_val    VARCHAR2(4000);
        v_data_type  VARCHAR2(64);
        v_row_count  NUMBER;

        -- Associative array used to pre-load existing MAP keys for the current
        -- column before the distinct-value cursor loop.  One bulk SELECT replaces
        -- N individual SELECT COUNT(*) round-trips (one per distinct value).
        TYPE t_str_set IS TABLE OF PLS_INTEGER INDEX BY VARCHAR2(4000);
        v_existing_map t_str_set;

        TYPE t_refcur IS REF CURSOR;
        v_cur        t_refcur;
    BEGIN
        p_log(i_run_id, NULL, 'p_build_map', 'START');
        get_cmd_rows(i_schema, i_table_name, v_cmds, v_cmd_cnt);

        IF v_cmd_cnt = 0 THEN
            p_log(i_run_id, NULL, 'p_build_map',
                  'WARN: no CMD rows found with IF_ANONYMIZE=1');
            RETURN;
        END IF;

        FOR c IN 1 .. v_cmd_cnt LOOP
            v_tbl := v_cmds(c).table_schema || '.' || v_cmds(c).table_name;
            p_log(i_run_id, v_tbl, 'p_build_map', 'Building map for table');

            v_safe_tbl := DBMS_ASSERT.ENQUOTE_NAME(v_cmds(c).table_schema, FALSE)
                       || '.' ||
                          DBMS_ASSERT.ENQUOTE_NAME(v_cmds(c).table_name, FALSE);

            get_col_rows(v_cmds(c).table_schema, v_cmds(c).table_name,
                         v_cols, v_col_cnt);

            FOR i IN 1 .. v_col_cnt LOOP

                -- MAP_SOURCE columns derive their mapping from another column;
                -- no own mapping needs to be built here
                IF v_cols(i).map_source_schema IS NOT NULL THEN
                    p_log(i_run_id, v_tbl, 'p_build_map',
                          'INFO: column ' || v_cols(i).column_name
                          || ' uses MAP_SOURCE — skipping own mapping');
                    CONTINUE;
                END IF;

                -- NULL_VAL columns need no mapping
                IF v_cols(i).anon_method = 'NULL_VAL' THEN
                    CONTINUE;
                END IF;

                -- Resolve Oracle data type for this column
                BEGIN
                    SELECT DATA_TYPE INTO v_data_type
                    FROM   DBA_TAB_COLUMNS
                    WHERE  UPPER(OWNER)       = UPPER(v_cmds(c).table_schema)
                    AND    UPPER(TABLE_NAME)  = UPPER(v_cmds(c).table_name)
                    AND    UPPER(COLUMN_NAME) = UPPER(v_cols(i).column_name);
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        p_log(i_run_id, v_tbl, 'p_build_map',
                              'WARN: column ' || v_cols(i).column_name
                              || ' not found in DBA_TAB_COLUMNS — skipped');
                        CONTINUE;
                END;

                v_safe_col := DBMS_ASSERT.ENQUOTE_NAME(v_cols(i).column_name, FALSE);

                -- Build the DISTINCT value query.
                -- DATE / TIMESTAMP: canonical 'YYYY-MM-DD HH24:MI:SS' format so
                --   SHIFT arithmetic and the p_apply_map back-cast are consistent.
                -- NUMBER: FM format with an explicit '.' decimal element.
                --   In Oracle, a literal '.' in a format mask is always a
                --   period regardless of NLS_NUMERIC_CHARACTERS, so the
                --   resulting string is the same whether the session uses
                --   US (period) or EU (comma) locale.  The FM modifier
                --   suppresses leading spaces, leading zeros, and the
                --   trailing decimal point for integers, giving the same
                --   compact output as TM9 without NLS sensitivity.
                --   Build and apply may therefore run in sessions with
                --   different NLS settings.
                --
                -- UPGRADE NOTE (2026-03-23): the prior version used
                --   TO_CHAR(col, 'TM9'), which is NLS-dependent.
                --   The TM9 and FM formats produce DIFFERENT strings for
                --   fractional numbers (e.g. EU locale: '3,14' vs '3.14').
                --   T_ANON_TOY_MAP entries built by the old code are
                --   INCOMPATIBLE with this version's apply queries.
                --   ACTION: if upgrading from a pre-2026-03-23 version,
                --   call p_clear_map(<schema>) then p_build_map(<schema>)
                --   before running p_apply_map.  Running p_apply_map
                --   against old TM9-format MAP data silently updates 0
                --   rows for NUMBER columns (no error is raised).
                -- VARCHAR2 / other: plain TO_CHAR cast for consistency.
                IF v_data_type = 'DATE' OR v_data_type LIKE 'TIMESTAMP%' THEN
                    v_sql := 'SELECT DISTINCT TO_CHAR(' || v_safe_col
                          || ', ''YYYY-MM-DD HH24:MI:SS'') FROM ' || v_safe_tbl
                          || ' WHERE ' || v_safe_col || ' IS NOT NULL';
                ELSIF v_data_type = 'NUMBER' THEN
                    v_sql := 'SELECT DISTINCT TO_CHAR(' || v_safe_col
                          || ', ''FM99999999999999999999999999990.999999999999'')'
                          || ' FROM ' || v_safe_tbl
                          || ' WHERE ' || v_safe_col || ' IS NOT NULL';
                ELSE
                    v_sql := 'SELECT DISTINCT TO_CHAR(' || v_safe_col
                          || ') FROM ' || v_safe_tbl
                          || ' WHERE ' || v_safe_col || ' IS NOT NULL';
                END IF;

                -- Pre-load all existing MAP keys for this column into an
                -- associative array.  Replaces N individual SELECT COUNT(*) calls
                -- (one per distinct value) with a single bulk query.
                v_existing_map.DELETE;
                FOR r IN (
                    SELECT OLD_VALUE
                    FROM   T_ANON_TOY_MAP
                    WHERE  SOURCE_SCHEMA = UPPER(v_cmds(c).table_schema)
                    AND    SOURCE_TABLE  = UPPER(v_cmds(c).table_name)
                    AND    SOURCE_COLUMN = UPPER(v_cols(i).column_name)
                ) LOOP
                    v_existing_map(r.OLD_VALUE) := 1;
                END LOOP;

                v_row_count := 0;
                OPEN v_cur FOR v_sql;
                LOOP
                    FETCH v_cur INTO v_old_val;
                    EXIT WHEN v_cur%NOTFOUND;

                    IF NOT v_existing_map.EXISTS(v_old_val) THEN
                        v_new_val := f_new_value(
                                         v_old_val, v_cols(i),
                                         v_cmds(c).table_schema,
                                         v_cmds(c).table_name,
                                         v_data_type, i_run_id);
                        BEGIN
                            INSERT INTO T_ANON_TOY_MAP
                                (SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN,
                                 OLD_VALUE, NEW_VALUE, ANON_METHOD, RUN_ID)
                            VALUES
                                (UPPER(v_cmds(c).table_schema),
                                 UPPER(v_cmds(c).table_name),
                                 UPPER(v_cols(i).column_name),
                                 v_old_val, v_new_val,
                                 v_cols(i).anon_method, i_run_id);
                            -- Increment only on successful INSERT; DUP means
                            -- another session already wrote this mapping.
                            v_row_count := v_row_count + 1;
                            -- Honour COMMIT_EVERY to limit MAP table transaction
                            -- size.  NULL or 0 disables mid-batch commits.
                            -- NOTE: v_row_count resets at the start of each
                            -- column's loop, so COMMIT_EVERY = N means "commit
                            -- every N MAP entries within one column", NOT "every
                            -- N total MAP entries for the table".  Wide tables
                            -- with many short columns (each < N distinct values)
                            -- will see no intermediate commits; all entries for
                            -- the table are flushed by the COMMIT below.
                            IF v_cmds(c).commit_every > 0
                               AND MOD(v_row_count, v_cmds(c).commit_every) = 0
                            THEN
                                COMMIT;
                            END IF;
                        EXCEPTION
                            WHEN DUP_VAL_ON_INDEX THEN NULL; -- concurrency guard
                        END;
                    END IF;

                END LOOP;
                CLOSE v_cur;

                p_log(i_run_id, v_tbl, 'p_build_map',
                      'INFO: column ' || v_cols(i).column_name
                      || ' — ' || v_row_count || ' new mapping(s) created');

            END LOOP; -- columns

            COMMIT; -- flush any remaining uncommitted MAP entries for this table

        END LOOP; -- tables

        p_log(i_run_id, NULL, 'p_build_map', 'END');

    EXCEPTION
        WHEN OTHERS THEN
            -- ROLLBACK first: discards any uncommitted MAP rows from the failing
            -- column.  Without this, those rows remain in the session's open
            -- transaction and could be committed by a caller that issues COMMIT
            -- after catching the raised application error (a common automation
            -- pattern).  p_log is autonomous so the ROLLBACK does not affect log
            -- writes.  Cursor close is safe after rollback.
            ROLLBACK;
            IF v_cur%ISOPEN THEN CLOSE v_cur; END IF;
            -- Use v_tbl (set at the start of each table iteration) so the log
            -- entry identifies which table was being processed when the error
            -- occurred.  v_tbl is NULL if the error fires before the table loop.
            g_fatal_occurred := TRUE;  -- signal p_run that build phase failed
            p_log(i_run_id, v_tbl, 'p_build_map',
                  'FATAL: ' || SQLERRM || ' | '
                  || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
    END p_build_map_core;

    -- =========================================================================
    -- Public: p_build_map
    -- Generates its own RUN_ID and delegates to p_build_map_core.
    -- NOTE: when used in the manual two-step workflow (p_build_map then
    -- p_apply_map), each call generates its own RUN_ID so build and apply
    -- phases log under different RUN_IDs.  Correlate by timestamp in
    -- T_ANON_TOY_LOG, or use p_run to get a single shared RUN_ID.
    -- =========================================================================
    PROCEDURE p_build_map (
        i_schema     IN VARCHAR2 DEFAULT NULL,
        i_table_name IN VARCHAR2 DEFAULT NULL
    )
    IS
        v_run_id NUMBER;
    BEGIN
        v_run_id := f_next_run_id();
        g_fatal_occurred := FALSE;
        p_build_map_core(i_schema, i_table_name, v_run_id);
        IF g_fatal_occurred THEN
            RAISE_APPLICATION_ERROR(-20001,
                'PKG_ANON_TOY.p_build_map: build phase FATAL'
                || ' — see T_ANON_TOY_LOG RUN_ID ' || v_run_id);
        END IF;
    END p_build_map;

    -- =========================================================================
    -- Private: p_apply_map_core
    -- Phase 2 implementation.  Accepts an externally generated RUN_ID so that
    -- p_run can share one RUN_ID across both phases.
    --
    -- Per-table error handling
    -- ------------------------
    -- Each column UPDATE is committed immediately (to limit UNDO pressure).
    -- If an error occurs, ROLLBACK undoes only the current uncommitted column.
    -- The error log entry reports exactly how many columns were committed before
    -- the failure so the operator can assess the partial state.
    -- IF_ANONYMIZE remains 1 on failure so the table is retried on the next run.
    -- =========================================================================
    PROCEDURE p_apply_map_core (
        i_schema     IN VARCHAR2,
        i_table_name IN VARCHAR2,
        i_run_id     IN NUMBER
    )
    IS
        v_cmds       t_cmd_tab;
        v_cmd_cnt    PLS_INTEGER;
        v_cols       t_col_tab;
        v_col_cnt    PLS_INTEGER;
        v_fks        t_fk_tab;
        v_fk_cnt     PLS_INTEGER;
        v_tbl        VARCHAR2(128);
        v_safe_tbl   VARCHAR2(200);
        v_safe_col   VARCHAR2(200);
        v_src_schema VARCHAR2(64);
        v_src_table  VARCHAR2(64);
        v_src_col    VARCHAR2(64);
        v_sql        VARCHAR2(32767);
        v_rows_upd   NUMBER;
        v_data_type  VARCHAR2(64);
        v_err_msg    VARCHAR2(4000);
        v_col_done   PLS_INTEGER;   -- columns committed before a failure
        v_fk_fail_cnt PLS_INTEGER;  -- FKs that could not be disabled (cross-ref)
    BEGIN
        p_log(i_run_id, NULL, 'p_apply_map', 'START');
        get_cmd_rows(i_schema, i_table_name, v_cmds, v_cmd_cnt);

        IF v_cmd_cnt = 0 THEN
            p_log(i_run_id, NULL, 'p_apply_map',
                  'WARN: no CMD rows found with IF_ANONYMIZE=1');
            RETURN;
        END IF;

        FOR c IN 1 .. v_cmd_cnt LOOP
            v_tbl := v_cmds(c).table_schema || '.' || v_cmds(c).table_name;
            p_log(i_run_id, v_tbl, 'p_apply_map', 'Applying map to table');

            v_safe_tbl := DBMS_ASSERT.ENQUOTE_NAME(v_cmds(c).table_schema, FALSE)
                       || '.' ||
                          DBMS_ASSERT.ENQUOTE_NAME(v_cmds(c).table_name, FALSE);

            get_col_rows(v_cmds(c).table_schema, v_cmds(c).table_name,
                         v_cols, v_col_cnt);

            -- Collect and disable FK constraints that reference this table
            -- (so child row updates in later LVL steps are not blocked)
            get_child_fk_constraints(v_cmds(c).table_schema, v_cmds(c).table_name,
                               v_fks, v_fk_cnt);
            IF v_fk_cnt > 0 THEN
                p_log(i_run_id, v_tbl, 'p_apply_map',
                      'INFO: disabling ' || v_fk_cnt || ' FK constraint(s)');
                p_disable_fks(v_fks, v_fk_cnt, i_run_id, v_fk_fail_cnt);
                IF v_fk_fail_cnt > 0 THEN
                    p_log(i_run_id, v_tbl, 'p_apply_map',
                          'WARN: ' || v_fk_fail_cnt
                          || ' FK constraint(s) could not be disabled'
                          || ' — correlated UPDATE of PK/referenced column(s)'
                          || ' may fail with ORA-02292; see preceding'
                          || ' p_disable_fks WARN entries for detail');
                END IF;
            END IF;

            v_col_done := 0;

            BEGIN -- per-table error block

                FOR i IN 1 .. v_col_cnt LOOP
                    v_safe_col := DBMS_ASSERT.ENQUOTE_NAME(
                                      v_cols(i).column_name, FALSE);

                    -- NULL_VAL: blanket update, no MAP lookup needed
                    IF v_cols(i).anon_method = 'NULL_VAL' THEN
                        v_sql := 'UPDATE ' || v_safe_tbl
                               || ' SET '   || v_safe_col || ' = NULL'
                               || ' WHERE ' || v_safe_col || ' IS NOT NULL';
                        EXECUTE IMMEDIATE v_sql;
                        v_rows_upd := SQL%ROWCOUNT;
                        p_log(i_run_id, v_tbl, 'p_apply_map',
                              'INFO: column ' || v_cols(i).column_name
                              || ' NULL_VAL: ' || v_rows_upd || ' row(s) updated');
                        COMMIT;
                        v_col_done := v_col_done + 1;
                        CONTINUE;
                    END IF;

                    -- Determine MAP source column
                    -- (own column, or a parent column for application-logic FKs)
                    IF v_cols(i).map_source_schema IS NOT NULL THEN
                        v_src_schema := UPPER(v_cols(i).map_source_schema);
                        v_src_table  := UPPER(v_cols(i).map_source_table);
                        v_src_col    := UPPER(v_cols(i).map_source_column);
                    ELSE
                        v_src_schema := UPPER(v_cmds(c).table_schema);
                        v_src_table  := UPPER(v_cmds(c).table_name);
                        v_src_col    := UPPER(v_cols(i).column_name);
                    END IF;

                    -- Defensive: escape any embedded single quotes in the MAP source
                    -- identifiers before they are embedded as SQL string literals.
                    -- Oracle identifiers cannot normally contain single quotes, but
                    -- the T_ANON_TOY_COLS configuration is user-supplied and the
                    -- package runs with elevated privileges (UPDATE ANY TABLE), so
                    -- defensive escaping is warranted.
                    v_src_schema := REPLACE(v_src_schema, '''', '''''');
                    v_src_table  := REPLACE(v_src_table,  '''', '''''');
                    v_src_col    := REPLACE(v_src_col,    '''', '''''');

                    -- Resolve data type for correct cast on SET and WHERE clause
                    BEGIN
                        SELECT DATA_TYPE INTO v_data_type
                        FROM   DBA_TAB_COLUMNS
                        WHERE  UPPER(OWNER)       = UPPER(v_cmds(c).table_schema)
                        AND    UPPER(TABLE_NAME)  = UPPER(v_cmds(c).table_name)
                        AND    UPPER(COLUMN_NAME) = UPPER(v_cols(i).column_name);
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            -- Matches p_build_map_core behaviour: skip the column
                            -- rather than guessing a type.  The build phase would
                            -- have logged the same WARN and created no MAP entries,
                            -- so applying with a wrong cast would update 0 rows anyway.
                            p_log(i_run_id, v_tbl, 'p_apply_map',
                                  'WARN: column ' || v_cols(i).column_name
                                  || ' not found in DBA_TAB_COLUMNS — skipped');
                            CONTINUE;
                    END;

                    -- Build correlated-subquery UPDATE.
                    -- The WHERE clause has two conditions:
                    --   1. col IS NOT NULL  — skip NULLs (no MAP entry for NULL)
                    --   2. EXISTS (...)     — skip rows whose old value has no
                    --                        mapping (avoids setting col to NULL
                    --                        when the subquery returns no row)
                    -- The cast in OLD_VALUE comparison must match the format
                    -- used by p_build_map_core when it fetched DISTINCT values.
                    IF v_data_type = 'DATE' THEN
                        v_sql :=
                            'UPDATE ' || v_safe_tbl
                         || ' SET ' || v_safe_col
                         || ' = (SELECT TO_DATE(m.NEW_VALUE,'
                         ||            ' ''YYYY-MM-DD HH24:MI:SS'')'
                         ||      ' FROM T_ANON_TOY_MAP m'
                         ||      ' WHERE m.SOURCE_SCHEMA = ''' || v_src_schema || ''''
                         ||      ' AND   m.SOURCE_TABLE  = ''' || v_src_table  || ''''
                         ||      ' AND   m.SOURCE_COLUMN = ''' || v_src_col    || ''''
                         ||      ' AND   m.OLD_VALUE     = TO_CHAR(' || v_safe_col
                         ||                              ', ''YYYY-MM-DD HH24:MI:SS''))'
                         || ' WHERE ' || v_safe_col || ' IS NOT NULL'
                         || ' AND EXISTS'
                         ||  ' (SELECT 1 FROM T_ANON_TOY_MAP m'
                         ||   ' WHERE m.SOURCE_SCHEMA = ''' || v_src_schema || ''''
                         ||   ' AND   m.SOURCE_TABLE  = ''' || v_src_table  || ''''
                         ||   ' AND   m.SOURCE_COLUMN = ''' || v_src_col    || ''''
                         ||   ' AND   m.OLD_VALUE     = TO_CHAR(' || v_safe_col
                         ||                            ', ''YYYY-MM-DD HH24:MI:SS''))';

                    ELSIF v_data_type LIKE 'TIMESTAMP%' THEN
                        v_sql :=
                            'UPDATE ' || v_safe_tbl
                         || ' SET ' || v_safe_col
                         || ' = (SELECT TO_TIMESTAMP(m.NEW_VALUE,'
                         ||            ' ''YYYY-MM-DD HH24:MI:SS'')'
                         ||      ' FROM T_ANON_TOY_MAP m'
                         ||      ' WHERE m.SOURCE_SCHEMA = ''' || v_src_schema || ''''
                         ||      ' AND   m.SOURCE_TABLE  = ''' || v_src_table  || ''''
                         ||      ' AND   m.SOURCE_COLUMN = ''' || v_src_col    || ''''
                         ||      ' AND   m.OLD_VALUE     = TO_CHAR(' || v_safe_col
                         ||                              ', ''YYYY-MM-DD HH24:MI:SS''))'
                         || ' WHERE ' || v_safe_col || ' IS NOT NULL'
                         || ' AND EXISTS'
                         ||  ' (SELECT 1 FROM T_ANON_TOY_MAP m'
                         ||   ' WHERE m.SOURCE_SCHEMA = ''' || v_src_schema || ''''
                         ||   ' AND   m.SOURCE_TABLE  = ''' || v_src_table  || ''''
                         ||   ' AND   m.SOURCE_COLUMN = ''' || v_src_col    || ''''
                         ||   ' AND   m.OLD_VALUE     = TO_CHAR(' || v_safe_col
                         ||                            ', ''YYYY-MM-DD HH24:MI:SS''))';

                    ELSIF v_data_type = 'NUMBER' THEN
                        -- FM format with explicit '.' matches the NLS-independent
                        -- format used in p_build_map_core.  TO_NUMBER with the same
                        -- format mask parses the stored string back to NUMBER using
                        -- period as decimal regardless of session NLS_NUMERIC_CHARACTERS.
                        v_sql :=
                            'UPDATE ' || v_safe_tbl
                         || ' SET ' || v_safe_col
                         || ' = (SELECT TO_NUMBER(m.NEW_VALUE,'
                         ||              ' ''FM99999999999999999999999999990.999999999999'')'
                         ||      ' FROM T_ANON_TOY_MAP m'
                         ||      ' WHERE m.SOURCE_SCHEMA = ''' || v_src_schema || ''''
                         ||      ' AND   m.SOURCE_TABLE  = ''' || v_src_table  || ''''
                         ||      ' AND   m.SOURCE_COLUMN = ''' || v_src_col    || ''''
                         ||      ' AND   m.OLD_VALUE     = TO_CHAR(' || v_safe_col
                         ||              ', ''FM99999999999999999999999999990.999999999999''))'
                         || ' WHERE ' || v_safe_col || ' IS NOT NULL'
                         || ' AND EXISTS'
                         ||  ' (SELECT 1 FROM T_ANON_TOY_MAP m'
                         ||   ' WHERE m.SOURCE_SCHEMA = ''' || v_src_schema || ''''
                         ||   ' AND   m.SOURCE_TABLE  = ''' || v_src_table  || ''''
                         ||   ' AND   m.SOURCE_COLUMN = ''' || v_src_col    || ''''
                         ||   ' AND   m.OLD_VALUE     = TO_CHAR(' || v_safe_col
                         ||              ', ''FM99999999999999999999999999990.999999999999''))';

                    ELSE
                        v_sql :=
                            'UPDATE ' || v_safe_tbl
                         || ' SET ' || v_safe_col
                         || ' = (SELECT m.NEW_VALUE'
                         ||      ' FROM T_ANON_TOY_MAP m'
                         ||      ' WHERE m.SOURCE_SCHEMA = ''' || v_src_schema || ''''
                         ||      ' AND   m.SOURCE_TABLE  = ''' || v_src_table  || ''''
                         ||      ' AND   m.SOURCE_COLUMN = ''' || v_src_col    || ''''
                         ||      ' AND   m.OLD_VALUE     = TO_CHAR(' || v_safe_col || '))'
                         || ' WHERE ' || v_safe_col || ' IS NOT NULL'
                         || ' AND EXISTS'
                         ||  ' (SELECT 1 FROM T_ANON_TOY_MAP m'
                         ||   ' WHERE m.SOURCE_SCHEMA = ''' || v_src_schema || ''''
                         ||   ' AND   m.SOURCE_TABLE  = ''' || v_src_table  || ''''
                         ||   ' AND   m.SOURCE_COLUMN = ''' || v_src_col    || ''''
                         ||   ' AND   m.OLD_VALUE     = TO_CHAR(' || v_safe_col || '))';
                    END IF;

                    EXECUTE IMMEDIATE v_sql;
                    v_rows_upd := SQL%ROWCOUNT;

                    p_log(i_run_id, v_tbl, 'p_apply_map',
                          'INFO: column ' || v_cols(i).column_name
                          || ' [' || v_cols(i).anon_method || ']'
                          || ' — ' || v_rows_upd || ' row(s) updated');

                    -- COMMIT_EVERY from T_ANON_TOY_CMD applies to the build
                    -- phase only.  The apply phase uses a bulk correlated UPDATE
                    -- per column; Oracle does not support row-level intermediate
                    -- commits within a single SQL statement.  Each column UPDATE
                    -- is committed here as the finest achievable granularity.
                    COMMIT;
                    v_col_done := v_col_done + 1;

                END LOOP; -- columns

                -- All columns committed; mark table done
                UPDATE T_ANON_TOY_CMD
                SET    IF_ANONYMIZE = 0
                WHERE  UPPER(TABLE_SCHEMA) = UPPER(v_cmds(c).table_schema)
                AND    UPPER(TABLE_NAME)   = UPPER(v_cmds(c).table_name);
                COMMIT;

            EXCEPTION
                WHEN OTHERS THEN
                    -- ROLLBACK undoes only the current uncommitted column.
                    -- v_col_done columns were already committed and remain changed.
                    -- IF_ANONYMIZE stays 1 so the table is retried on next run.
                    v_err_msg := SQLERRM || ' | '
                                 || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                    ROLLBACK;
                    p_log(i_run_id, v_tbl, 'p_apply_map',
                          'ERROR: ' || v_err_msg
                          || ' | partial state: ' || v_col_done
                          || ' of ' || v_col_cnt
                          || ' column(s) committed before failure'
                          || ' — table requires manual inspection');
            END; -- per-table block

            -- Re-enable FK constraints regardless of per-table success/failure
            IF v_fk_cnt > 0 THEN
                p_log(i_run_id, v_tbl, 'p_apply_map',
                      'INFO: re-enabling ' || v_fk_cnt || ' FK constraint(s)'
                      || ' (NOVALIDATE; run ENABLE VALIDATE after full run'
                      || ' to verify consistency)');
                p_enable_fks(v_fks, v_fk_cnt, i_run_id);
            END IF;

        END LOOP; -- tables

        p_log(i_run_id, NULL, 'p_apply_map', 'END');

    EXCEPTION
        WHEN OTHERS THEN
            -- Use v_tbl (set at the start of each table iteration) so the log
            -- entry identifies which table was being processed when the error
            -- occurred.  v_tbl is NULL if the error fires before the table loop.
            g_fatal_occurred := TRUE;  -- signal p_run that apply phase failed
            p_log(i_run_id, v_tbl, 'p_apply_map',
                  'FATAL: ' || SQLERRM || ' | '
                  || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
    END p_apply_map_core;

    -- =========================================================================
    -- Public: p_apply_map
    -- Generates its own RUN_ID and delegates to p_apply_map_core.
    -- =========================================================================
    PROCEDURE p_apply_map (
        i_schema     IN VARCHAR2 DEFAULT NULL,
        i_table_name IN VARCHAR2 DEFAULT NULL
    )
    IS
        v_run_id NUMBER;
    BEGIN
        v_run_id := f_next_run_id();
        g_fatal_occurred := FALSE;
        p_apply_map_core(i_schema, i_table_name, v_run_id);
        IF g_fatal_occurred THEN
            RAISE_APPLICATION_ERROR(-20002,
                'PKG_ANON_TOY.p_apply_map: apply phase FATAL'
                || ' — see T_ANON_TOY_LOG RUN_ID ' || v_run_id);
        END IF;
    END p_apply_map;

    -- =========================================================================
    -- Public: p_run
    -- Generates a single RUN_ID shared across both phases so the entire run
    -- appears as one coherent group in T_ANON_TOY_LOG.
    -- =========================================================================
    PROCEDURE p_run (
        i_schema     IN VARCHAR2 DEFAULT NULL,
        i_table_name IN VARCHAR2 DEFAULT NULL
    )
    IS
        v_run_id    NUMBER;
        v_dummy_num NUMBER;
    BEGIN
        BEGIN
            v_run_id := f_next_run_id();
        EXCEPTION
            WHEN OTHERS THEN
                -- Sequence failure before any run_id exists; cannot log to table.
                -- Re-raise as a named application error so automation sees context.
                RAISE_APPLICATION_ERROR(-20000,
                    'PKG_ANON_TOY.p_run: sequence failure — ' || SQLERRM);
        END;

        v_dummy_num := f_log(v_run_id, NULL, 'p_run', 'START');

        -- Reset failure flag; core procedures set it TRUE in their FATAL handlers.
        g_fatal_occurred := FALSE;
        p_build_map_core(i_schema, i_table_name, v_run_id);
        IF g_fatal_occurred THEN
            v_dummy_num := f_log(v_run_id, NULL, 'p_run',
                'FATAL: build phase failed — apply phase skipped;'
                || ' see FATAL entry above for detail');
            RAISE_APPLICATION_ERROR(-20001,
                'PKG_ANON_TOY.p_run: build phase FATAL'
                || ' — see T_ANON_TOY_LOG RUN_ID ' || v_run_id);
        END IF;

        g_fatal_occurred := FALSE;
        p_apply_map_core(i_schema, i_table_name, v_run_id);
        IF g_fatal_occurred THEN
            v_dummy_num := f_log(v_run_id, NULL, 'p_run',
                'FATAL: apply phase failed;'
                || ' see FATAL entry above for detail');
            RAISE_APPLICATION_ERROR(-20002,
                'PKG_ANON_TOY.p_run: apply phase FATAL'
                || ' — see T_ANON_TOY_LOG RUN_ID ' || v_run_id);
        END IF;

        v_dummy_num := f_log(v_run_id, NULL, 'p_run', 'END');
    EXCEPTION
        WHEN OTHERS THEN
            -- ORA-20001 / ORA-20002 are raised by this procedure's own
            -- IF g_fatal_occurred blocks above and are already fully logged
            -- by the core procedure that set g_fatal_occurred.  Re-logging
            -- them here would produce a second FATAL entry per build/apply
            -- failure, which misleads log analysis and automation that counts
            -- FATAL records.  Only log genuinely unexpected errors that
            -- originate outside the core procedures (e.g. sequence failure,
            -- f_log failure after the END log entry).
            IF SQLCODE NOT BETWEEN -20999 AND -20000 THEN
                v_dummy_num := f_log(v_run_id, NULL, 'p_run',
                                     'FATAL: unexpected error: ' || SQLERRM
                                     || ' | '
                                     || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
            END IF;
            RAISE;
    END p_run;

END PKG_ANON_TOY;
/
