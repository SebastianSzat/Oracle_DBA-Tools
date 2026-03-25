-- =============================================================================
-- 06_T_ANON_TOY_LOG.sql
-- Log table and sequence — every step taken by the package is recorded here.
-- All entries from a single run share the same RUN_ID.
--
-- Run as ANON_TOY after 05_T_ANON_TOY_MAP.sql.
-- =============================================================================

CREATE SEQUENCE SEQ_ANON_TOY_LOG_ID
    START WITH     1
    INCREMENT BY   1
    CACHE          20
    NOCYCLE;


CREATE TABLE T_ANON_TOY_LOG (

    ID              NUMBER(12)      DEFAULT SEQ_ANON_TOY_LOG_ID.NEXTVAL NOT NULL,
    -- Unique log entry identifier; provides strict ordering within a run.

    RUN_ID          NUMBER(12)      NOT NULL,
    -- Groups all entries from a single p_run / p_build_map / p_apply_map
    -- invocation.  Use WHERE RUN_ID = <value> ORDER BY ID to replay a run.

    TABLE_NAME      VARCHAR2(128),
    -- Target table (SCHEMA.TABLE format, or NULL for run-level entries).

    STEP            VARCHAR2(64),
    -- Package procedure or phase that produced this entry.
    -- Examples: p_run, p_build_map, p_apply_map, p_analyze, f_log.

    STATUS_INFO     VARCHAR2(4000),
    -- Free-text message.  Prefixes:
    --   (none)  Normal progress
    --   INFO:   Non-critical notice
    --   WARN:   Potential problem — review before proceeding
    --   ERROR:  Per-table failure with rollback; run continues to next table
    --   FATAL:  Failure outside per-table block; run aborted

    MODIFIED        DATE            DEFAULT SYSDATE NOT NULL,

    CONSTRAINT PK_ANON_TOY_LOG PRIMARY KEY (ID)

);

COMMENT ON TABLE  T_ANON_TOY_LOG IS
    'PKG_ANON_TOY log table. Every run step is recorded here, grouped by RUN_ID.';
COMMENT ON COLUMN T_ANON_TOY_LOG.ID           IS 'Unique entry ID from SEQ_ANON_TOY_LOG_ID.';
COMMENT ON COLUMN T_ANON_TOY_LOG.RUN_ID       IS 'Run identifier. Groups all entries from one invocation.';
COMMENT ON COLUMN T_ANON_TOY_LOG.TABLE_NAME   IS 'Target table (SCHEMA.TABLE). NULL for run-level entries.';
COMMENT ON COLUMN T_ANON_TOY_LOG.STEP         IS 'Package procedure or phase name.';
COMMENT ON COLUMN T_ANON_TOY_LOG.STATUS_INFO  IS 'Log message. Prefixed with INFO:/WARN:/ERROR:/FATAL: where appropriate.';
COMMENT ON COLUMN T_ANON_TOY_LOG.MODIFIED     IS 'Timestamp when this entry was written.';

-- Indexes supporting the three most common query patterns
CREATE INDEX IDX_ANON_TOY_LOG_RUN
    ON T_ANON_TOY_LOG (RUN_ID, ID);

CREATE INDEX IDX_ANON_TOY_LOG_TBL
    ON T_ANON_TOY_LOG (TABLE_NAME, MODIFIED);

CREATE INDEX IDX_ANON_TOY_LOG_MOD
    ON T_ANON_TOY_LOG (MODIFIED);
