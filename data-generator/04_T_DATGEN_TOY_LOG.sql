-- =============================================================================
-- 04_T_DATGEN_TOY_LOG.sql
-- Logging infrastructure for PKG_DATGEN_TOY.
-- Every step taken by the package is written here, grouped by RUN_ID so that
-- a single invocation of f_fill_tables can be traced end-to-end.
--
-- Objects created:
--   SEQ_DATGEN_TOY_LOG_ID     sequence (provides unique IDs and RUN_IDs)
--   T_DATGEN_TOY_LOG          table
--   PK_DATGEN_TOY_LOG         primary key constraint
--   TRG_DATGEN_TOY_LOG        before-insert/update trigger (maintains MODIFIED)
--   I_DATGEN_TOY_LOG_1        index on TABLE_NAME  (filter by table)
--   I_DATGEN_TOY_LOG_2        index on MODIFIED    (filter by time)
--   I_DATGEN_TOY_LOG_3        index on RUN_ID      (filter by run)
--   COMMENT ON TABLE / COLUMN metadata
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Sequence: shared source of unique IDs for both the log rows (ID column)
-- and the per-run identifiers (RUN_ID) assigned at the start of each
-- f_fill_tables call.
-- NOCACHE avoids gaps on instance restart; ORDER guarantees monotonic values
-- in RAC environments.
-- -----------------------------------------------------------------------------
CREATE SEQUENCE SEQ_DATGEN_TOY_LOG_ID
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    ORDER;


-- -----------------------------------------------------------------------------
-- Log table
-- -----------------------------------------------------------------------------
CREATE TABLE T_DATGEN_TOY_LOG (

    -- Unique row identifier, sourced from SEQ_DATGEN_TOY_LOG_ID.
    -- Primary key; use for deterministic ordering of log entries.
    ID          NUMBER  DEFAULT SEQ_DATGEN_TOY_LOG_ID.NEXTVAL  NOT NULL,

    -- All log entries produced by a single call to f_fill_tables share the
    -- same RUN_ID. The value is drawn from SEQ_DATGEN_TOY_LOG_ID at the
    -- start of each run, so it is always lower than the IDs of the entries
    -- that follow it.
    -- Filter by RUN_ID to retrieve the complete trace for one run.
    RUN_ID      NUMBER          NOT NULL,

    -- Name of the target table being processed when this entry was written.
    -- NULL for run-level entries (start, end, fatal errors).
    TABLE_NAME  VARCHAR2(64),

    -- Name of the package function that wrote this entry.
    STEP        VARCHAR2(64),

    -- Human-readable status message, progress note, or error detail.
    STATUS_INFO VARCHAR2(4000),

    -- Timestamp written automatically by trigger TRG_DATGEN_TOY_LOG.
    MODIFIED    TIMESTAMP,

    CONSTRAINT PK_DATGEN_TOY_LOG PRIMARY KEY (ID)
);


-- -----------------------------------------------------------------------------
-- Trigger: keep MODIFIED current on every write.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER TRG_DATGEN_TOY_LOG
BEFORE INSERT OR UPDATE ON T_DATGEN_TOY_LOG
FOR EACH ROW
BEGIN
    :NEW.MODIFIED := SYSTIMESTAMP;
END TRG_DATGEN_TOY_LOG;
/


-- -----------------------------------------------------------------------------
-- Indexes: support the three most common query patterns against the log.
-- -----------------------------------------------------------------------------

-- Filter all log entries for a specific target table across all runs.
CREATE INDEX I_DATGEN_TOY_LOG_1 ON T_DATGEN_TOY_LOG (TABLE_NAME);

-- Filter log entries by time window (e.g. "everything in the last hour").
CREATE INDEX I_DATGEN_TOY_LOG_2 ON T_DATGEN_TOY_LOG (MODIFIED);

-- Filter all log entries belonging to one run (most common query pattern).
CREATE INDEX I_DATGEN_TOY_LOG_3 ON T_DATGEN_TOY_LOG (RUN_ID);


-- -----------------------------------------------------------------------------
-- Oracle data dictionary comments
-- -----------------------------------------------------------------------------
COMMENT ON TABLE T_DATGEN_TOY_LOG IS
    'Execution log for PKG_DATGEN_TOY. Every step of a f_fill_tables call is recorded here. All entries from a single invocation share the same RUN_ID, enabling end-to-end tracing of one run. Query: SELECT * FROM T_DATGEN_TOY_LOG WHERE run_id = <id> ORDER BY id;';

COMMENT ON COLUMN T_DATGEN_TOY_LOG.ID IS
    'Unique row identifier. Sourced from SEQ_DATGEN_TOY_LOG_ID. Use for deterministic ordering of log entries within or across runs.';

COMMENT ON COLUMN T_DATGEN_TOY_LOG.RUN_ID IS
    'Identifier shared by all log entries from a single f_fill_tables call. Drawn from SEQ_DATGEN_TOY_LOG_ID at run start. Filter by this column to retrieve the complete trace of one run.';

COMMENT ON COLUMN T_DATGEN_TOY_LOG.TABLE_NAME IS
    'Target table being processed when this entry was written. NULL for run-level entries such as run start, run end, and fatal errors not tied to a specific table.';

COMMENT ON COLUMN T_DATGEN_TOY_LOG.STEP IS
    'Name of the package function that wrote this log entry (e.g. f_fill_tables, f_random_blob). Useful for isolating which function produced an error.';

COMMENT ON COLUMN T_DATGEN_TOY_LOG.STATUS_INFO IS
    'Human-readable message. Contains progress notes (rows inserted, rows skipped), informational warnings (composite PK detected, unsupported column type), or full SQLERRM text on error.';

COMMENT ON COLUMN T_DATGEN_TOY_LOG.MODIFIED IS
    'Timestamp of the log entry. Set automatically by trigger TRG_DATGEN_TOY_LOG — do not populate manually.';
