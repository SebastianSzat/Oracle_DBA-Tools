-- =============================================================================
-- 01_cleanup.sql
-- Drops all ANON_TOY objects in dependency-safe order.
--
-- Run this script as SYSDBA or as the ANON_TOY user before re-deploying.
-- Safe to run repeatedly — each DROP is wrapped in a WHEN OTHERS handler
-- so "object does not exist" errors are silently ignored.
-- (ORA-04043 for packages, ORA-00942 for tables, ORA-02289 for sequences.)
--
-- SECTION A (default): drops all objects owned by ANON_TOY but keeps the
--   user and its grants intact.  Use this for a clean re-deploy.
--
-- SECTION B (commented out): drops the ANON_TOY user entirely, including
--   all owned objects.  Uncomment only when decommissioning the tool.
--   Must be run as SYSDBA.
-- =============================================================================


-- =============================================================================
-- SECTION A — Drop all ANON_TOY objects (keep the user)
-- Connect as ANON_TOY or as a DBA.
-- =============================================================================
BEGIN

    -- Package
    FOR obj IN (SELECT object_name, object_type
                FROM   all_objects
                WHERE  owner       = 'ANON_TOY'
                  AND  object_type = 'PACKAGE'
                  AND  object_name = 'PKG_ANON_TOY')
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP PACKAGE ANON_TOY.PKG_ANON_TOY';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    -- Tables (CASCADE CONSTRAINTS removes FK dependencies automatically)
    FOR tbl IN (SELECT table_name
                FROM   all_tables
                WHERE  owner      = 'ANON_TOY'
                  AND  table_name LIKE '%ANON_TOY%'
                ORDER  BY CASE table_name
                              WHEN 'T_ANON_TOY_COLS' THEN 1
                              WHEN 'T_ANON_TOY_CMD'  THEN 2
                              WHEN 'T_ANON_TOY_MAP'  THEN 3
                              WHEN 'T_ANON_TOY_LOG'  THEN 4
                              WHEN 'T_ANON_TOY_POOL' THEN 5
                              ELSE 9
                          END)
    LOOP
        BEGIN
            EXECUTE IMMEDIATE
                'DROP TABLE ANON_TOY.' || tbl.table_name || ' CASCADE CONSTRAINTS';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    -- Sequences
    FOR seq IN (SELECT sequence_name
                FROM   all_sequences
                WHERE  sequence_owner = 'ANON_TOY'
                  AND  sequence_name LIKE '%ANON_TOY%')
    LOOP
        BEGIN
            EXECUTE IMMEDIATE
                'DROP SEQUENCE ANON_TOY.' || seq.sequence_name;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('ANON_TOY objects dropped successfully.');

END;
/


-- =============================================================================
-- SECTION B — Drop the ANON_TOY user and ALL owned objects
-- Uncomment the block below only when fully decommissioning the tool.
-- Must be run connected as SYSDBA.
-- =============================================================================
/*
BEGIN
    EXECUTE IMMEDIATE 'DROP USER ANON_TOY CASCADE';
    DBMS_OUTPUT.PUT_LINE('ANON_TOY user and all objects dropped.');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -1918 THEN   -- ORA-01918: user does not exist
            DBMS_OUTPUT.PUT_LINE('ANON_TOY user does not exist — nothing to drop.');
        ELSE
            RAISE;
        END IF;
END;
/
*/
