-- =============================================================================
-- 01_cleanup.sql
-- Drops all DATGEN_TOY objects from the current schema.
-- Safe to run repeatedly; errors on individual drops are suppressed so that
-- a partial installation can be cleaned up without manual intervention.
-- Run this before re-deploying the full object set.
-- =============================================================================

BEGIN
    FOR obj IN (
        SELECT object_name, object_type
        FROM   user_objects
        WHERE  object_name LIKE '%DATGEN_TOY%'
          AND  object_type NOT IN ('INDEX')   -- indexes are dropped with their table
        ORDER BY
            -- Drop in dependency-safe order:
            -- triggers first (no dependants), then packages, then tables, then sequences
            CASE object_type
                WHEN 'TRIGGER'  THEN 1
                WHEN 'PACKAGE'  THEN 2
                WHEN 'TABLE'    THEN 3
                WHEN 'SEQUENCE' THEN 4
                ELSE                 5
            END
    ) LOOP
        BEGIN
            IF obj.object_type = 'TABLE' THEN
                -- CASCADE CONSTRAINTS removes FKs from child tables before drop
                EXECUTE IMMEDIATE 'DROP ' || obj.object_type
                                  || ' ' || obj.object_name
                                  || ' CASCADE CONSTRAINTS';
            ELSE
                EXECUTE IMMEDIATE 'DROP ' || obj.object_type
                                  || ' ' || obj.object_name;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                -- Suppress individual drop errors (e.g. object already gone,
                -- or a dependency that CASCADE did not cover).
                -- The next deployment step will fail loudly if something
                -- genuinely blocking was left behind.
                NULL;
        END;
    END LOOP;
END;
/
