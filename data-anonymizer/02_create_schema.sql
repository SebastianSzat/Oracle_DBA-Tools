-- =============================================================================
-- 02_create_schema.sql
-- Creates the ANON_TOY schema (Oracle user) and grants all privileges
-- required for the anonymizer package to operate.
--
-- MUST be run as SYSDBA (or a DBA with CREATE USER and GRANT ANY PRIVILEGE).
--
-- The ANON_TOY user:
--   - Owns all anonymizer objects (tables, sequences, package).
--   - Has DBA-level ANY privileges so it can read and modify tables in any
--     schema without those schemas needing to grant anything.
--   - Can disable and re-enable FK constraints in any schema.
--   - Is NOT granted to PUBLIC.  The package is not executable from any other
--     user, preventing accidental runs.  To use the package, connect as
--     ANON_TOY directly.
--
-- PASSWORD: replace <password> below with a strong password before running.
--   Recommended: use a password manager or Oracle wallet.
--   Example strong pattern: At least 16 chars, mixed case, digits, special.
-- =============================================================================


-- =============================================================================
-- 1. Create the user
-- =============================================================================
CREATE USER ANON_TOY
    IDENTIFIED BY "<replace_with_strong_password>"
    DEFAULT   TABLESPACE USERS
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON USERS;


-- =============================================================================
-- 2. Basic session and schema privileges
-- =============================================================================
GRANT CREATE SESSION     TO ANON_TOY;
GRANT CREATE TABLE       TO ANON_TOY;
GRANT CREATE SEQUENCE    TO ANON_TOY;
GRANT CREATE PROCEDURE   TO ANON_TOY;
GRANT CREATE TRIGGER     TO ANON_TOY;


-- =============================================================================
-- 3. Cross-schema data access
--    The package reads data from and writes anonymized values to tables in
--    any schema.  These ANY privileges cover all target schemas without
--    requiring per-schema grants.
-- =============================================================================
GRANT SELECT  ANY TABLE  TO ANON_TOY;
GRANT UPDATE  ANY TABLE  TO ANON_TOY;
-- INSERT ANY TABLE is not granted: the package only INSERTs into its own
-- tables (T_ANON_TOY_MAP, T_ANON_TOY_LOG), which ANON_TOY owns directly.


-- =============================================================================
-- 4. DDL on other schemas' constraints
--    Required to DISABLE / ENABLE FK constraints during the anonymization run.
-- =============================================================================
GRANT ALTER  ANY TABLE   TO ANON_TOY;


-- =============================================================================
-- 5. Data dictionary access
--    Required to auto-discover FK relationships, column data types, and
--    constraint metadata for all target schemas via DBA_ views.
-- =============================================================================
GRANT SELECT ANY DICTIONARY TO ANON_TOY;


-- =============================================================================
-- 6. Built-in package execution
-- =============================================================================
GRANT EXECUTE ON SYS.DBMS_RANDOM   TO ANON_TOY;
GRANT EXECUTE ON SYS.DBMS_ASSERT   TO ANON_TOY;
GRANT EXECUTE ON SYS.DBMS_UTILITY  TO ANON_TOY;
GRANT EXECUTE ON SYS.DBMS_OUTPUT   TO ANON_TOY;


-- =============================================================================
-- 7. Confirm
-- =============================================================================
BEGIN
    DBMS_OUTPUT.PUT_LINE('ANON_TOY schema created and all privileges granted.');
    DBMS_OUTPUT.PUT_LINE('Next step: connect as ANON_TOY and run 03 through 08.');
END;
/
