#!/bin/bash

## =============================================================== ##
## Script name     : ora-user-ensure.sh                            ##
## Script version  : 2.2.0                                         ##
## Script creator  : Szatai Zalán                                  ##
## Script created  : 2026.02.27.                                   ##
## Last modified by: Szatai Zalán                                  ##
## Last modified   : 2026.03.20.                                   ##
## =============================================================== ##

#####################################################################
## COLOUR DEFINITIONS
## Defined once here, used by both Bash console messages and the
## log() function. $'...' embeds the actual ESC character so echo
## does not need the -e flag.
#####################################################################
COL_RESET=$'\033[0m'
COL_GREEN=$'\033[0;32m'
COL_RED=$'\033[0;31m'
COL_YELLOW=$'\033[1;33m'
COL_CYAN=$'\033[0;36m'

#####################################################################
## CONFIGURATION AND VARIABLES
## All defaults are defined here. Every value can be overridden by
## the corresponding command-line flag. Timestamped log file names
## are generated once at startup so all entries in a run share the
## same filename.
##
## ENV_FILE_PATTERN supports a <SID> token that is replaced at
## runtime with the actual discovered SID, allowing one pattern to
## cover every database on every host. If <SID> is absent from the
## pattern, the same file is used for all databases.
#####################################################################
HOST_FILE="hosts.txt"
ENV_FILE_PATTERN='$HOME/env<SID>.sh'
SQL_SCRIPT="create_user.sql"
MAIN_LOG="oracle_check_$(date +%Y%m%d_%H%M%S).log"
RESULT_LOG="latest_result.log"
SQL_LOG="oracle_sql_$(date +%Y%m%d_%H%M%S).log"

## SSH options applied to every remote call.
## BatchMode=yes prevents the script from hanging on a password prompt
## if key-based authentication fails mid-run on any host.
SSH_OPTS=(-q -o ConnectTimeout=10 -o BatchMode=yes)

## Runtime parameters — populated by argument parsing
TARGET_USER=""
PDB_MODE="on"
PDB_LIST=""
RAC_MODE="on"
AUTO_CREATE="off"

#####################################################################
## HELP FUNCTION
#####################################################################
show_help() {
    cat << EOF
Usage: ./ora-user-ensure.sh -u <username> [OPTIONS]

Required:
  -u  <username>     Oracle username to check and optionally create.

Input files:
  -hosts <file>      Host list file.                  (Default: hosts.txt)
  -ef    <pattern>   Env file pattern. Use <SID> as a token — it is
                     replaced at runtime with the discovered SID.
                     If <SID> is absent, the same file is used for all DBs.
                     (Default: \$HOME/env<SID>.sh)
  -sql   <file>      SQL script to run when creating the user.
                                                       (Default: create_user.sql)

Output files:
  -l   <file>        Main timestamped log file.
                     (Default: oracle_check_YYYYMMDD_HHMMSS.log)
  -rl  <file>        Latest result summary — overwritten each run.
                     (Default: latest_result.log)
  -sl  <file>        SQL execution log.
                     (Default: oracle_sql_YYYYMMDD_HHMMSS.log)

Behaviour:
  -pdb     off       Disable multitenant PDB handling.
                     Default: on — the script queries each CDB for open,
                     read-write, non-restricted PDBs and checks/creates
                     the user in each one.
  -pdb-list A,B,C    Comma-separated list of PDB names to target.
                     Ignored when -pdb off is set.
                     Default: all qualifying open PDBs.
  -rac     off       Disable RAC SID suffix stripping.
                     Default: on — trailing digits are removed from the
                     SID so that instance DB1/DB2 maps to envDB.sh.
  -ac                Auto-create mode. Creates missing users without
                     prompting. Default: off (interactive prompt).
  -h, -help          Show this help message.

Notes:
  - PDB$SEED and CDB\$ROOT are never included in PDB processing.
  - Only PDBs with open_mode = READ WRITE and restricted = NO are processed.
  - The result log (latest_result.log) is overwritten on each run.
  - SQL log entries are separated by host, database, and PDB.

EOF
}

#####################################################################
## ARGUMENT PARSING
## Flag-based parsing using a while/case loop. Flags that accept
## a value consume two arguments (shift 2). -ac is a boolean toggle
## and only consumes one (shift 1).
#####################################################################
while [[ $# -gt 0 ]]; do
    case $1 in
        -u)        TARGET_USER="$2";      shift 2 ;;
        -hosts)    HOST_FILE="$2";        shift 2 ;;
        -ef)       ENV_FILE_PATTERN="$2"; shift 2 ;;
        -sql)      SQL_SCRIPT="$2";       shift 2 ;;
        -l)        MAIN_LOG="$2";         shift 2 ;;
        -rl)       RESULT_LOG="$2";       shift 2 ;;
        -sl)       SQL_LOG="$2";          shift 2 ;;
        -pdb)      PDB_MODE="$2";         shift 2 ;;
        -pdb-list) PDB_LIST="$2";         shift 2 ;;
        -rac)      RAC_MODE="$2";         shift 2 ;;
        -ac)       AUTO_CREATE="on";      shift 1 ;;
        -h|-help)  show_help; exit 0 ;;
        *)         echo "Unknown option: $1"; echo "Use -h for help."; exit 1 ;;
    esac
done

#####################################################################
## INPUT VALIDATION
## -u is the only required flag. All file paths are validated in the
## PRE-FLIGHT section below.
#####################################################################
if [[ -z "$TARGET_USER" ]]; then
    echo "${COL_RED}ERROR: -u <username> is required.${COL_RESET}"
    echo "Use -h for help."
    exit 1
fi

## Validate TARGET_USER format to prevent SQL injection.
## Oracle usernames: start with a letter; letters, digits, _, $, # only; max 30 chars.
if [[ ! "$TARGET_USER" =~ ^[A-Za-z][A-Za-z0-9_\$#]{0,29}$ ]]; then
    echo "${COL_RED}ERROR: Invalid Oracle username: '${TARGET_USER}'.${COL_RESET}"
    echo "       Must start with a letter; may contain letters, digits, _, \$, # only; max 30 characters."
    exit 1
fi

## Normalise PDB_MODE and RAC_MODE to lowercase for safe comparison
PDB_MODE=$(echo "$PDB_MODE" | tr '[:upper:]' '[:lower:]')
RAC_MODE=$(echo "$RAC_MODE" | tr '[:upper:]' '[:lower:]')

#####################################################################
## PRE-FLIGHT CHECKS
## Verify all required local input files exist before any remote
## work begins. Failing early avoids connecting to every host only
## to find the inputs are missing.
#####################################################################
for F in "$HOST_FILE" "$SQL_SCRIPT"; do
    if [[ ! -f "$F" ]]; then
        echo "${COL_RED}ERROR: Required file not found: $F${COL_RESET}"
        exit 1
    fi
done

## Initialise the result log (overwrite any previous run)
echo "" > "$RESULT_LOG"

#####################################################################
## LOGGING FUNCTION
## Prints a timestamped, colour-highlighted message to the terminal
## and writes a clean (ANSI-stripped) copy to the main log file.
## Separating the two prevents raw escape codes from appearing in
## the log file and breaking grep or further log processing.
#####################################################################
log() {
    local msg="$1"
    local timestamp
    timestamp="$(date '+%Y-%m-%d %H:%M:%S')"
    echo "${timestamp} - ${msg}"
    echo "${timestamp} - ${msg}" | sed $'s/\033\\[[0-9;]*m//g' >> "$MAIN_LOG"
}

#####################################################################
## SQL LOG SEPARATOR FUNCTION
## Writes a clearly formatted header block into the SQL log before
## each sqlplus execution so output from different hosts, databases,
## and PDBs is easy to distinguish.
#####################################################################
sql_log_header() {
    local host="$1"
    local sid="$2"
    local pdb="$3"
    local context="${host} | DB: ${sid}"
    [[ -n "$pdb" ]] && context="${context} | PDB: ${pdb}"
    echo "" >> "$SQL_LOG"
    echo "=========================================================================================" >> "$SQL_LOG"
    echo "  HOST: ${context} | $(date '+%Y-%m-%d %H:%M:%S')" >> "$SQL_LOG"
    echo "=========================================================================================" >> "$SQL_LOG"
}

#####################################################################
## CHECK AND CREATE USER FUNCTION
## Core logic for one database context (CDB or a specific PDB).
## Called once per qualifying PDB in PDB mode, or once per SID in
## non-PDB mode.
##
## Arguments:
##   $1 - HOST          Remote hostname
##   $2 - SID           Oracle SID (after RAC stripping)
##   $3 - ENV_FILE      Resolved env file path on the remote host
##   $4 - PDB_NAME      PDB name, or empty string for non-PDB context
##   $5 - LABEL         Human-readable label for log output
#####################################################################
check_and_create_user() {
    local host="$1"
    local sid="$2"
    local env_file="$3"
    local pdb_name="$4"
    local label="$5"

    ## Build the optional container switch SQL.
    ## Only injected when a PDB name is provided.
    local pdb_switch=""
    if [[ -n "$pdb_name" ]]; then
        pdb_switch="ALTER SESSION SET CONTAINER = ${pdb_name};"
    fi

    ## Run the user existence check via sqlplus.
    ## COUNT(*) always returns a number so the result is unambiguous —
    ## unlike SELECT username which returns empty rows for a missing user.
    local check_output
    check_output=$(ssh "${SSH_OPTS[@]}" "$host" "
        source \"${env_file}\"
        sqlplus -S / as sysdba << '__SQLEND__'
            SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF TRIMSPOOL ON
            ${pdb_switch}
            SELECT COUNT(*) FROM dba_users WHERE username = UPPER('${TARGET_USER}');
            EXIT;
__SQLEND__
    " 2>/dev/null)

    ## Extract the count — take the last non-empty line and strip whitespace
    local user_count
    user_count=$(echo "$check_output" | grep -v "^[[:space:]]*$" | tail -n 1 | tr -d '[:space:]')

    if [[ "$user_count" == "1" ]]; then
        ## User exists — nothing to do
        log "   ${COL_GREEN}[OK]${COL_RESET}      ${label} — ${TARGET_USER} exists."
        echo ">  ${label} - OK, ${TARGET_USER} exists" >> "$RESULT_LOG"

    elif [[ "$user_count" == "0" ]]; then
        ## User is missing — prompt or auto-create
        log "   ${COL_YELLOW}[MISSING]${COL_RESET}  ${label} — ${TARGET_USER} not found."
        echo ">  ${label} - MISSING, ${TARGET_USER} not found" >> "$RESULT_LOG"

        local do_create="n"
        if [[ "$AUTO_CREATE" == "on" ]]; then
            ## Auto-create mode: skip the prompt
            do_create="y"
            log "   Auto-create enabled — proceeding with creation in ${label}."
        else
            ## Interactive mode: read from /dev/tty because stdin is
            ## occupied by the host list file descriptor (exec 3<)
            read -r -p "   Create ${TARGET_USER} in ${label}? (y/n): " do_create < /dev/tty
        fi

        if [[ "$do_create" == "y" ]]; then
            log "   Creating ${TARGET_USER} in ${label}..."
            sql_log_header "$host" "$sid" "$pdb_name"

            ## Pipe the SQL script to sqlplus, prepending the container
            ## switch if this is a PDB context. The SQL script provides
            ## all DDL (CREATE USER, GRANT, etc.) and must include EXIT.
            {
                [[ -n "$pdb_name" ]] && echo "ALTER SESSION SET CONTAINER = ${pdb_name};"
                cat "$SQL_SCRIPT"
            } | ssh "${SSH_OPTS[@]}" "$host" "
                source \"${env_file}\"
                sqlplus -S / as sysdba
            " >> "$SQL_LOG" 2>&1

            ## Verify the user was actually created regardless of grant errors.
            ## sqlplus exits 0 even when individual statements fail (e.g. granting a
            ## role that does not exist on this database). Re-querying dba_users is
            ## the only reliable way to confirm success without false negatives.
            local verify_output
            verify_output=$(ssh "${SSH_OPTS[@]}" "$host" "
                source \"${env_file}\"
                sqlplus -S / as sysdba << '__SQLEND__'
                    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF TRIMSPOOL ON
                    ${pdb_switch}
                    SELECT COUNT(*) FROM dba_users WHERE username = UPPER('${TARGET_USER}');
                    EXIT;
__SQLEND__
            " 2>/dev/null)

            local verify_count
            verify_count=$(echo "$verify_output" | grep -v "^[[:space:]]*$" | tail -n 1 | tr -d '[:space:]')

            if [[ "$verify_count" == "1" ]]; then
                log "   ${COL_GREEN}[CREATED]${COL_RESET} ${label} — ${TARGET_USER} created successfully."
                echo ">  ${label} - CREATED, ${TARGET_USER} created" >> "$RESULT_LOG"
            else
                log "   ${COL_RED}[FAILED]${COL_RESET}  ${label} — user not found after creation attempt. See SQL log: ${SQL_LOG}"
                echo ">  ${label} - FAILED, creation error" >> "$RESULT_LOG"
            fi
        else
            log "   Skipped creation in ${label}."
            echo ">  ${label} - SKIPPED" >> "$RESULT_LOG"
        fi

    else
        ## Unexpected output — query may have failed or sqlplus errored
        log "   ${COL_RED}[ERROR]${COL_RESET}   ${label} — could not query user. Raw output: ${check_output}"
        echo ">  ${label} - ERROR, query failed" >> "$RESULT_LOG"
    fi
}

#####################################################################
## MAIN LOOP
## Reads the host list via file descriptor 3 so that stdin (/dev/tty)
## remains available for interactive prompts inside the loop.
## Blank lines and lines starting with # are skipped.
#####################################################################
log "===== ora-user-ensure started ====="
log "Target user : ${TARGET_USER}"
log "PDB mode    : ${PDB_MODE}$( [[ "$PDB_MODE" == "on" && -n "$PDB_LIST" ]] && echo " (filter: ${PDB_LIST})" )"
log "RAC mode    : ${RAC_MODE}"
log "Auto-create : ${AUTO_CREATE}"
log "Host file   : ${HOST_FILE}"
log "Env pattern : ${ENV_FILE_PATTERN}"
log "SQL script  : ${SQL_SCRIPT}"
log "================================================================="

exec 3< "$HOST_FILE"
while read -u 3 -r HOST; do
    [[ -z "$HOST" || "$HOST" =~ ^# ]] && continue

    log ""
    log "--- ${HOST} : starting ---"
    echo "" >> "$RESULT_LOG"
    echo "===== ${HOST} =====" >> "$RESULT_LOG"

    #################################################################
    ## SSH CONNECTIVITY CHECK
    ## A lightweight probe before doing any real work on the host.
    ## BatchMode=yes prevents SSH from hanging on password prompts.
    #################################################################
    ssh "${SSH_OPTS[@]}" "$HOST" "true" 2>/dev/null
    if [[ $? -ne 0 ]]; then
        log "${COL_RED}[UNREACHABLE]${COL_RESET} ${HOST} — SSH connection failed."
        echo ">  CONNECTION FAILED" >> "$RESULT_LOG"
        continue
    fi

    #################################################################
    ## SID DISCOVERY
    ## Finds all running Oracle instances by inspecting ora_pmon_*
    ## processes. ASM, Grid, and MGMTDB processes are excluded as
    ## they are infrastructure processes, not user databases.
    #################################################################
    SIDS=$(ssh "${SSH_OPTS[@]}" "$HOST" \
        "ps -ef | grep ora_pmon_ | grep -v grep \
                | grep -v '+ASM' | grep -v 'GRID' | grep -v 'MGMTDB' \
                | awk -F'ora_pmon_' '{print \$2}'" 2>/dev/null)

    if [[ -z "$SIDS" ]]; then
        log " > No running databases found on ${HOST}."
        echo ">  No running databases found" >> "$RESULT_LOG"
        continue
    fi

    #################################################################
    ## PER-SID PROCESSING LOOP
    ## SEEN_SIDS tracks which base SIDs have already been processed
    ## on this host. On a RAC cluster where both PROD1 and PROD2 run
    ## on the same node, both strip to PROD — without the guard the
    ## same database would be queried and logged twice.
    #################################################################
    declare -A SEEN_SIDS=()
    for RAW_SID in $SIDS; do

        ## Strip whitespace from the SID string
        SID=$(echo "$RAW_SID" | tr -d '[:space:]')

        ## RAC SID suffix stripping (default: on)
        ## Oracle RAC instance names append a digit to the DB name
        ## (e.g. PROD1, PROD2). Stripping gives the base SID used in
        ## the env file name (e.g. envPROD.sh covers both instances).
        if [[ "$RAC_MODE" == "on" ]]; then
            BASE_SID=$(echo "$SID" | sed 's/[0-9]*$//')
            if [[ -z "$BASE_SID" ]]; then
                log "  -> ${COL_YELLOW}[SKIP]${COL_RESET} SID '${SID}' reduced to empty after RAC stripping — skipping."
                continue
            fi
            [[ "$BASE_SID" != "$SID" ]] && log "  -> RAC instance detected: ${SID} → using base SID: ${BASE_SID}"
            SID="$BASE_SID"
        fi

        ## Skip if this base SID was already processed on this host
        if [[ -n "${SEEN_SIDS[$SID]+_}" ]]; then
            log "  -> ${SID} already processed on ${HOST} — skipping duplicate RAC instance."
            continue
        fi
        SEEN_SIDS[$SID]=1

        log " > DB: ${SID}"

        ## Resolve the env file path by replacing the <SID> token
        ## in the pattern with the actual SID value.
        ## If the pattern contains no <SID>, the same file is used
        ## for all databases on all hosts.
        ENV_FILE_PATH="${ENV_FILE_PATTERN//<SID>/$SID}"

        ## Verify the env file exists on the remote host before
        ## attempting to source it. Missing env files are logged and
        ## skipped rather than causing a cryptic sqlplus error.
        ssh "${SSH_OPTS[@]}" "$HOST" "test -f \"${ENV_FILE_PATH}\"" 2>/dev/null
        if [[ $? -ne 0 ]]; then
            log "  -> ${COL_YELLOW}[SKIP]${COL_RESET} Env file not found on ${HOST}: ${ENV_FILE_PATH}"
            echo ">  ${HOST}/${SID} - SKIP, env file missing: ${ENV_FILE_PATH}" >> "$RESULT_LOG"
            continue
        fi

        log "  -> Env file found: ${ENV_FILE_PATH}"

        #############################################################
        ## PDB MODE
        ## Query v$pdbs for all open, read-write, non-restricted PDBs.
        ## PDB$SEED and CDB$ROOT are always excluded.
        ## If v$pdbs returns no rows the instance is a non-CDB and
        ## falls through to standard (non-PDB) processing.
        #############################################################
        if [[ "$PDB_MODE" == "on" ]]; then

            PDB_NAMES=$(ssh "${SSH_OPTS[@]}" "$HOST" "
                source \"${ENV_FILE_PATH}\"
                sqlplus -S / as sysdba << '__SQLEND__'
                    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF TRIMSPOOL ON
                    SELECT name FROM v\$pdbs
                    WHERE  open_mode  = 'READ WRITE'
                    AND    restricted = 'NO'
                    AND    name NOT IN ('PDB\$SEED','CDB\$ROOT')
                    ORDER BY name;
                    EXIT;
__SQLEND__
            " 2>/dev/null | grep -v "^[[:space:]]*$" | tr -d '[:space:]')

            if [[ -z "$PDB_NAMES" ]]; then
                ## No open PDBs found — treat as a regular non-CDB instance
                log "  -> No qualifying PDBs found. Processing as non-CDB."
                check_and_create_user "$HOST" "$SID" "$ENV_FILE_PATH" "" "${HOST}/${SID}"
            else
                log "  -> CDB detected. Processing PDBs..."

                for PDB in $PDB_NAMES; do

                    ## Apply PDB list filter if -pdb-list was specified.
                    ## Comparison is case-insensitive to be forgiving of input.
                    if [[ -n "$PDB_LIST" ]]; then
                        if ! echo ",${PDB_LIST}," | grep -qFi ",${PDB},"; then
                            log "   -> ${COL_CYAN}[FILTERED]${COL_RESET} PDB ${PDB} not in pdb-list — skipping."
                            continue
                        fi
                    fi

                    log "   -> PDB: ${PDB}"
                    check_and_create_user "$HOST" "$SID" "$ENV_FILE_PATH" "$PDB" "${HOST}/${SID}/${PDB}"

                done
            fi

        else
            #############################################################
            ## NON-PDB MODE (-pdb off)
            ## Process the SID as a single database with no PDB handling.
            #############################################################
            check_and_create_user "$HOST" "$SID" "$ENV_FILE_PATH" "" "${HOST}/${SID}"
        fi

    done

    log "--- ${HOST} : done ---"

done
exec 3<&-

#####################################################################
## SUMMARY
#####################################################################
log ""
log "===== ora-user-ensure finished ====="
log "Main log    : ${MAIN_LOG}"
log "Result log  : ${RESULT_LOG}"
log "SQL log     : ${SQL_LOG}"
echo ""
echo "${COL_GREEN}Done.${COL_RESET} Summary:"
echo "  Main log    : ${MAIN_LOG}"
echo "  Result log  : ${RESULT_LOG}"
echo "  SQL log     : ${SQL_LOG}"
echo ""
