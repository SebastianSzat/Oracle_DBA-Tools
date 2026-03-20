#!/bin/bash

## =============================================================== ##
## Script name            : tns_replacer.sh                        ##
## Script version         : 1.3.0                                  ##
## Script creator         : Szatai Zalán                           ##
## Script created         : 2026.03.19.                            ##
## =============================================================== ##

#####################################################################
## DEFAULT VALUES & VARIABLES
## All runtime flags default to OFF (0) or empty.
## File name defaults point to input files expected in the same
## directory as the script. SEARCH_PATH is the standard Oracle
## product install root on Linux hosts. LOG_FILE and DATE_STR are
## generated once at startup so all entries in a run share the same
## timestamp.
#####################################################################
TEST_MODE=0
SAFE_MODE=0
FORCED_MODE=0
TNS_NAME=""
HOST_LIST="tnsr_hostlist.lst"
ORIGIN_FILE="tnsr_tns_origin.tns"
NEW_FILE="tnsr_tns_new.tns"
SEARCH_PATH="/u01/app/oracle/product"
LOG_FILE="tns_replacer_$(date +%Y%m%d_%H%M%S).log"
DATE_STR=$(date +%Y%m%d)

#####################################################################
## HELP / MAN SHEET FUNCTION
## Printed when -h or -help is passed. Uses a heredoc so the
## formatting is preserved exactly as written.
#####################################################################
show_help() {
    cat << EOF
Usage: ./tns_replacer.sh -tn <TNS_NAME> [OPTIONS]

Manual for tns_replacer.sh:
  -tn         Name of the TNS entry to search for (Required).
  -test       Run in test mode (no changes made to remote files).
  -tp         Remote path to search for tnsnames.ora (Default: /u01/app/oracle/product).
  -fh         Path to hostlist file (Default: tnsr_hostlist.lst).
  -fto        Path to original TNS entry file (Default: tnsr_tns_origin.tns).
  -ftn        Path to new TNS entry file (Default: tnsr_tns_new.tns).
  -sm         Safe mode: only replace if there is an exact character match.
  -f          Forced mode: replace even if content mismatch is detected.
  -h, -help   Show this help message.

EOF
}

#####################################################################
## ARGUMENT PARSING
## Iterates over all passed arguments using a while/case loop.
## Each recognized flag sets its corresponding variable. Flags that
## take a value (e.g. -tn) consume two arguments with "shift 2".
## Unknown flags exit immediately to prevent silent misconfiguration.
## After parsing, -tn is validated as it is the only required flag.
#####################################################################
while [[ $# -gt 0 ]]; do
    case $1 in
        -test)  TEST_MODE=1; shift ;;
        -tn)    TNS_NAME="$2"; shift 2 ;;
        -tp)    SEARCH_PATH="$2"; shift 2 ;;
        -fh)    HOST_LIST="$2"; shift 2 ;;
        -fto)   ORIGIN_FILE="$2"; shift 2 ;;
        -ftn)   NEW_FILE="$2"; shift 2 ;;
        -sm)    SAFE_MODE=1; shift ;;
        -f)     FORCED_MODE=1; shift ;;
        -h|-help) show_help; exit 0 ;;
        *)      echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "$TNS_NAME" ]]; then
    echo "Error: -tn (TNS name) is required."
    exit 1
fi

if [[ "$SAFE_MODE" -eq 1 && "$FORCED_MODE" -eq 1 ]]; then
    echo "Error: -sm (safe mode) and -f (forced mode) cannot be used together."
    exit 1
fi

#####################################################################
## PRE-FLIGHT CHECKS
## Verifies all three required local input files exist before any
## remote work begins. Failing early avoids connecting to dozens of
## hosts only to find the input is missing.
## After the file checks, content is read into variables once so it
## does not need to be re-read for every host. ORIGIN_CLEAN and
## NEW_CLEAN are whitespace-stripped versions used for fuzzy matching
## on remote hosts where formatting may differ.
#####################################################################
for f in "$HOST_LIST" "$ORIGIN_FILE" "$NEW_FILE"; do
    if [[ ! -f "$f" ]]; then
        echo "Error: Local file $f not found." | tee -a "$LOG_FILE"
        exit 1
    fi
done

ORIGIN_CONTENT=$(cat "$ORIGIN_FILE")
NEW_CONTENT=$(cat "$NEW_FILE")
ORIGIN_CLEAN=$(echo "$ORIGIN_CONTENT" | tr -d '\r\n[:space:]')
NEW_CLEAN=$(echo "$NEW_CONTENT" | tr -d '\r\n[:space:]')

#####################################################################
## REMOTE EXECUTION LOGIC
## process_host() connects to a single host via SSH and performs the
## full find-and-replace workflow on that host. It is called once per
## host in the MAIN LOOP below.
##
## Variable expansion in the heredoc:
##   $VAR  (no backslash) — expanded locally before sending to SSH.
##         Used for values that are only known on the local machine
##         (e.g. TNS_NAME, ORIGIN_CONTENT, mode flags).
##   \$VAR (backslash)    — expanded remotely inside the SSH session.
##         Used for variables that are set or discovered on the remote
##         host (e.g. TNS_FILES, TNS_PATH, EXISTING_BLOCK).
##
## Output routing:
##   SSH stdout -> tee -> writes to log file AND passes to stdout of
##                        this function, so the MAIN LOOP can grep it.
##   SSH stderr -> appended directly to log file only (connection
##                 warnings, bash errors — not useful on the console).
#####################################################################
process_host() {
    local host=$1
    echo "#####################################################################" >> "$LOG_FILE"
    echo "***** Host: $host *****" >> "$LOG_FILE"

    ## StrictHostKeyChecking=no is intentional for closed internal environments — see README Security Notes for risks and a safer alternative.
    ssh -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=10 "$host" "bash -s" << EOF 2>> "$LOG_FILE" | tee -a "$LOG_FILE"

        ## Find all tnsnames.ora files under the search path.
        ## The -path "*samples*" -prune excludes Oracle sample directories
        ## which contain template tnsnames.ora files that should not be modified.
        TNS_FILES=\$(find "$SEARCH_PATH" -path "*samples*" -prune -o -name tnsnames.ora -print 2>/dev/null)

        if [[ -z "\$TNS_FILES" ]]; then
            echo ">>> HOST_STATUS: No tnsnames.ora found"
            exit
        fi

        ## Export local values as environment variables so Perl can read them
        ## on the remote side without needing further shell expansion.
        export TNS_NAME_ENV="$TNS_NAME"
        export NEW_CONTENT_ENV="$NEW_CONTENT"

        for TNS_PATH in \$TNS_FILES; do

            ## Extract the full TNS entry block from the file using Perl.
            ## Perl is used because TNS entries contain deeply nested parentheses
            ## which standard regex cannot reliably match.
            ## -0777 slurps the whole file as one string.
            ## The recursive pattern (?2) matches balanced parenthesis pairs
            ## of any depth, capturing the complete entry as group 1.
            EXISTING_BLOCK=\$(perl -0777 -ne 'print "\$1" if /^(\$ENV{TNS_NAME_ENV}\s*=\s*(\((?:[^()]+|(?2))*\)))/mi' "\$TNS_PATH")

            ## If no matching entry was found, log and move to the next file.
            if [[ -z "\$EXISTING_BLOCK" ]]; then
                echo ">>> FILE_STATUS: \$TNS_PATH -> NOT FOUND"
                echo ""
                continue
            fi

            ## Strip whitespace from the found block for fuzzy comparison.
            CLEAN_EXISTING=\$(echo "\$EXISTING_BLOCK" | tr -d '\r\n[:space:]')

            ## If the existing entry already matches the new content (ignoring
            ## whitespace), no update is needed — skip this file.
            if [[ "\$CLEAN_EXISTING" == "$NEW_CLEAN" ]]; then
                echo ">>> FILE_STATUS: \$TNS_PATH -> Already up to date"
                echo ""
                continue
            fi

            if [[ "$TEST_MODE" -eq 1 ]]; then
                ## TEST MODE: classify the match quality and report it.
                ## No changes are made to any file.
                ## Exact   — character-perfect match including whitespace.
                ## Fuzzy   — content matches after stripping whitespace/linebreaks.
                ## Mismatch — content differs; would not be replaced in normal mode.
                if [[ "\$EXISTING_BLOCK" == "$ORIGIN_CONTENT" ]]; then
                    echo ">>> FILE_STATUS: \$TNS_PATH -> Match (Exact)"
                elif [[ "\$CLEAN_EXISTING" == "$ORIGIN_CLEAN" ]]; then
                    echo ">>> FILE_STATUS: \$TNS_PATH -> Match (Fuzzy)"
                else
                    echo ">>> FILE_STATUS: \$TNS_PATH -> Content mismatch"
                fi
            else
                ## NORMAL MODE: evaluate match quality and attempt replacement.
                ## Export the found block so Perl can use it as the search string.
                export OLD_BLOCK_ENV="\$EXISTING_BLOCK"

                ## Replacement is allowed when any of these conditions are true:
                ## 1. Exact match  — the block is character-perfect.
                ## 2. Fuzzy match  — content matches after whitespace stripping,
                ##                   only allowed when Safe Mode is OFF.
                ## 3. Forced mode  — replace regardless of content match.
                if [[ "\$EXISTING_BLOCK" == "$ORIGIN_CONTENT" || ("$SAFE_MODE" -eq 0 && "\$CLEAN_EXISTING" == "$ORIGIN_CLEAN") || "$FORCED_MODE" -eq 1 ]]; then

                    ## Backup is created only when replacement will actually proceed.
                    ## Creating it unconditionally would leave stale backup files on
                    ## hosts where the entry was found but the content did not match.
                    BACKUP_FILE="\${TNS_PATH}.bck_tnsnames.ora_${DATE_STR}"
                    cp "\$TNS_PATH" "\$BACKUP_FILE"

                    ## Perl replaces the old block with the new content in-place.
                    ## \Q...\E quotes the search string literally so parentheses,
                    ## dots, and slashes in TNS entries are not treated as regex.
                    perl -i -0777 -pe 'BEGIN{ \$old=\$ENV{OLD_BLOCK_ENV}; \$new=\$ENV{NEW_CONTENT_ENV} } s/\Q\$old\E/\$new/' "\$TNS_PATH"

                    ## Log the result. The label distinguishes forced replacements
                    ## (where content did not match the origin) from normal ones.
                    ## Counts are derived at the end by grep-ing these labels.
                    if [[ "$FORCED_MODE" -eq 1 && "\$EXISTING_BLOCK" != "$ORIGIN_CONTENT" && "\$CLEAN_EXISTING" != "$ORIGIN_CLEAN" ]]; then
                        echo ">>> FILE_STATUS: \$TNS_PATH -> Success (Forced replacement)"
                    else
                        echo ">>> FILE_STATUS: \$TNS_PATH -> Success (Replaced)"
                    fi

                    ## Show a unified diff of the backup vs the updated file so the
                    ## change is visible in the log without opening both files manually.
                    echo "==== DIFF ===="
                    diff "\$BACKUP_FILE" "\$TNS_PATH"
                    echo "==== ==== ===="
                    echo ""
                else
                    echo ">>> FILE_STATUS: \$TNS_PATH -> Failed (Mismatch)"
                fi
            fi
        done
EOF

    ## PIPESTATUS[0] holds SSH's own exit code. Plain $? would return
    ## tee's exit code instead, masking SSH connection failures.
    ## Writing to stderr (>&2) ensures the message always appears on the
    ## console, bypassing the FILE_STATUS grep filter in the MAIN LOOP.
    if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
        echo "$host: Connection failed" >&2
    fi
}

#####################################################################
## MAIN LOOP
## Reads the host list line by line. Blank lines and lines starting
## with # are skipped so the file can contain comments and spacing.
## The "|| [[ -n ... ]]" handles files whose last line has no newline.
## For each host, process_host is called and its stdout is filtered
## for FILE_STATUS / HOST_STATUS lines. Prefixing each line with the
## hostname makes it easy to scan the console output across many hosts.
#####################################################################
echo ""
echo "#####################################################################" >> "$LOG_FILE"
echo "Script started at $(date +"%Y.%m.%d %H:%M:%S")" >> "$LOG_FILE"
while IFS= read -r remote_host || [[ -n "$remote_host" ]]; do
    [[ -z "$remote_host" || "$remote_host" =~ ^# ]] && continue
    echo "Processing Hostname: $remote_host - $(date +"%Y.%m.%d %H:%M:%S")"
    process_host "$remote_host" | grep -E "FILE_STATUS:|HOST_STATUS:" | while read -r line; do
        CLEAN_LINE=$(echo "$line" | sed -e 's/FILE_STATUS: //' -e 's/HOST_STATUS: //')
        echo "$remote_host: $CLEAN_LINE"
    done
    echo "" >> "$LOG_FILE"

done < "$HOST_LIST"
echo "" >> "$LOG_FILE"
echo "#####################################################################" >> "$LOG_FILE"
echo "Script finished at $(date +"%Y.%m.%d %H:%M:%S")" >> "$LOG_FILE"
echo "#####################################################################" >> "$LOG_FILE"

#####################################################################
## SUMMARY
## All counts are read from the log file using grep -c.
## This is reliable because tee guarantees every SSH output line is
## written to the log. Shell variable counters cannot be used here
## because any variable incremented inside the SSH heredoc lives in
## the remote session and is lost when the connection closes.
#####################################################################
N_PROCESS_COUNT=$(grep -cF "Success (Replaced)" "$LOG_FILE")
F_PROCESS_COUNT=$(grep -cF "Success (Forced replacement)" "$LOG_FILE")

echo ""
echo "Done. Logs: $LOG_FILE"
echo ""
echo "Found TNS entries for ${TNS_NAME}"
echo "--------------------------------------------"
echo "Already up-to-date:     $(grep -cF "Already up to date" "$LOG_FILE")"
echo "Perfect TNS match:      $(grep -cF "Match (Exact)" "$LOG_FILE")"
echo "Semi-perfect TNS match: $(grep -cF "Match (Fuzzy)" "$LOG_FILE")      (whitespaces, linebreaks different, content matches)"
echo "Content mismatch:       $(grep -cF "Content mismatch" "$LOG_FILE")"
echo "TNS entry not found:    $(grep -cF "NOT FOUND" "$LOG_FILE")"
echo "Summa:                  $(grep -cF ">>> FILE_STATUS" "$LOG_FILE")"
echo ""
echo "Processed TNS entries"
echo "--------------------------------------------"
echo "Normal replacement: ${N_PROCESS_COUNT}"
echo "Forced replacement: ${F_PROCESS_COUNT}"
echo "Summa: $((N_PROCESS_COUNT + F_PROCESS_COUNT))"
echo ""
echo ""
