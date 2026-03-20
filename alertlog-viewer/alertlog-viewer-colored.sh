#!/bin/bash

## =============================================================== ##
## Script name     : alertlog-viewer-colored.sh                    ##
## Script version  : 1.3.0                                         ##
## Script creator  : Szatai Zalán                                  ##
## Script created  : 2025.11.27.                                   ##
## Last modified by: Szatai Zalán                                  ##
## Last modified   : 2026.03.20.                                   ##
## =============================================================== ##

#####################################################################
## COLOUR DEFINITIONS
## All ANSI colour codes are defined once here and shared by both the
## Bash console messages and the Perl log line highlighting.
## The $'...' (ANSI-C quoting) syntax embeds the actual ESC character
## (0x1B) into each variable, so echo does not need the -e flag and
## Perl can use the values directly via environment variables without
## any further escaping.
#####################################################################
COL_RESET=$'\033[0m'
COL_GREEN=$'\033[0;32m'
COL_RED=$'\033[0;31m'
COL_YELLOW=$'\033[1;33m'
COL_BLUE=$'\033[1;34m'
COL_PURPLE=$'\033[1;35m'

## Export so Perl can access them via %ENV
export COL_RESET COL_GREEN COL_RED COL_YELLOW COL_BLUE COL_PURPLE

#####################################################################
## CONFIGURATION AND VARIABLES
## Oracle 19c ADR default alert log path:
##   $ORACLE_BASE/diag/rdbms/<SID>/<SID>/trace/alert_<SID>.log
## $ORACLE_BASE defaults to /u01/app/oracle if not set in the env.
## DEFAULT_LOG_PATH is only used as a fallback when -l is not given.
## TEMP_OUTPUT_FILE is written to /tmp to avoid write permission
## issues when the script is run from a read-only directory.
#####################################################################
ORACLE_BASE_PATH="${ORACLE_BASE:-/u01/app/oracle}"
DEFAULT_LOG_PATH="$ORACLE_BASE_PATH/diag/rdbms/$ORACLE_SID/$ORACLE_SID/trace/alert_$ORACLE_SID.log"
TEMP_OUTPUT_FILE="/tmp/temp_alert_bytime_clean_$$.log"
START_TIME=$(date +%s)

## Runtime parameters — populated by argument parsing below
FROM_TIME=""
TO_TIME=""
LOG_FILE_PATH=""

#####################################################################
## HELP FUNCTION
## Displayed when -h or -help is passed.
## The heredoc uses regular EOF so $TEMP_OUTPUT_FILE expands to its
## value. Dollar signs that should print literally are escaped (\$).
#####################################################################
show_help() {
    cat << EOF
Usage: ./alertlog-viewer-colored.sh -f <from> -t <to> [OPTIONS]

Options:
  -f <timestamp>   Start of the time window (Required).
                   Format: yyyy.mm.dd-HH:MM:SS  (e.g. 2025.11.27-13:22:00)
  -t <timestamp>   End of the time window (Required).
                   Format: yyyy.mm.dd-HH:MM:SS  (e.g. 2025.11.27-13:52:00)
  -l <path>        Full path to the Oracle alert log file.
                   Default (Oracle 19c ADR):
                   \$ORACLE_BASE/diag/rdbms/<SID>/<SID>/trace/alert_<SID>.log
                   If the default path does not exist, the script will
                   prompt for the path interactively.
  -h, -help        Show this help message.

Notes:
  - If -f or -t are omitted, the script prompts for them interactively.
  - Coloured output is printed to the terminal.
  - A clean (uncoloured) copy is saved to /tmp/temp_alert_bytime_clean_<PID>.log
  - Both modern (ADR) and classic (pre-11g) Oracle timestamp formats
    are supported.

EOF
}

#####################################################################
## ARGUMENT PARSING
## Flag-based parsing using a while/case loop.
## Each flag that accepts a value consumes two arguments (shift 2).
## Unknown flags exit immediately to prevent silent misconfiguration.
#####################################################################
while [[ $# -gt 0 ]]; do
    case $1 in
        -f)       FROM_TIME="$2";     shift 2 ;;
        -t)       TO_TIME="$2";       shift 2 ;;
        -l)       LOG_FILE_PATH="$2"; shift 2 ;;
        -h|-help) show_help; exit 0 ;;
        *)        echo "Unknown option: $1"; echo "Use -h for help."; exit 1 ;;
    esac
done

#####################################################################
## INTERACTIVE PROMPTS
## Any required parameter not supplied via flags is requested here.
## FROM_TIME and TO_TIME are always required.
## LOG_FILE_PATH resolution order:
##   1. Value supplied with -l flag
##   2. Oracle 19c ADR default path (if the file exists)
##   3. Interactive prompt (if the default does not exist)
#####################################################################

## Prompt for start time if not provided via -f
if [ -z "$FROM_TIME" ]; then
    echo "${COL_GREEN}Please enter the start timestamp:${COL_RESET}"
    echo "Format: yyyy.mm.dd-HH:MM:SS  (e.g. 2025.11.27-13:22:00)"
    read -r -p "Start time: " FROM_TIME
    if [ -z "$FROM_TIME" ]; then
        echo "${COL_RED}ERROR: Start time cannot be empty.${COL_RESET}"
        exit 1
    fi
fi

## Prompt for end time if not provided via -t
if [ -z "$TO_TIME" ]; then
    echo "${COL_GREEN}Please enter the end timestamp:${COL_RESET}"
    echo "Format: yyyy.mm.dd-HH:MM:SS"
    read -r -p "End time: " TO_TIME
    if [ -z "$TO_TIME" ]; then
        echo "${COL_RED}ERROR: End time cannot be empty.${COL_RESET}"
        exit 1
    fi
fi

## Resolve log file path if not provided via -l
if [ -z "$LOG_FILE_PATH" ]; then
    if [ -f "$DEFAULT_LOG_PATH" ]; then
        ## Oracle 19c default exists — use it automatically
        LOG_FILE_PATH="$DEFAULT_LOG_PATH"
        echo "No log file specified. Using Oracle 19c default:"
        echo "  ${COL_GREEN}$LOG_FILE_PATH${COL_RESET}"
    else
        ## Default not found — prompt the user
        echo "${COL_YELLOW}Default alert log not found: $DEFAULT_LOG_PATH${COL_RESET}"
        echo "Please enter the full path to the alert log file:"
        read -r -p "Log file path: " LOG_FILE_PATH
    fi
fi

## Final validation: confirm the resolved log file actually exists
if [ ! -f "$LOG_FILE_PATH" ]; then
    echo "${COL_RED}ERROR: Log file not found: $LOG_FILE_PATH${COL_RESET}"
    exit 1
fi

#####################################################################
## PROCESSING WITH PERL
## Perl is used for all date parsing and line filtering because it is
## significantly faster than pure Bash loops on large alert log files
## and has built-in support for epoch-based time comparison via
## Time::Local (standard module on all Oracle Linux hosts).
##
## All variables are passed via environment to avoid injection risks
## from special characters in file paths or timestamp strings.
##
## Two Oracle alert log timestamp formats are supported:
##   Modern (ADR/XML)  : 2025-11-27T13:22:00  /  2025.11.27 13:22:00
##   Classic (pre-11g) : Thu Nov 27 13:22:00 2025
##
## Output is written to two destinations simultaneously:
##   TEMP_OUTPUT_FILE — clean plain-text copy (no ANSI codes) for
##                      archiving or further processing.
##   STDOUT           — colour-highlighted version for terminal use.
#####################################################################
echo "Starting processing..."
echo "Time range : $FROM_TIME  ->  $TO_TIME"
echo "Log file   : $LOG_FILE_PATH"
echo "Output file: $TEMP_OUTPUT_FILE"
echo "----------------------------------------------------------------"

## Export runtime parameters for Perl
export FROM_TIME TO_TIME LOG_FILE_PATH TEMP_OUTPUT_FILE

perl -e '
    use strict;
    use warnings;
    use Time::Local;

    #################################################################
    ## Month name lookup — used for classic Oracle timestamp parsing
    ## (e.g. "Thu Nov 27 13:22:00 2025")
    #################################################################
    my %MONTH_INDEX = (
        Jan => 0, Feb => 1, Mar =>  2, Apr =>  3,
        May => 4, Jun => 5, Jul =>  6, Aug =>  7,
        Sep => 8, Oct => 9, Nov => 10, Dec => 11
    );

    #################################################################
    ## parse_input_date
    ## Converts a user-supplied timestamp (yyyy.mm.dd-HH:MM:SS) into
    ## a Unix epoch value for numeric range comparison.
    ## Dies with a clear message on format mismatch — the exit code
    ## is checked by the shell after the Perl block.
    #################################################################
    sub parse_input_date {
        my $date_str = shift;
        if ($date_str =~ /^(\d{4})\.(\d{2})\.(\d{2})-(\d{2}):(\d{2}):(\d{2})$/) {
            ## timelocal(sec, min, hour, day, month-1, year)
            return timelocal($6, $5, $4, $3, $2 - 1, $1);
        }
        die "Invalid date format: \"$date_str\". Expected: yyyy.mm.dd-HH:MM:SS\n";
    }

    #################################################################
    ## parse_log_date
    ## Attempts to extract a Unix epoch timestamp from the start of
    ## an alert log line. Returns undef for lines without a timestamp
    ## (continuation lines, stack traces, SQL text, etc.).
    ##
    ## Handles two Oracle alert log timestamp formats:
    ##   Modern (ADR) : 2025-11-27T13:22:00  /  2025.11.27 13:22:00
    ##   Classic      : Thu Nov 27 13:22:00 2025  (pre-11g / non-ADR)
    #################################################################
    sub parse_log_date {
        my $line = shift;

        ## Modern ADR format: YYYY[-.]MM[-.]DD followed by T, space,
        ## or dash separator, then HH:MM:SS
        if ($line =~ /^(\d{4})[-.](\d{2})[-.](\d{2})[T\s-](\d{2}):(\d{2}):(\d{2})/) {
            return timelocal($6, $5, $4, $3, $2 - 1, $1);
        }

        ## Classic Oracle format: DayAbbr MonAbbr DD HH:MM:SS YYYY
        ## Example: Thu Nov 27 13:22:00 2025
        if ($line =~ /^\w{3}\s+(\w{3})\s+(\d{1,2})\s+(\d{2}):(\d{2}):(\d{2})\s+(\d{4})/) {
            my ($mon_str, $day, $h, $m, $s, $year) = ($1, $2, $3, $4, $5, $6);
            ## Skip lines with unrecognised month abbreviations rather than dying
            return undef unless exists $MONTH_INDEX{$mon_str};
            return timelocal($s, $m, $h, $day, $MONTH_INDEX{$mon_str}, $year);
        }

        return undef;
    }

    #################################################################
    ## INITIALISE
    ## Convert input timestamps to epoch and open file handles.
    ## Colour codes are read from the environment so a single
    ## definition in the Bash header covers both shell messages
    ## and Perl output.
    #################################################################
    my $epoch_from  = parse_input_date($ENV{FROM_TIME});
    my $epoch_to    = parse_input_date($ENV{TO_TIME});

    die "Start time is not before end time — check -f and -t values.\n"
        if $epoch_from >= $epoch_to;

    my $log_path    = $ENV{LOG_FILE_PATH};
    my $output_path = $ENV{TEMP_OUTPUT_FILE};

    ## Colour codes from the shared Bash definitions
    my $col_reset  = $ENV{COL_RESET};
    my $col_blue   = $ENV{COL_BLUE};
    my $col_red    = $ENV{COL_RED};
    my $col_purple = $ENV{COL_PURPLE};
    my $col_yellow = $ENV{COL_YELLOW};
    my $col_green  = $ENV{COL_GREEN};

    open(my $fh_in,  "<", $log_path)    or die "Cannot open log file for reading: $!\n";
    open(my $fh_out, ">", $output_path) or die "Cannot open output file for writing: $!\n";

    #################################################################
    ## MAIN PROCESSING LOOP
    ## Reads the log file line by line and filters to the requested
    ## time window.
    ##
    ## $in_range tracks whether the current line falls within the
    ## window. Lines without a timestamp (continuation lines, stack
    ## traces) inherit the state of the previous timestamped line,
    ## ensuring complete multi-line log entries are captured.
    #################################################################
    my $in_range = 0;

    while (my $line = <$fh_in>) {

        my $line_epoch = parse_log_date($line);

        if (defined $line_epoch) {

            ## Past the end of the window — stop reading immediately.
            ## This is a key optimisation for large files: no point
            ## reading further once the window has been exceeded.
            if ($line_epoch > $epoch_to) {
                last;
            }

            ## Set range state based on position relative to the window.
            ## Resetting to 0 below v_from ensures pre-window lines are
            ## never included even after a prior in_range = 1.
            if ($line_epoch >= $epoch_from) {
                $in_range = 1;
            } else {
                $in_range = 0;
            }
        }

        ## Write the line if inside the time window.
        ## Non-timestamped lines are included as long as $in_range
        ## is active (carried over from the last timestamped line).
        if ($in_range) {

            ## Clean copy to the output file (no colour codes)
            print $fh_out $line;

            ## ---------------------------------------------------
            ## COLOUR HIGHLIGHTING FOR TERMINAL OUTPUT
            ## A coloured copy is printed to STDOUT. The output
            ## file always stays clean for further processing.
            ## Each substitution resets the colour after the match
            ## to prevent bleed into subsequent text on the line.
            ## ---------------------------------------------------
            my $line_colored = $line;

            ## 1. Modern ADR timestamp at line start (blue)
            $line_colored =~ s/^(\d{4}[-.]\d{2}[-.]\d{2}[T\s-]\d{2}:\d{2}:\d{2})/$col_blue$1$col_reset/;

            ## 2. Classic Oracle timestamp at line start (blue)
            $line_colored =~ s/^(\w{3}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})/$col_blue$1$col_reset/;

            ## 3. ORA-XXXX error codes (red) — all occurrences on the line
            $line_colored =~ s/(ORA-\d+)/$col_red$1$col_reset/g;

            ## 4. Severity keywords: CRITICAL, FATAL, ERROR (purple)
            $line_colored =~ s/(CRITICAL|FATAL|ERROR)/$col_purple$1$col_reset/gi;

            ## 5. Warning keyword (yellow)
            $line_colored =~ s/(WARNING)/$col_yellow$1$col_reset/gi;

            ## 6. Informational keywords (green)
            $line_colored =~ s/(LGWR switch|SCN:)/$col_green$1$col_reset/gi;

            print $line_colored;
        }
    }

    close($fh_in);
    close($fh_out);
'

#####################################################################
## PERL EXIT CODE CHECK
## If Perl exited with a non-zero code (bad date format, file open
## failure, etc.) report the error and exit. Without this check the
## success message would print even when processing failed.
#####################################################################
PERL_EXIT_CODE=$?
if [[ $PERL_EXIT_CODE -ne 0 ]]; then
    echo "${COL_RED}ERROR: Processing failed (Perl exit code: $PERL_EXIT_CODE).${COL_RESET}"
    echo "Check the date format (yyyy.mm.dd-HH:MM:SS) and the log file path."
    exit 1
fi

#####################################################################
## SUMMARY
## Calculate and display the total script runtime.
#####################################################################
END_TIME=$(date +%s)
RUN_TIME=$((END_TIME - START_TIME))

echo "----------------------------------------------------------------"
echo "${COL_GREEN}Processing complete. Runtime: ${RUN_TIME} second(s).${COL_RESET}"
echo "Clean output saved to: ${COL_GREEN}$TEMP_OUTPUT_FILE${COL_RESET}"
