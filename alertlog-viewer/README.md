# Alert Log Viewer (Coloured)

A Bash utility for Oracle DBAs to extract and view a time-windowed slice of an Oracle alert log, with colour-highlighted output in the terminal.

## Background

Oracle alert logs can grow to hundreds of megabytes over time, making it impractical to open them in a text editor when investigating an incident. This script accepts a start and end timestamp, extracts only the relevant lines from the log, and prints them to the terminal with colour highlighting — making it easy to spot errors, warnings, and key events at a glance.

A clean (uncoloured) copy of the extracted lines is always saved to a temp file for archiving, piping, or further processing.

---

## How It Works

1. Accepts a time window (`-f` start, `-t` end) and an optional log file path (`-l`)
2. Falls back to the Oracle 19c ADR default log path if `-l` is not provided
3. If the default path does not exist either, prompts for the path interactively
4. Passes the parameters to an embedded Perl script for fast date parsing and filtering
5. For each line inside the time window:
   - Writes a clean copy to `/tmp/temp_alert_bytime_colored.log`
   - Prints a colour-highlighted version to the terminal
6. Prints a runtime summary on completion

---

## Requirements

- Bash
- Perl with `Time::Local` module (standard on Oracle Linux and Red Hat)

---

## Parameters

| Flag | Value | Required | Description |
|------|-------|----------|-------------|
| `-f` | timestamp | Yes* | Start of the time window |
| `-t` | timestamp | Yes* | End of the time window |
| `-l` | file path | No | Full path to the alert log file |
| `-h` / `-help` | — | No | Show help and exit |

*If `-f` or `-t` are omitted, the script prompts for them interactively.

**Timestamp format:** `yyyy.mm.dd-HH:MM:SS`  — e.g. `2025.11.27-13:22:00`

---

## Log File Path Resolution

When `-l` is not provided, the script resolves the log file in this order:

1. **Oracle 19c ADR default path** (auto-detected):
   ```
   $ORACLE_BASE/diag/rdbms/<SID>/<SID>/trace/alert_<SID>.log
   ```
   `$ORACLE_BASE` defaults to `/u01/app/oracle` if the environment variable is not set.

2. **Interactive prompt** — if the default path does not exist on the current host, the script asks for the path.

---

## Supported Timestamp Formats

The script handles both Oracle alert log timestamp formats automatically:

| Format | Example | Used by |
|--------|---------|---------|
| Modern (ADR) | `2025-11-27T13:22:00` | Oracle 11g+ with ADR enabled |
| Modern (dot) | `2025.11.27 13:22:00` | Some ADR variants |
| Classic | `Thu Nov 27 13:22:00 2025` | Pre-11g / non-ADR environments |

---

## Colour Scheme

> Note: Terminal colour rendering cannot be shown in markdown. The table below describes what gets highlighted at runtime.

| Colour | Applied to |
|--------|------------|
| Blue | Timestamps at the start of each line |
| Red | `ORA-XXXX` Oracle error codes |
| Purple | `CRITICAL`, `FATAL`, `ERROR` keywords |
| Yellow | `WARNING` keyword |
| Green | `LGWR switch`, `SCN:` informational markers |

---

## Example Runs

### Minimal — prompts for missing values
```bash
./alertlog-viewer-colored.sh
```
```
Please enter the start timestamp:
Format: yyyy.mm.dd-HH:MM:SS  (e.g. 2025.11.27-13:22:00)
Start time: 2025.11.27-13:00:00
Please enter the end timestamp:
Format: yyyy.mm.dd-HH:MM:SS
End time: 2025.11.27-14:00:00
No log file specified. Using Oracle 19c default:
  /u01/app/oracle/diag/rdbms/MYDB/MYDB/trace/alert_MYDB.log
```

---

### Full flags — no prompts
```bash
./alertlog-viewer-colored.sh \
  -f 2025.11.27-13:00:00 \
  -t 2025.11.27-14:00:00 \
  -l /u01/app/oracle/diag/rdbms/MYDB/MYDB/trace/alert_MYDB.log
```
```
Starting processing...
Time range : 2025.11.27-13:00:00  ->  2025.11.27-14:00:00
Log file   : /u01/app/oracle/diag/rdbms/MYDB/MYDB/trace/alert_MYDB.log
Output file: /tmp/temp_alert_bytime_colored.log
----------------------------------------------------------------
... (highlighted log lines) ...
----------------------------------------------------------------
Processing complete. Runtime: 2 second(s).
Clean output saved to: /tmp/temp_alert_bytime_colored.log
```

---

### With default Oracle 19c path (no -l flag)
```bash
./alertlog-viewer-colored.sh -f 2025.11.27-13:00:00 -t 2025.11.27-14:00:00
```
The script auto-detects the alert log using `$ORACLE_BASE` and `$ORACLE_SID` from the current Oracle environment.

---

### Default path not found — interactive fallback
```bash
./alertlog-viewer-colored.sh -f 2025.11.27-13:00:00 -t 2025.11.27-14:00:00
```
```
Default alert log not found: /u01/app/oracle/diag/rdbms/MYDB/MYDB/trace/alert_MYDB.log
Please enter the full path to the alert log file:
Log file path: /opt/oracle/admin/MYDB/bdump/alert_MYDB.log
```

---

### Non-standard Oracle install path
```bash
./alertlog-viewer-colored.sh \
  -f 2025.11.27-08:00:00 \
  -t 2025.11.27-09:00:00 \
  -l /opt/oracle/diag/rdbms/orcl/orcl/trace/alert_orcl.log
```

---

### Show help
```bash
./alertlog-viewer-colored.sh -h
```
```
Usage: ./alertlog-viewer-colored.sh -f <from> -t <to> [OPTIONS]

Options:
  -f <timestamp>   Start of the time window (Required).
                   Format: yyyy.mm.dd-HH:MM:SS  (e.g. 2025.11.27-13:22:00)
  -t <timestamp>   End of the time window (Required).
                   Format: yyyy.mm.dd-HH:MM:SS
  -l <path>        Full path to the Oracle alert log file.
                   Default (Oracle 19c ADR):
                   $ORACLE_BASE/diag/rdbms/<SID>/<SID>/trace/alert_<SID>.log
                   If the default path does not exist, the script will
                   prompt for the path interactively.
  -h, -help        Show this help message.
...
```

---

## Example Terminal Output

The output below shows the plain text content of what is printed. At runtime, colours are applied as described in the [Colour Scheme](#colour-scheme) section above.

```
2025-11-27T13:22:05.123456+00:00      <- Blue (timestamp)
Thread 1 opened at log sequence 42890
Current log# 3 seq# 42890 mem# 0: /u01/oradata/MYDB/redo03.log

2025-11-27T13:25:11.456789+00:00      <- Blue (timestamp)
ORA-00600: internal error code         <- Red  (ORA- code)
Arguments: [kksfbc-reparse-infinite-loop], ...

2025-11-27T13:31:44.000000+00:00      <- Blue (timestamp)
WARNING: db_recovery_file_dest_size    <- Yellow (WARNING)

2025-11-27T13:45:00.000000+00:00      <- Blue (timestamp)
CRITICAL: Archiver continuing          <- Purple (CRITICAL)

2025-11-27T13:55:22.000000+00:00      <- Blue (timestamp)
LGWR switch                            <- Green (informational)
  SCN: 0x0001.23456789
```

---

## Output Files

| Output | Description |
|--------|-------------|
| Terminal (STDOUT) | Colour-highlighted lines for interactive reading |
| `/tmp/temp_alert_bytime_colored.log` | Clean plain-text copy, no ANSI codes, safe for piping or archiving |

The temp file is overwritten on each run.

---

## Notes

- Files are read-only — the original alert log is never modified
- The script exits early once the end timestamp is exceeded, making it efficient on large logs
- Multi-line log entries (stack traces, ORA- details) are captured in full, not just the timestamped header line
- Both Oracle environment variables (`$ORACLE_BASE`, `$ORACLE_SID`) are optional if `-l` is supplied explicitly
