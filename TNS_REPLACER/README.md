# TNS Replacer

A Bash utility for Oracle DBAs to find and replace a TNS entry across a large fleet of remote Oracle database servers over SSH.

## Background

In environments with many Oracle database servers, the `tnsnames.ora` file on each host must be kept in sync. When a database is migrated to a new server, renamed, or its connection details change, the TNS entry needs to be updated on every host manually — a slow and error-prone process at scale.

`tns_replacer.sh` automates this by SSHing into each host in a list, locating all `tnsnames.ora` files, and replacing the target TNS entry with a new version. It supports a test mode to assess impact before making any changes, safe and forced modes to control replacement strictness, and produces a full log with diffs for every file it touches.

---

## How It Works

1. Reads a list of target hosts from a file
2. SSHs into each host and searches for all `tnsnames.ora` files under a given path
3. Extracts the target TNS entry block from each file using Perl (handles deeply nested parentheses)
4. Compares the found block against the expected original content
5. Replaces it with the new content if the comparison passes
6. Creates a dated backup of every file before modifying it
7. Logs all actions and prints a summary report at the end

---

## Input Files

The script requires three input files, by default expected in the same directory as the script.

### `tnsr_hostlist.lst` — Host List

One hostname or IP address per line. Blank lines and lines starting with `#` are ignored.

```
# Production servers
db-prod-01.example.com
db-prod-02.example.com

# Test servers
db-test-01.example.com
```

### `tnsr_tns_origin.tns` — Original TNS Entry

The TNS entry block **as it currently exists** on the remote servers. This is what the script searches for. The entry must match what is in the remote `tnsnames.ora` files (see Safe Mode and Fuzzy matching below for how differences in whitespace are handled).

```
MYDB =
  (DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = old-db-server.example.com)(PORT = 1521))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = mydb.example.com)
      (SERVER = DEDICATED)
    )
  )
```

### `tnsr_tns_new.tns` — New TNS Entry

The replacement TNS entry block that will be written into the remote files.

```
MYDB =
  (DESCRIPTION =
    (ADDRESS_LIST =
      (ADDRESS = (PROTOCOL = TCP)(HOST = new-db-server.example.com)(PORT = 1521))
    )
    (CONNECT_DATA =
      (SERVICE_NAME = mydb.example.com)
      (SERVER = DEDICATED)
    )
  )
```

---

## Parameters

| Flag | Value | Default | Description |
|------|-------|---------|-------------|
| `-tn` | TNS name | *(required)* | Name of the TNS entry to search for |
| `-test` | — | off | Test mode: no changes made, match quality reported only |
| `-sm` | — | off | Safe mode: only replace on exact character match |
| `-f` | — | off | Forced mode: replace even if content does not match the origin |
| `-tp` | path | `/u01/app/oracle/product` | Remote path to search for `tnsnames.ora` files |
| `-fh` | file | `tnsr_hostlist.lst` | Path to the host list file |
| `-fto` | file | `tnsr_tns_origin.tns` | Path to the original TNS entry file |
| `-ftn` | file | `tnsr_tns_new.tns` | Path to the new TNS entry file |
| `-h` / `-help` | — | — | Show the help page and exit |

---

## Operating Modes

### Match Types

Before deciding whether to replace, the script compares the found block against the origin file in two ways:

| Match Type | What it means |
|------------|---------------|
| **Exact** | The block is character-perfect, including all whitespace and line endings |
| **Fuzzy** | The block content matches after stripping all whitespace and line breaks — formatting differs but the data is the same |
| **Mismatch** | The content differs — the TNS entry on the remote host is not the same as the expected origin |

---

### Test Mode (`-test`)

Connects to every host and reports the match quality for each `tnsnames.ora` file found. **No files are modified.** Use this before any real run to understand the current state across your fleet.

**Example:**
```bash
./tns_replacer.sh -tn MYDB -test
```

**Expected output per file:**
```
db-prod-01.example.com: /u01/app/oracle/product/19c/db_1/network/admin/tnsnames.ora -> Match (Exact)
db-prod-02.example.com: /u01/app/oracle/product/19c/db_1/network/admin/tnsnames.ora -> Match (Fuzzy)
db-test-01.example.com: /u01/app/oracle/product/19c/db_1/network/admin/tnsnames.ora -> Content mismatch
```

---

### Normal Mode (default)

The default operating mode when `-test`, `-sm`, and `-f` are all omitted. Replaces the TNS entry when an **Exact** or **Fuzzy** match is found. Creates a backup before modifying any file.

**Example — with default file names and search path:**
```bash
./tns_replacer.sh -tn MYDB
```

**Example — with a custom search path:**
```bash
./tns_replacer.sh -tn MYDB -tp /u01/app/oracle/product/19c
```

**Example — with custom input files:**
```bash
./tns_replacer.sh -tn MYDB -fh my_hosts.lst -fto origin_mydb.tns -ftn new_mydb.tns
```

**What it does:**
- Exact match → replaces, logs `Success (Replaced)`
- Fuzzy match → replaces, logs `Success (Replaced)`
- Mismatch → skips, logs `Failed (Mismatch)`
- Already up to date → skips, logs `Already up to date`
- Entry not found → skips, logs `NOT FOUND`

---

### Safe Mode (`-sm`)

Restricts replacement to **Exact matches only**. Fuzzy matches are skipped. Use this when the whitespace and formatting of the TNS entry is significant, or when you want to avoid touching files where formatting has drifted.

**Example:**
```bash
./tns_replacer.sh -tn MYDB -sm
```

**What it does compared to Normal Mode:**
- Exact match → replaces ✓
- Fuzzy match → **skips** (treated as mismatch) ✗
- Mismatch → skips ✗

---

### Forced Mode (`-f`)

Replaces the TNS entry on every host where it is found, **regardless of whether the content matches the origin file**. Use this when you know the remote entries have diverged and you want to overwrite them anyway.

> **Caution:** Forced mode will overwrite any version of the entry it finds. Always run with `-test` first to understand what is currently on each host before using `-f`.

**Example:**
```bash
./tns_replacer.sh -tn MYDB -f
```

**What it does:**
- Any found block → replaces, logs `Success (Forced replacement)`
- Already up to date → skips ✓
- Entry not found → skips ✓

---

### Combined Examples

**Test run with a custom host list and non-default search path:**
```bash
./tns_replacer.sh -tn MYDB -test -fh prod_hosts.lst -tp /u01/app/oracle/product/19c
```
Scans only the hosts in `prod_hosts.lst`, searches only under the 19c product path, reports match quality — no changes made.

**Safe mode with custom input files:**
```bash
./tns_replacer.sh -tn MYDB -sm -fto origin_mydb.tns -ftn new_mydb.tns -fh prod_hosts.lst
```
Uses custom origin and new files, replaces only on exact matches.

**Forced mode on a specific host list with a custom search path:**
```bash
./tns_replacer.sh -tn MYDB -f -fh emergency_hosts.lst -tp /u01/app/oracle
```
Overwrites the MYDB entry on all hosts in `emergency_hosts.lst` regardless of current content, searching the full Oracle base path.

---

## Output & Logging

### Console

During the run the console shows one line per host being processed, followed by a status line for each `tnsnames.ora` file found:

```
Processing Hostname: db-prod-01.example.com - 2026.03.19 10:05:01
db-prod-01.example.com: /u01/app/oracle/product/19c/db_1/network/admin/tnsnames.ora -> Success (Replaced)

Processing Hostname: db-prod-02.example.com - 2026.03.19 10:05:04
db-prod-02.example.com: /u01/app/oracle/product/19c/db_1/network/admin/tnsnames.ora -> Already up to date
```

Connection failures are printed immediately to the console regardless of other output:
```
db-test-01.example.com: Connection failed
```

### Log File

A timestamped log file (`tns_replacer_YYYYMMDD_HHMMSS.log`) is created in the working directory. It contains the full output for every host including diffs of modified files:

```
***** Host: db-prod-01.example.com *****
>>> FILE_STATUS: /u01/.../tnsnames.ora -> Success (Replaced)
==== DIFF ====
< (HOST = old-db-server.example.com)
> (HOST = new-db-server.example.com)
==== ==== ====
```

### Backup Files

Before any `tnsnames.ora` is modified, a backup is created in the same directory on the remote host:
```
tnsnames.ora.bck_tnsnames.ora_20260319
```

### Summary Report

Printed at the end of every run:

```
Found TNS entries for MYDB
--------------------------------------------
Already up-to-date:     2
Perfect TNS match:      0
Semi-perfect TNS match: 0
Content mismatch:       0
TNS entry not found:    1
Summa:                  3

Processed TNS entries
--------------------------------------------
Normal replacement: 1
Forced replacement: 0
Summa: 1
```

> In Test Mode the "Processed TNS entries" counts will always be 0 as no replacements are made.

---

## Requirements

- Bash
- SSH key-based authentication configured for all target hosts
- Perl available on all remote hosts (standard on Oracle Linux and RedHat)

---

## Security Notes

### `StrictHostKeyChecking=no`

The script uses `StrictHostKeyChecking=no` on all SSH connections. This means SSH will automatically accept any host key — including a **changed** key — without prompting or failing.

**Risk:** In an environment where a server is rebuilt or replaced and its host key changes, the script will connect silently without detecting the change. In an open or untrusted network this could allow a man-in-the-middle attack to intercept the session and receive the file modifications.

**Why it is set this way:** This script is intended for use in closed internal environments where all target hosts are trusted infrastructure machines and network-level isolation prevents MITM attacks. Automatic key acceptance avoids failures caused by routine server rebuilds that change host keys.

**Safer alternative:** On Oracle Linux 8+ (OpenSSH 7.6+) replace `StrictHostKeyChecking=no` with `StrictHostKeyChecking=accept-new`. This accepts keys for hosts that have never been seen before, but **rejects changed keys** and exits immediately — detecting rebuilt or replaced servers without blocking new additions to the fleet.
