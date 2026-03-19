# ORA User Ensure

A Bash utility for Oracle DBAs to verify that a specific Oracle user exists across a fleet of database servers, and optionally create it if missing. Supports single-instance databases, RAC environments, and Oracle multitenant (CDB/PDB) architectures.

## Background

In large Oracle environments, ensuring that a required database user exists on every server — and in every relevant PDB — is a tedious and error-prone task when done manually. This script automates the process: it SSHes into each host in a list, discovers all running Oracle instances, and for each one checks whether the target user exists. If it does not, the script can create it by running a supplied SQL script, either interactively (prompting per database) or fully automatically.

---

## How It Works

1. Reads a list of target hosts from a file
2. SSHes into each host and discovers running Oracle instances via `ora_pmon_*` processes (ASM, Grid, and MGMTDB are excluded)
3. For each instance, resolves the env file path using the configured pattern and `<SID>` token
4. If **PDB mode** is on (default): queries `v$pdbs` for all open, read-write, non-restricted PDBs and processes each one
5. If **PDB mode** is off: processes the SID directly as a single database
6. For each database context, runs a `SELECT COUNT(*)` to check if the user exists
7. If the user is missing: prompts (or auto-creates with `-ac`) by piping the SQL script to sqlplus
8. Writes a timestamped main log, a SQL execution log, and a rolling result summary

---

## Requirements

- Bash
- SSH key-based authentication configured for all target hosts
- Oracle `sqlplus` available on all remote hosts (via sourced env file)
- The connecting OS user must be able to connect as `/ as sysdba`

---

## Parameters

### Required

| Flag | Description |
|------|-------------|
| `-u <username>` | Oracle username to check and optionally create |

### Input Files

| Flag | Default | Description |
|------|---------|-------------|
| `-hosts <file>` | `hosts.txt` | List of target hostnames or IPs |
| `-ef <pattern>` | `~/env<SID>.sh` | Env file pattern — `<SID>` is replaced at runtime with the discovered SID |
| `-sql <file>` | `create_user.sql` | SQL script to run when creating the user |

### Output Files

| Flag | Default | Description |
|------|---------|-------------|
| `-l <file>` | `oracle_check_YYYYMMDD_HHMMSS.log` | Timestamped main log |
| `-rl <file>` | `latest_result.log` | Result summary — overwritten each run |
| `-sl <file>` | `oracle_sql_YYYYMMDD_HHMMSS.log` | SQL execution output log |

### Behaviour

| Flag | Default | Description |
|------|---------|-------------|
| `-pdb off` | on | Disable multitenant PDB handling |
| `-pdb-list A,B,C` | all qualifying | Only check these specific PDBs |
| `-rac off` | on | Disable RAC SID suffix stripping |
| `-ac` | off | Auto-create missing users without prompting |
| `-h` / `-help` | — | Show help and exit |

---

## Key Concepts

### The `<SID>` Token in Env File Patterns

The `-ef` flag accepts a pattern string with an optional `<SID>` token. At runtime, `<SID>` is replaced with the actual discovered SID for each database:

| Pattern | SID discovered | Resolved path |
|---------|---------------|---------------|
| `~/env<SID>.sh` | `PROD` | `~/envPROD.sh` |
| `~/envfiles/env_<SID>.sh` | `TESTDB` | `~/envfiles/env_TESTDB.sh` |
| `~/oracle_env.sh` | *(any)* | `~/oracle_env.sh` *(same file for all)* |

### RAC SID Stripping

In Oracle RAC, each node runs a numbered instance (`PROD1`, `PROD2`) of the same database. By default the script strips trailing digits so both instances map to the same env file (`envPROD.sh`). Use `-rac off` to disable this when your SID names legitimately end in digits.

### PDB Mode

When enabled (default), the script connects to each CDB and queries `v$pdbs` for qualifying PDBs:
- `open_mode = READ WRITE`
- `restricted = NO`
- Not `PDB$SEED` or `CDB$ROOT`

If `v$pdbs` returns no rows the instance is treated as a non-CDB automatically. Use `-pdb-list` to target only specific PDBs.

---

## Example Runs

### Basic check — prompts for creation if missing
```bash
./ora-user-ensure.sh -u APPUSER
```
Uses all defaults: reads `hosts.txt`, env pattern `~/env<SID>.sh`, PDB mode on, RAC mode on.

---

### Full flags — no defaults used
```bash
./ora-user-ensure.sh \
  -u     APPUSER \
  -hosts prod_hosts.lst \
  -ef    ~/envfiles/env_<SID>.sh \
  -sql   create_appuser.sql \
  -l     run_20260319.log \
  -rl    result_20260319.log \
  -sl    sql_20260319.log
```

---

### Auto-create mode — no interactive prompts
```bash
./ora-user-ensure.sh -u APPUSER -ac
```
Creates the user in every database where it is missing without asking. Useful for scheduled runs or CI/CD pipelines.

---

### Target specific PDBs only
```bash
./ora-user-ensure.sh -u APPUSER -pdb-list HR,FINANCE,CRM
```
Only checks and creates the user in the `HR`, `FINANCE`, and `CRM` PDBs. All other open PDBs are skipped.

---

### Disable PDB handling (non-CDB or legacy environments)
```bash
./ora-user-ensure.sh -u APPUSER -pdb off
```
Processes each discovered SID as a plain database. No PDB discovery query is run.

---

### RAC environment with per-instance env files
```bash
./ora-user-ensure.sh -u APPUSER -rac off -ef ~/env<SID>.sh
```
Disables SID stripping so `PROD1` and `PROD2` map to `~/envPROD1.sh` and `~/envPROD2.sh` respectively instead of both resolving to `~/envPROD.sh`.

---

### Single env file for all databases on a host
```bash
./ora-user-ensure.sh -u APPUSER -ef ~/oracle_env.sh
```
No `<SID>` in the pattern — the same env file is sourced for every database on every host.

---

### Combined: auto-create, PDB filter, custom files
```bash
./ora-user-ensure.sh \
  -u        MONITORING_USER \
  -hosts    monitoring_targets.lst \
  -ef       ~/env<SID>.sh \
  -sql      create_monitoring_user.sql \
  -pdb-list PROD_HR,PROD_FIN \
  -ac
```
Checks only the `PROD_HR` and `PROD_FIN` PDBs across all hosts, auto-creates the monitoring user where missing.

---

## Example Output

### Console / Main Log

```
2026-03-19 10:05:00 - ===== ora-user-ensure started =====
2026-03-19 10:05:00 - Target user : APPUSER
2026-03-19 10:05:00 - PDB mode    : on
2026-03-19 10:05:00 - RAC mode    : on
2026-03-19 10:05:00 - =================================================

2026-03-19 10:05:00 - --- db-prod-01.example.com : starting ---
2026-03-19 10:05:02 -  > DB: PROD
2026-03-19 10:05:02 -   -> Env file found: ~/envPROD.sh
2026-03-19 10:05:03 -   -> CDB detected. Processing PDBs...
2026-03-19 10:05:03 -    -> PDB: FINANCE
2026-03-19 10:05:04 -    [OK]      db-prod-01/PROD/FINANCE — APPUSER exists.
2026-03-19 10:05:04 -    -> PDB: HR
2026-03-19 10:05:05 -    [MISSING]  db-prod-01/PROD/HR — APPUSER not found.
   Create APPUSER in db-prod-01/PROD/HR? (y/n): y
2026-03-19 10:05:08 -    [CREATED] db-prod-01/PROD/HR — APPUSER created successfully.

2026-03-19 10:05:09 - --- db-prod-02.example.com : starting ---
2026-03-19 10:05:10 -    [SKIP]   Env file not found on db-prod-02: ~/envPROD.sh

2026-03-19 10:05:10 - --- db-test-01.example.com : starting ---
2026-03-19 10:05:11 - [UNREACHABLE] db-test-01.example.com — SSH connection failed.

2026-03-19 10:05:11 - ===== ora-user-ensure finished =====
```

### Result Summary (`latest_result.log`)

```
===== db-prod-01.example.com =====
>  db-prod-01/PROD/FINANCE - OK, APPUSER exists
>  db-prod-01/PROD/HR - CREATED, APPUSER created

===== db-prod-02.example.com =====
>  db-prod-02/PROD - SKIP, env file missing: ~/envPROD.sh

===== db-test-01.example.com =====
>  CONNECTION FAILED
```

### SQL Log (`oracle_sql_YYYYMMDD_HHMMSS.log`)

```
=========================================================================================
  HOST: db-prod-01.example.com | DB: PROD | PDB: HR | 2026-03-19 10:05:08
=========================================================================================
ALTER SESSION SET CONTAINER = HR;

User created.

Grant succeeded.

Grant succeeded.
```

---

## Output Files

| File | Purpose |
|------|---------|
| `oracle_check_YYYYMMDD_HHMMSS.log` | Full timestamped log of every action — one file per run |
| `latest_result.log` | Concise per-host/DB/PDB status summary — **overwritten** each run |
| `oracle_sql_YYYYMMDD_HHMMSS.log` | Full sqlplus output for every creation attempt, separated by host/DB/PDB headers |

---

## Notes

- Hosts that fail the SSH connectivity check are logged and skipped; the run continues
- `PDB$SEED` and `CDB$ROOT` are always excluded from PDB processing
- The result log is overwritten on every run — rename or copy it before re-running if you need to keep it
- The SQL creation script must include `EXIT;` at the end
- In PDB mode, `ALTER SESSION SET CONTAINER = <PDB>` is automatically prepended to the SQL script before execution — do not include it in the script itself
