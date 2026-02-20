# TradeDangerous Listener for SERVER (tradedangerous_listener.py)
A multiprocessing listener/supervisor for TradeDangerous, designed to ingest live EDDN market data, periodically import Spansh dumps, and publish export CSVs for downstream clients.

Unless you are running a Trade:Dangerous SERVER you almost certainly want tradedangerous_listener_client.py and not this.

Python 3.10+ required. MariaDB backend ONLY.

---

# Notes

- This program requires TradeDangerous to be installed and importable (either via pip or from a TD checkout).
- The listener is now a **supervisor with multiple worker processes**, not a simple threaded script.
- The Spansh import is run automatically (unless disabled via CLI).
- Exports (`listings-live.csv` and `listings.csv`) are handled internally.
- EDDN uses 0MQ — you must install `pyzmq`.

Example:

```bash
pip install pyzmq
```

---

# Features

## 1) Live EDDN ingestion

- Connects to `tcp://eddn.edcd.io:9500`
- Subscribes to `commodity/3` schema
- Filters messages by configurable whitelist
- Coalesces updates per station (latest wins)
- Writes snapshots into `StationItem`
- Sets `from_live = 1` for all live updates
- Uses per-station advisory locks (MariaDB) to avoid deadlocks
- Never drops updates purely due to temporary lock contention (retries with backoff)

---

## 2) Spansh update process (separate process)

Runs automatically on startup, then every `check_update_every_x_hour` hours (default 24).

Process:

- HEAD request to `https://downloads.spansh.co.uk/galaxy_stations.json`
- Download if remote is newer
- Run `trade import -P spansh` in listener-safe mode
- Signal processor to refresh in-memory dict caches
- Publish full dump `listings.csv`
- Demote older `from_live=1` rows to baseline (`from_live=0`)

Disable entirely with:

```bash
python tradedangerous_listener.py --no-update
```

---

## 3) Live export process

Runs every `export_live_every_x_min` minutes (default 5).

- Exports `StationItem WHERE from_live=1`
- Writes `listings-live.csv`
- Uses atomic write (tmp + rename)
- Read-only DB access (does not pause processor)

---

## 4) Optional maintenance windows

The supervisor can perform scheduled:

- Purge of old `StationItem` rows
- MariaDB optimise/analyse operations

During maintenance:

- All worker processes are stopped
- Maintenance runs
- Workers restart cleanly

SQLite users should leave DB maintenance disabled.

---

# Running

Basic:

```bash
python tradedangerous_listener.py
```

Safe shutdown:

- Press **CTRL-C**
- Supervisor signals all workers
- Processes are joined cleanly
- Forced terminate/kill only if required

Do not kill the process abruptly unless necessary.

---

# Command Line Options

## Long-running mode

- `--no-update`
  Disable Spansh update process.

## One-shot utility modes (no worker processes started)

- `--stats`
  Print StationItem statistics (total rows, live rows, newest timestamp).

- `--export-live-now`
  Export a single `listings-live.csv` and exit.

- `--export-dump-now`
  Export a single `listings.csv` and exit.

---

# Configuration File

Default name:

```
tradedangerous-listener-config.json
```

Override location:

```
TD_LISTENER_CONFIG=/path/to/config.json
```

If missing, a default config is written on first run.

If invalid JSON is detected:
- The file is backed up to `*.broken.<timestamp>`
- A fresh default is written
- The program exits

Config updates (timestamps etc.) are written atomically with a lock file.

---

## Default Configuration

```json
{
    "verbose": true,
    "debug": false,
    "last_update": 0,
    "last_purge": 0,
    "last_db_maint": 0,
    "check_update_every_x_hour": 24,
    "spansh_log_interval": 30,
    "export_live_every_x_min": 5,
    "export_path": "./tmp",
    "purge_every_x_hour": 24,
    "purge_retention_days": 30,
    "db_maint_every_x_days": 30,
    "db_maint_cnf": "mariadb_check.cnf",
    "eddn_url": null,
    "eddn_schema_ref": null,
    "whitelist": [
        {
            "software": "E:D Market Connector [Windows]",
            "minversion": "6.0.0"
        },
        {
            "software": "E:D Market Connector [Mac OS]",
            "minversion": "6.0.0"
        },
        {
            "software": "E:D Market Connector [Linux]",
            "minversion": "6.0.0"
        },
        {
            "software": "EDDiscovery"
        },
        {
            "software": "EDDLite"
        }
    ]
}
```

---

## Configuration Notes

### verbose / debug
Control console logging.
- `debug` enables extra rejection diagnostics.

### whitelist
Filters EDDN messages by `header.softwareName`.

- Case-insensitive match.
- If `minversion` exists, versions below it are rejected.
- Version comparison uses `packaging.version`.

### check_update_every_x_hour
Interval between Spansh checks (runs once immediately on startup).

### spansh_log_interval
Controls progress logging during spansh import.

### export_live_every_x_min
Interval for live CSV export.

### export_path
Directory for:
- `listings-live.csv`
- `listings.csv`
- Optional diagnostics JSONL

### purge settings
- `purge_every_x_hour`
- `purge_retention_days`

Deletes StationItem rows older than retention window.

Set `purge_every_x_hour = 0` to disable.

### DB maintenance settings
- `db_maint_every_x_days`
- `db_maint_cnf`

Runs:
```
mariadb-check --optimize
mariadb-check --analyze
```

Requires:
- `TD_DB_CONFIG`
- `mariadb-check`
- mysql/mariadb client in PATH

Set `db_maint_every_x_days = 0` to disable.

---

# Optional / Advanced Config Keys

These are not in defaults but are supported if manually added.

## Diagnostics

- `listener_diag`
- `diagnostics`
- `listener_diag_stats_every`
- `listener_diag_path`

Enables JSONL diagnostic logging.

---

## DB retry tuning

- `listener_db_max_retries`
- `listener_db_backoff_min`
- `listener_db_backoff_cap`
- `listener_station_lock_backoff_min`
- `listener_station_lock_backoff_cap`

Tune retry/backoff behaviour for advisory locking.

---

## Throughput tuning

- `listener_coalesce_drain_cap`
- `listener_processor_rate_every`

Control batch size and rate logging interval.

---

# Environment Variables

## Listener-specific

- `TD_LISTENER_CONFIG`
  Override config path.

- `TD_CSV`
  Override TD CSV directory.

- `TDL_LISTENER_LOCK_TIMEOUT_SECONDS`
- `TDL_LISTENER_LOCK_MAX_RETRIES`
- `TDL_LISTENER_LOCK_BACKOFF_START_SECONDS`

Tune advisory lock behaviour.

---

## Test / Developer switches

- `TDL_REFRESH_QUICK_TEST=1`
  Skip Spansh and only trigger dict refresh event.

- `TDL_REFRESH_QUICK_TEST_DELAY_SECONDS`
  Delay for quick test mode.

- `TDL_TEST_FORCE_DEFER_EVERY_N`
  Artificially defer every Nth station update.

Not for production use.

---

## TradeDangerous DB layer

- `TD_DB_CONFIG`
  Path to TD DB configuration INI.

- `TD_DATA`
  Override TD data directory.

- `TD_TMP`
  Override TD tmp directory.

---

# How it works (current architecture)

The program runs as a supervisor and spawns four processes:

## 1) Listener process
- Receives EDDN ZMQ messages
- Decompresses payload
- Filters by schema + whitelist
- Dedupes per station within batch window
- Pushes newest per station into queue

## 2) Processor process
- Drains queue into per-station coalescer
- Acquires per-station advisory lock
- Deletes existing StationItem rows for station
- Inserts full snapshot
- Sets `from_live = 1`
- Commits in batches

If a station lock is busy:
- Update is deferred
- Retried with exponential backoff
- Not dropped

After Spansh import:
- Refreshes in-memory mapping dicts

---

## 3) Spansh update process
- Periodically checks remote dump
- Downloads if newer
- Runs TD spansh plugin
- Signals dict refresh
- Publishes `listings.csv`
- Re-baselines older live rows

---

## 4) Live exporter process
- Every N minutes:
  - Select `from_live = 1`
  - Write `listings-live.csv`
  - Atomic rename

---

# Meaning of from_live

`from_live = 1` means:
> Updated since the last published full dump.

After a Spansh import completes and `listings.csv` is published:
- Older `from_live=1` rows are demoted to `0`.

Therefore:

- `listings.csv` = full snapshot
- `listings-live.csv` = delta since last dump

---

# What changed from the original listener

- No more `side = client/server` mode.
- No global “busy pause everything” signalling.
- True multiprocessing with separate processes.
- Per-station advisory locking instead of whole-DB blocking.
- Coalescing queue instead of raw FIFO writes.
- Atomic config updates.
- Scheduled purge and DB maintenance support.
- Explicit full-dump publishing after Spansh.
