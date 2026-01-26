# TradeDangerous Listener - Client Version

A listener for **Trade Dangerous** that consumes live market data from the
**Elite Dangerous Data Network (EDDN)** and updates a local Trade Dangerous
database.

This repository now supports **two distinct listener roles**:

- **Client listener** (this file / default use-case)
- **Server listener** (advanced, server-only, documented elsewhere)

This README describes the **client listener**.

Python **3.10+** required.

---

## What the client listener is for

The **client listener** is designed for **individual users** who want:

- Near-real-time market updates while playing
- Automatic refreshes of Trade Dangerous data from the server
- No manual spansh imports
- No CSV export, no server-side publishing, no database locking complexity

It is safe to run against **SQLite**, **MariaDB**, or any SQLAlchemy-supported
backend, but SQLite is the normal client setup.

---

## What the client listener does

- Listens to the **Elite Dangerous Data Network (EDDN)** via ZeroMQ
- Accepts market messages from **whitelisted tools** (EDMC, EDDiscovery, etc.)
- Deduplicates and sanitises incoming data
- Updates the local Trade Dangerous database
- Periodically checks for updated listings from the server via **eddblink**
  and refreshes the local database automatically

**It does NOT:**
- Run spansh
- Export `listings.csv` or `listings-live.csv`
- Act as a data source for other users

---

## Requirements

- Trade Dangerous must be installed on the same machine
- `pyzmq` must be installed:
  ```
  pip install pyzmq
  ```
- If Trade Dangerous was installed via `pip`, the listener may live anywhere  
  Otherwise, it must be in the same directory as `trade.py`

---

## Running the client listener

```
python tradedangerous_listener_client.py
```

You’ll know it’s running when you see:

```
Press CTRL-C at any time to quit gracefully.
```

You may minimise the terminal and leave it running.

---

## Stopping safely

Always stop the listener with **CTRL-C**.

This allows the listener to shut down cleanly and avoids database corruption.
Closing the terminal window or killing the process is not safe.

(macOS users: if CTRL-C doesn’t work, try `⌘-.`)

---

## Command-line options

### `--no-update`

```
python tradedangerous_listener_client.py --no-update
```

Disables the **eddblink update checker**.

With this flag:
- Live EDDN messages are still processed
- No automatic refresh from the server occurs

This does **not** relate to spansh (which the client listener never runs).

---

## Configuration file

On first run, the listener creates:

```
tradedangerous-listener-config.json
```

The client listener **always runs in client mode**; the `side` value is forced
internally and cannot be used to switch roles.

### Default client configuration

```
{
    "side": "client",
    "verbose": true,
    "debug": false,
    "client_options": "clean",
    "check_update_every_x_min": 1440,
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

## Whitelist behaviour

- Entries **without** `minversion` accept all versions
- Entries **with** `minversion` reject older clients
- This protects against malformed or duplicate commodity messages

---

## How the client listener works (internals)

The client listener runs **three threads**:

### 1. Listener thread
- Subscribes to EDDN via ZeroMQ
- Filters messages by schema and whitelist
- Queues valid market messages

### 2. Message processor
- Dequeues messages
- Deduplicates commodity rows per station
- Ignores non-market (“cargo-only”) entries
- Writes clean, consistent data to the database

### 3. Update checker (optional)
- Periodically checks the Trade Dangerous server for updated listings
- Runs `eddblink` imports when updates are available
- Pauses message processing briefly during the import

No spansh code is involved at any point.

---

## Server listener (out of scope here)

The **server listener** is a separate, advanced configuration intended for
running a Trade Dangerous data server. It:

- Runs spansh imports
- Exports `listings-live.csv`
- Uses multiprocessing and database advisory locks
- Requires MariaDB/MySQL

That setup is intentionally **not covered in this README**.

---

## Summary

If you are:
- A Trade Dangerous user
- Running locally
- Wanting live market updates

→ **Use `tradedangerous_listener_client.py` and this README applies.**

If you are operating a data server for others, you want the server listener
documentation instead.
