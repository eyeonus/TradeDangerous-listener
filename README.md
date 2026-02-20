# TradeDangerous Listener

A listener system for **Trade Dangerous** that integrates live market data from the  
**Elite Dangerous Data Network (EDDN)** and supports both individual users and
server operators.

Python **3.10+** required.

---

## Repository Overview

This repository contains **two distinct listener roles**:

### 1️⃣ Client Listener  
For individual commanders running Trade Dangerous locally.

- Consumes live EDDN data
- Updates a local TD database
- Optionally refreshes from a TD server via `eddblink`
- Does **not** run Spansh
- Does **not** export CSVs
- Safe for SQLite or MariaDB

See:

```
README_client.md
```

---

### 2️⃣ Server Listener  
For operators running a Trade Dangerous data server.

- Consumes live EDDN data
- Runs automated Spansh imports
- Publishes `listings.csv` (full dump)
- Publishes `listings-live.csv` (delta since last dump)
- Uses multiprocessing
- Uses advisory locks for concurrency safety
- Supports scheduled purge and DB maintenance
- Intended for MariaDB/MySQL

See:

```
README_server.md
```

---

## Which one should I use?

If you are:

- A normal Trade Dangerous user
- Running locally
- Wanting live updates while playing

→ Use the **client listener**.

If you are:

- Operating infrastructure for other users
- Hosting a Trade Dangerous data service
- Managing exports and Spansh imports

→ Use the **server listener**.

---

## Shared Requirements

Both versions require:

- Trade Dangerous installed and importable
- Python 3.10+
- `pyzmq` installed

Install ZeroMQ bindings with:

```
pip install pyzmq
```

---

## Philosophy

The project now separates concerns clearly:

- The **client listener** is lightweight, safe, and designed for individual use.
- The **server listener** is production-oriented, concurrent, and built for
  controlled database environments.

They share core ingestion logic but differ significantly in:

- Process model
- Database expectations
- Export behaviour
- Maintenance responsibilities

---

## Notable Architecture Changes (vs legacy listener)

- No longer a single “side” switch in one script
- Explicit client and server roles
- Server version uses multiprocessing, advisory locking, and maintenance windows
- Client version remains thread-based and local-first
- Spansh import is server-only
- Export publishing is server-only

---

## Summary

This repository provides:

- A safe, local **client listener** for everyday Trade Dangerous use.
- A robust, concurrent **server listener** for publishing live data to others.

Choose the role appropriate to your environment and refer to the corresponding README.

---

If you are unsure which version you need, you almost certainly want the **client listener**.
