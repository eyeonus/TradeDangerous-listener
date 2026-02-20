#!/usr/bin/env python3

from __future__ import generators
import os
import json
import time
import zlib
import zmq
import threading
import multiprocessing
import signal
from datetime import datetime, timedelta
import csv
import codecs
import configparser
import shutil
import subprocess
# TD SQLAlchemy session bootstrap (MariaDB-first; SQLite OK)
from contextlib import contextmanager
from typing import Generator
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import text

# Make things easier for Tromador.
# import ssl
#
# ssl._create_default_https_context = ssl._create_unverified_context

try:
    import cache
    import commands
    import trade
    import tradedb
    import tradeenv
    import transfers
    import plugins.spansh_plug
    # New SQLAlchemy DB API (repo-local)
    from db import load_config as load_db_config, make_engine_from_config, get_session_factory, ensure_fresh_db, resolve_data_dir
except ImportError:
    from tradedangerous import cli as trade, cache, tradedb, tradeenv, transfers, plugins, commands
    from tradedangerous.plugins import spansh_plug
    from tradedangerous.db import load_config as load_db_config, make_engine_from_config, get_session_factory, ensure_fresh_db, resolve_data_dir

from urllib import request
from calendar import timegm
from pathlib import Path
from collections import defaultdict, namedtuple, deque, OrderedDict
from packaging.version import Version

_CFG = None
_ENGINE = None
_DATA_DIR = None
_BACKEND = ""
_SessionFactory = None
_CONFIG_FILENAME = "tradedangerous-listener-config.json"

def get_config_path():
    env_path = os.environ.get("TD_LISTENER_CONFIG")
    if env_path:
        return Path(env_path)
    try:
        return Path(__file__).resolve().with_name(_CONFIG_FILENAME)
    except Exception:
        return Path(_CONFIG_FILENAME).resolve()

def init_db(cfg=None):
    global _CFG, _ENGINE, _DATA_DIR, _BACKEND, _SessionFactory
    if cfg is None or "database" not in cfg:
        cfg = load_db_config()
    _CFG = cfg
    _ENGINE = make_engine_from_config(_CFG)
    _DATA_DIR = resolve_data_dir(_CFG)
    _BACKEND = (_ENGINE.dialect.name or "").lower()
    _SessionFactory = get_session_factory(_ENGINE)

_minute = 60
_hour = 3600
_SPANSH_FILE = "galaxy_stations.json"
_SOURCE_URL = f'https://downloads.spansh.co.uk/{_SPANSH_FILE}'
# print(f'Spansh import plugin source file is: {_SOURCE_URL}')

@contextmanager
def sa_session() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy Session bound to the TD engine (one per use)."""
    if _SessionFactory is None:
        raise RuntimeError("Session factory not initialised; call init_db() first.")
    with _SessionFactory() as s:
        yield s

# Backend Specific Maintenance Routines
def perform_db_maintenance_sa():
    """
    MariaDB maintenance window task:
    run: mariadb-check --optimize --analyze
    
    Assumes ensure_db_maint_cnf_ready() has already validated:
      - db_maint_cnf exists/is usable (or generated)
      - TD_DB_CONFIG is present and has [mariadb] name
      - mariadb-check is available in PATH
    """
    raw = config.get("db_maint_cnf")
    cnf = Path.cwd() / "mariadb_check.cnf"
    if isinstance(raw, str) and raw.strip():
        p = Path(raw).expanduser()
        if p.is_absolute():
            cnf = p
    
    td_db_cfg = os.environ.get("TD_DB_CONFIG")
    if not td_db_cfg:
        print("[Maintenance] ERROR: TD_DB_CONFIG is not set; cannot run mariadb-check")
        raise SystemExit(2)
    
    cp = configparser.ConfigParser()
    cp.read(td_db_cfg)
    db_name = (cp.get("mariadb", "name", fallback="") or "").strip()
    if not db_name:
        print("[Maintenance] ERROR: TD_DB_CONFIG missing [mariadb] name; cannot run mariadb-check")
        raise SystemExit(2)
    
    def run_check(op_flag, label):
        started = time.time()
        print(f"[Maintenance] Running mariadb-check {op_flag} ({db_name})")
        res = subprocess.run(
            [
                "mariadb-check",
                f"--defaults-extra-file={str(cnf)}",
                op_flag,
                db_name,
            ],
            capture_output=True,
            text=True,
        )
        elapsed = time.time() - started
        
        print(f"[Maintenance] mariadb-check {label} rc={res.returncode} duration={elapsed:.1f}s")
        
        out = (res.stdout or "").strip()
        err = (res.stderr or "").strip()
        
        if res.returncode != 0:
            if out:
                print(f"[Maintenance] mariadb-check {label} stdout:")
                print(out)
            if err:
                print(f"[Maintenance] mariadb-check {label} stderr:")
                print(err)
        else:
            if out:
                lines = out.splitlines()
                first = lines[0] if lines else ""
                more = max(len(lines) - 1, 0)
                if more:
                    print(f"[Maintenance] mariadb-check {label} stdout: {first} (+{more} lines)")
                else:
                    print(f"[Maintenance] mariadb-check {label} stdout: {first}")
            if err:
                print(f"[Maintenance] mariadb-check {label} stderr (non-fatal):")
                print(err)
        
        return res
    
    res = run_check("--optimize", "optimize")
    if res.returncode != 0:
        return res
    
    res = run_check("--analyze", "analyze")
    return res

class MarketPrice(namedtuple('MarketPrice', [
        'system',
        'station',
        'market_id',
        'commodities',
        'timestamp',
        'uploader',
        'software',
        'version',
        ])):
    pass

class Listener(object):
    """
    Provides an object that will listen to the Elite Dangerous Data Network
    firehose and capture messages for later consumption.
    
    Rather than individual updates, prices are captured across a window of
    between minBatchTime and maxBatchTime. When a new update is received,
    Rather than returning individual messages, messages are captured across
    a window of potentially several seconds and returned to the caller in
    batches.
    
    Attributes:
        zmqContext          Context this object is associated with,
        minBatchTime        Allow at least this long for a batch (ms),
        maxBatchTime        Don't allow a batch to run longer than this (ms),
        reconnectTimeout    Reconnect the socket after this long with no data,
        burstLimit          Read a maximum of this many messages between
                            timer checks
        subscriber          ZMQ socket we're using
        lastRecv            time of the last receive (or 0)
    """
    
    uri = 'tcp://eddn.edcd.io:9500'
    supportedSchema = 'https://eddn.edcd.io/schemas/commodity/3'
    
    def __init__(
        self,
        zmqContext = None,
        minBatchTime = 36.,  # seconds
        maxBatchTime = 60.,  # seconds
        reconnectTimeout = 30.,  # seconds
        burstLimit = 500,
    ):
        self.lastJsData = None
        self.lastRecv = None
        assert burstLimit > 0
        if not zmqContext:
            print(f"[listener] PID {os.getpid()} ZMQ context init")
            zmqContext = zmq.Context()
        self.zmqContext = zmqContext
        self.subscriber = None
        
        self.minBatchTime = minBatchTime
        self.maxBatchTime = maxBatchTime
        self.reconnectTimeout = reconnectTimeout
        self.burstLimit = burstLimit
        
        self.connect()
    
    def connect(self):
        """
        Start a connection
        """
        # tear up the new connection first
        if self.subscriber:
            self.subscriber.close()
            del self.subscriber
        self.subscriber = newsub = self.zmqContext.socket(zmq.SUB)
        newsub.setsockopt(zmq.SUBSCRIBE, b"")
        newsub.connect(self.uri)
        self.lastRecv = time.time()
        self.lastJsData = None
    
    def disconnect(self):
        del self.subscriber
    
    def wait_for_data(self, softCutoff, hardCutoff):
        """
        Waits for data until maxBatchTime ms has elapsed
        or cutoff (absolute time) has been reached.
        """
        
        now = time.time()
        
        cutoff = min(softCutoff, hardCutoff)
        if self.lastRecv < now - self.reconnectTimeout:
            self.connect()
            now = time.time()
        
        nextCutoff = min(now + self.minBatchTime, cutoff)
        if now > nextCutoff:
            return False
        
        timeout = (nextCutoff - now) * 1000  # milliseconds
        
        # Wait for an event
        events = self.subscriber.poll(timeout = timeout)
        if events == 0:
            return False
        return True
    
    def get_batch(self, queue, stop_event=None):
        """
        Greedily collect deduped prices from the firehose over a
        period of between minBatchTime and maxBatchTime, with
        built-in auto-reconnection if there is nothing from the
        firehose for a period of time.
        
        As json data is decoded, it is stored in self.lastJsData.
        
        Validated market list messages are added to the queue.
        """
        while (stop_event is None or not stop_event.is_set()):
            now = time.time()
            hardCutoff = now + self.maxBatchTime
            softCutoff = now + self.minBatchTime
            
            # hoists
            supportedSchema = self.supportedSchema
            sub = self.subscriber
            
            # Prices are stored as a dictionary of
            # (sys,stn,item) => [MarketPrice]
            # The list thing is a trick to save us having to do
            # the dictionary lookup twice.
            batch = defaultdict(list)
            
            bursts = 0
            if self.wait_for_data(softCutoff, hardCutoff):
                # When wait_for_data returns True, there is some data waiting,
                # possibly multiple messages. At this point we can afford to
                # suck down whatever is waiting in "nonblocking" mode until
                # we either reach the burst limit or get EAGAIN.
                for _ in range(self.burstLimit):
                    if stop_event is not None and stop_event.is_set():
                        break
                    self.lastJsData = None
                    zdata = None
                    try:
                        zdata = sub.recv(flags = zmq.NOBLOCK, copy = False)
                    except zmq.error.Again:
                        continue
                    except zmq.error.ZMQError:
                        pass
                    
                    self.lastRecv = time.time()
                    bursts += 1
                    
                    try:
                        jsdata = zlib.decompress(zdata)
                    except Exception:
                        continue
                    
                    bdata = jsdata.decode()
                    
                    try:
                        data = json.loads(bdata)
                    except ValueError:
                        continue
                    
                    self.lastJsData = jsdata
                    
                    try:
                        schema = data["$schemaRef"]
                    except KeyError:
                        continue
                    if schema != supportedSchema:
                        continue
                    try:
                        header = data["header"]
                        message = data["message"]
                        system = message["systemName"].upper()
                        station = message["stationName"].upper()
                        market_id = message["marketId"]
                        commodities = message["commodities"]
                        timestamp = message["timestamp"]
                        uploader = header["uploaderID"]
                        software = header["softwareName"]
                        swVersion = header["softwareVersion"]
                    except (KeyError, ValueError):
                        continue
                    whitelist_match = list(filter(lambda x: x.get('software').lower() == software.lower(), config['whitelist']))
                    # Upload software not on whitelist is ignored.
                    if len(whitelist_match) == 0:
                        if config['debug'] or config['verbose']:
                            print(f'{system}/{station} update rejected from client not on whitelist: {software} v{swVersion}')
                        continue
                    
                    # Upload software with version less than the defined minimum is ignored.
                    if whitelist_match[0].get("minversion"):
                        if Version(swVersion) < Version(whitelist_match[0].get("minversion")):
                            if config['debug']:
                                print(f'{system}/{station} rejected with: {software} v{swVersion}')
                            continue
                    # We've received real data.
                    
                    # Normalize timestamps
                    timestamp = timestamp.replace("T", " ").replace("+00:00", "")
                    
                    # Find the culprit!
                    if '.' in timestamp and config['debug']:
                        print(f'Client {software}, v{swVersion}, uses microseconds.')
                        for key in header:
                            if "timestamp" in key:
                                print(f'{key}: {header[key]}')
                        for key in message:
                            if "timestamp" in key:
                                print(f'{key}: {message[key]}')
                    
                    # We'll get either an empty list or a list containing
                    # a MarketPrice. This saves us having to do the expensive
                    # index operation twice.
                    oldEntryList = batch[(system, station)]
                    if oldEntryList:
                        if oldEntryList[0].timestamp > timestamp:
                            continue
                    else:
                        # Add a blank entry to make the list size > 0
                        oldEntryList.append(None)
                    
                    # Here we're replacing the contents of the list.
                    # This simple array lookup is several hundred times less
                    # expensive than looking up a potentially large dictionary
                    # by STATION/SYSTEM:ITEM...
                    oldEntryList[0] = MarketPrice(
                        system, station, market_id, commodities,
                        timestamp,
                        uploader, software, swVersion,
                    )
            
            # For the edge-case where we wait 4.999 seconds and then
            # get a burst of data: stick around a little longer.
            if bursts >= self.burstLimit:
                softCutoff = min(softCutoff, time.time() + 0.5)
            
            for entry in batch.values():
                payload = entry[0]
                if hasattr(payload, "_asdict"):
                    payload = payload._asdict()
                if hasattr(queue, "put"):
                    queue.put(payload)
                else:
                    queue.append(payload)
        self.disconnect()
        print("Listener reporting shutdown.")

def run_listener(stop, queue, cfg):
    """
    Runner for the EDDN listener. Constructs its own Listener and pumps
    market batches into the provided queue.
    """
    global config
    config = cfg
    if multiprocessing.current_process().name != "MainProcess":
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    print(f"[listener] PID {os.getpid()} starting")
    listener = Listener()
    listener.get_batch(queue, stop_event=stop)
    print(f"[listener] PID {os.getpid()} exiting")

def run_update(stop, cfg, flags=None, refresh_event=None):
    """
    Batch update/import runner.
    
    Every N hours (check_update_every_x_hour):
      1) Run Spansh import
      2) If (and only if) Spansh succeeds, export listings.csv
    
    No busy/ack is used. Live ingestion/export continues concurrently.
    """
    global config, refresh_dicts_event, spansh_busy
    config = cfg
    refresh_dicts_event = refresh_event
    if flags is not None:
        spansh_busy = flags.get("spansh_busy", spansh_busy)
    
    if multiprocessing.current_process().name != "MainProcess":
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    
    try:
        init_db(cfg)
    except Exception as e:
        print(f"[Update] failed to initialise DB: {e!r}")
        return
    
    interval_hours = float(cfg.get("check_update_every_x_hour", 24) or 24)
    interval_seconds = max(1.0, interval_hours * _hour)
    
    # Set false to defer Spansh for 'check_update_every_x_hour' before running.
    run_immediately = True
    
    print(f"[Update] PID {os.getpid()} starting (interval={interval_seconds}s)")
    
    quick_test = os.environ.get("TDL_REFRESH_QUICK_TEST") == "1"
    if quick_test:
        delay = os.environ.get("TDL_REFRESH_QUICK_TEST_DELAY_SECONDS")
        try:
            delay_seconds = float(delay) if delay else 2.0
        except Exception:
            delay_seconds = 2.0
        print(f"[Update] Quick-test mode: signalling dict refresh in {delay_seconds}s")
        if stop is not None:
            if stop.wait(delay_seconds):
                return
        else:
            time.sleep(delay_seconds)
        if refresh_dicts_event is not None:
            try:
                refresh_dicts_event.set()
                print("[Update] Quick-test: set refresh_dicts_event")
            except Exception as e:
                print(f"[Update] Quick-test: failed to set refresh_dicts_event: {e!r}")
        return
    
    while (stop is None or not stop.is_set()):
        if not run_immediately:
            if stop is not None:
                if stop.wait(interval_seconds):
                    break
            else:
                time.sleep(interval_seconds)
        run_immediately = False
        
        try:
            tdb = tradedb.TradeDB(load=False)
            update_file = Path(tdb.tdenv.tmpDir, _SPANSH_FILE)
            
            if spansh_busy is not False and hasattr(spansh_busy, "set"):
                spansh_busy.set()
            
            # Ensure we have a local file to import. Refresh if remote is newer.
            last_modified = 0
            try:
                req = request.Request(_SOURCE_URL, method="HEAD")
                with request.urlopen(req, timeout=30) as response:
                    url_time = response.getheader("Last-Modified")
                if url_time:
                    last_modified = int(datetime.strptime(
                        url_time,
                        "%a, %d %b %Y %H:%M:%S %Z",
                    ).timestamp())
            except Exception as e:
                print(f"[Spansh] WARNING: failed to read dump headers: {e!r}")
            
            local_mod_time = 0 if not update_file.exists() else update_file.stat().st_mtime
            if (not update_file.exists()) or (last_modified and local_mod_time < last_modified):
                if update_file.exists():
                    update_file.unlink()
                
                print(f"[Spansh] Download start: {_SOURCE_URL}")
                tmp_file = update_file.with_suffix(update_file.suffix + ".tmp")
                if tmp_file.exists():
                    tmp_file.unlink()
                
                started = time.time()
                try:
                    req = request.Request(_SOURCE_URL, headers={"User-Agent": "TradeDangerous"})
                    with request.urlopen(req, timeout=60) as resp, open(tmp_file, "wb") as out:
                        while True:
                            chunk = resp.read(8 * 1024 * 1024)
                            if not chunk:
                                break
                            out.write(chunk)
                    os.replace(tmp_file, update_file)
                finally:
                    if tmp_file.exists():
                        tmp_file.unlink()
                
                if last_modified:
                    os.utime(update_file, (last_modified, last_modified))
                
                elapsed = time.time() - started
                size = update_file.stat().st_size
                print(f"[Spansh] Download complete: {size} bytes in {elapsed:.1f}s")
            
            # Keep existing maxage logic bounded (unchanged semantics from baseline).
            last_update_ts = cfg.get("last_update", 0)
            if last_update_ts and last_update_ts > 0:
                days_since = (datetime.now() - datetime.fromtimestamp(last_update_ts)) / timedelta(days=1)
                maxage = min(days_since + 1.5, 30)
            else:
                maxage = 30
            options = '-'
            if cfg.get('debug'):
                options += 'w'
            if cfg.get('verbose'):
                options += 'vv'
            if options == '-':
                options = ''
            
            if stop is not None and hasattr(stop, "is_set") and stop.is_set():
                break
            
            log_interval = int(cfg.get('spansh_log_interval', 30) or 30)
            
            try:
                print("[Spansh] Running import")
                try:
                    trade.main((
                        'trade.py', 'import', '-P', 'spansh',
                        '-O', f'file={update_file},maxage={maxage},skip_stationitems=1,listener_mode=1,log_interval={log_interval}',
                        options,
                    ))
                except SystemExit as e:
                    print(f"[Spansh] Import failed: {e!r}")
                    continue
                
                if stop is not None and hasattr(stop, "is_set") and stop.is_set():
                    break
                
                last_modified_ts = int(last_modified) if last_modified else 0
                if last_modified_ts > 0:
                    cfg['last_update'] = last_modified_ts
                    update_config_keys({
                        "last_update": last_modified_ts,
                    })
                else:
                    print(
                        "[Spansh] WARNING: missing/invalid Last-Modified; "
                        "preserving existing last_update"
                    )
                
                print("[Spansh] Import complete")
                if refresh_dicts_event is not None:
                    try:
                        refresh_dicts_event.set()
                        print("[Update] Signalled processor to refresh dict caches")
                    except Exception as e:
                        print(f"[Update] WARNING: failed to signal dict refresh: {e!r}")
                # from_live semantic: "delta since last published dump".
                # Draw the line in the sand immediately before publishing the new dump.
                dump_cutoff = datetime.utcnow()
                
                print("[Dump] Exporting listings.csv")
                ok = run_dump_exporter(stop, cfg)
                if ok:
                    print("[Dump] Export complete")
                    try:
                        demote_sql = text("""
                            UPDATE StationItem
                            SET from_live = 0
                            WHERE from_live = 1
                              AND (modified IS NULL OR modified < :cutoff)
                        """)
                        with sa_session() as s:
                            res = s.execute(demote_sql, {"cutoff": dump_cutoff})
                            s.commit()
                            demoted = getattr(res, "rowcount", 0) or 0
                        print(f"[from_live] Demoted {int(demoted):,} row(s) (cutoff={dump_cutoff})")
                    except Exception as e:
                        print(f"[from_live] WARNING: demotion failed: {e!r}")
                else:
                    print("[Dump] Export failed")
            finally:
                if spansh_busy is not False and hasattr(spansh_busy, "clear"):
                    spansh_busy.clear()
        
        except Exception as e:
            print(f"[Update] ERROR: batch cycle failed: {e!r}")
            # Failure handling rule: no immediate retry loop; wait for next cycle.
            continue
    
    print("[Update] reporting shutdown.")

def default_config():
    """
    Returns the default listener config as an OrderedDict.
    Kept as a function so load_config() and validate_config() share a single default source.
    """
    return OrderedDict([
        ('verbose', True),
        ('debug', False),
        ('last_update', 0),
        ('last_purge', 0),
        ('last_db_maint', 0),
        ('check_update_every_x_hour', 24),
        ('spansh_log_interval', 30),
        ('export_live_every_x_min', 5),
        ('export_path', './tmp'),
        # Purge schedule (0 disables)
        ('purge_every_x_hour', 24),
        ('purge_retention_days', 30),
        # DB maintenance schedule (0 disables)
        ('db_maint_every_x_days', 30),
        ('db_maint_cnf', 'mariadb_check.cnf'),
        # Added for EDDN compatibility with MP listener (kept even if unset)
        ('eddn_url', None),
        ('eddn_schema_ref', None),
        ('whitelist',
            [
                OrderedDict([('software', 'E:D Market Connector [Windows]'), ('minversion', '6.0.0')]),
                OrderedDict([('software', 'E:D Market Connector [Mac OS]'), ('minversion', '6.0.0')]),
                OrderedDict([('software', 'E:D Market Connector [Linux]'), ('minversion', '6.0.0')]),
                OrderedDict([('software', 'EDDiscovery')]),
                OrderedDict([('software', 'EDDLite')]),
            ]
        ),
    ])

def load_config():
    """
    Loads the settings from 'tradedangerous-listener-config.json'.
    If the config_file does not exist or is missing any settings,
    the default will be used for any missing setting,
    and the config_file will be updated to include all settings,
    preserving the existing (if any) settings' current values.
    """
    # Initialize config with default settings.
    # NOTE: Whitespace added for readability.
    config = default_config()
    config_path = get_config_path()
    
    # Load the settings from the configuration file if it exists.
    if config_path.exists():
        with open(config_path, "r") as fh:
            try:
                temp = json.load(fh, object_pairs_hook=OrderedDict)
            except Exception as e:
                path = config_path
                ts = int(time.time())
                backup_path = path.with_suffix(path.suffix + f".broken.{ts}")
                lock_path = path.with_suffix(path.suffix + ".lock")
                tmp_path = path.with_suffix(path.suffix + ".tmp")
                
                print(f"[Config] ERROR: failed to parse {path.name}: {e!r}")
                print(f"[Config] ERROR: backing up broken config to: {backup_path}")
                
                with open(lock_path, "a+", encoding="utf-8") as lock_fh:
                    try:
                        import fcntl
                        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
                    except Exception:
                        pass
                    
                    try:
                        os.replace(path, backup_path)
                    except FileNotFoundError:
                        # Another process likely already moved it; do not treat as fatal.
                        print("[Config] ERROR: config file already moved; proceeding.")
                    except Exception as backup_err:
                        print(f"[Config] ERROR: failed to back up broken config: {backup_err!r}")
                        print("[Config] ERROR: refusing to overwrite the existing file.")
                        raise SystemExit(2)
                    
                    # If a replacement already exists now, leave it untouched.
                    if path.exists():
                        print(f"[Config] ERROR: replacement config already exists at: {path}")
                    else:
                        try:
                            with open(tmp_path, "w", encoding="utf-8") as out:
                                json.dump(config, out, indent=4)
                                out.flush()
                                os.fsync(out.fileno())
                            os.replace(tmp_path, path)
                        finally:
                            try:
                                if tmp_path.exists():
                                    tmp_path.unlink()
                            except Exception:
                                pass
                        
                        print(f"[Config] ERROR: wrote fresh default config to: {path}")
                
                print("[Config] ERROR: fix the config as needed, then restart.")
                raise SystemExit(2)
        
        missing = OrderedDict()
        # For each setting in config, if file setting exists overwrite config setting.
        # If missing, backfill only that key.
        for setting in config:
            if setting in temp:
                if config[setting] != temp[setting]:
                    config[setting] = temp[setting]
            else:
                missing[setting] = config[setting]
        
        if missing:
            update_config_keys(missing)
    else:
        # If the config_file doesn't exist, create it with defaults.
        update_config_keys(config)
    
    return config

def update_config_keys(updates, config_path=None):
    """
    Atomically update specific keys in the on-disk JSON config file, without
    clobbering unrelated keys written by other processes.
    
    Uses an adjacent lock file (fcntl.flock on POSIX) and os.replace for atomicity.
    """
    path = Path(config_path) if config_path is not None else get_config_path()
    lock_path = path.with_suffix(path.suffix + ".lock")
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    
    with open(lock_path, "a+", encoding="utf-8") as lock_fh:
        try:
            import fcntl
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        except Exception:
            # Best-effort fallback (e.g. non-POSIX); atomic replace still helps.
            pass
        
        try:
            with open(path, "r", encoding="utf-8") as fh:
                cfg_on_disk = json.load(fh, object_pairs_hook=OrderedDict)
        except FileNotFoundError:
            cfg_on_disk = OrderedDict()
        except Exception as e:
            ts = int(time.time())
            backup_path = path.with_suffix(path.suffix + f".broken.{ts}")
            
            print(f"[Config] ERROR: failed to parse {path.name}: {e!r}")
            print(f"[Config] ERROR: backing up broken config to: {backup_path}")
            try:
                os.replace(path, backup_path)
            except FileNotFoundError:
                print("[Config] ERROR: config file already moved; proceeding.")
            except Exception as backup_err:
                print(f"[Config] ERROR: failed to back up broken config: {backup_err!r}")
                print("[Config] ERROR: refusing to overwrite the existing file.")
                raise SystemExit(2)
            
            if path.exists():
                print(f"[Config] ERROR: replacement config already exists at: {path}")
                print("[Config] ERROR: refusing to overwrite the existing file.")
                raise SystemExit(2)
            
            fresh_cfg = default_config()
            for k, v in updates.items():
                fresh_cfg[k] = v
            
            try:
                with open(tmp_path, "w", encoding="utf-8") as out:
                    json.dump(fresh_cfg, out, indent=4)
                    out.flush()
                    os.fsync(out.fileno())
                os.replace(tmp_path, path)
            finally:
                try:
                    if tmp_path.exists():
                        tmp_path.unlink()
                except Exception:
                    pass
            
            print(f"[Config] ERROR: wrote fresh default config to: {path}")
            print("[Config] ERROR: fix the config as needed, then restart.")
            raise SystemExit(2)
        
        for k, v in updates.items():
            cfg_on_disk[k] = v
        
        try:
            with open(tmp_path, "w", encoding="utf-8") as out:
                json.dump(cfg_on_disk, out, indent=4)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp_path, path)
        finally:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

def validate_config():
    """
    Validates loaded config values. Invalid keys are reset to defaults and
    persisted key-by-key via update_config_keys() to avoid clobbering unrelated keys.
    """
    global config
    defaults = default_config()
    
    updates = OrderedDict()
    
    def _is_int(v):
        return isinstance(v, int) and not isinstance(v, bool)
    
    def _reset(key):
        if key in defaults:
            updates[key] = defaults[key]
            config[key] = defaults[key]
    
    if not isinstance(config.get("verbose"), bool):
        _reset("verbose")
    
    if not isinstance(config.get("debug"), bool):
        _reset("debug")
    
    for key in ("last_update", "last_purge", "last_db_maint"):
        v = config.get(key)
        if not (_is_int(v) and v >= 0):
            _reset(key)
    
    v = config.get("check_update_every_x_hour")
    if not (_is_int(v) and 1 <= v <= 240):
        _reset("check_update_every_x_hour")
    
    v = config.get("spansh_log_interval")
    if not (_is_int(v) and 1 <= v <= 3600):
        _reset("spansh_log_interval")
    
    v = config.get("export_live_every_x_min")
    if not (_is_int(v) and 1 <= v <= 720):
        _reset("export_live_every_x_min")
    
    v = config.get("export_path")
    if not (isinstance(v, str) and v.strip()):
        _reset("export_path")
    
    v = config.get("purge_every_x_hour")
    if not (_is_int(v) and v >= 0):
        _reset("purge_every_x_hour")
    
    v = config.get("purge_retention_days")
    if not (_is_int(v) and v >= 1):
        _reset("purge_retention_days")
    
    v = config.get("db_maint_every_x_days")
    if not (_is_int(v) and v >= 0):
        _reset("db_maint_every_x_days")
    
    v = config.get("db_maint_cnf")
    if not (isinstance(v, str) and v.strip()):
        _reset("db_maint_cnf")
    
    for key in ("eddn_url", "eddn_schema_ref"):
        v = config.get(key)
        if v is not None and not isinstance(v, str):
            _reset(key)
    
    v = config.get("whitelist")
    if not isinstance(v, list):
        _reset("whitelist")
    
    if updates:
        update_config_keys(updates)
        config = load_config()

def ensure_db_maint_cnf_ready(cfg: dict) -> None:
    """
    If db maintenance is enabled (db_maint_every_x_days > 0), ensure db_maint_cnf exists,
    is parseable, and can authenticate non-interactively *now* (SELECT 1), before children spawn.
    
    If the cnf is missing/invalid, generate/overwrite it from TD_DB_CONFIG.
    If connection test fails, retry once after regeneration; then exit non-zero.
    """
    try:
        days = int(cfg.get("db_maint_every_x_days", 0) or 0)
    except Exception:
        days = -1
    
    if days <= 0:
        return
    
    def _fail(msg: str) -> None:
        print(f"[Maintenance] ERROR: {msg}")
        raise SystemExit(2)
    
    raw = cfg.get("db_maint_cnf")
    target = None
    if isinstance(raw, str) and raw.strip():
        p = Path(raw).expanduser()
        if p.is_absolute():
            try:
                if p.exists():
                    if os.access(str(p), os.R_OK | os.W_OK):
                        target = p
                else:
                    if p.parent.exists() and os.access(str(p.parent), os.W_OK):
                        target = p
            except Exception:
                target = None
    
    if target is None:
        target = Path.cwd() / "mariadb_check.cnf"
    
    td_db_cfg = os.environ.get("TD_DB_CONFIG")
    if not td_db_cfg:
        _fail("TD_DB_CONFIG is not set; cannot derive MariaDB credentials for db_maint_cnf")
    
    dbp = configparser.ConfigParser()
    try:
        read_ok = dbp.read(td_db_cfg)
    except Exception:
        read_ok = []
    
    if not read_ok or not dbp.has_section("mariadb"):
        _fail(f"Unable to read [mariadb] section from TD_DB_CONFIG={td_db_cfg!r}")
    
    m = dbp["mariadb"]
    host = (m.get("host") or "").strip()
    port = (m.get("port") or "").strip()
    socket = (m.get("socket") or "").strip()
    user = (m.get("user") or "").strip()
    password = (m.get("password") or "").strip()
    name = (m.get("name") or "").strip()
    if not (user and password and name and (socket or (host and port))):
        _fail("TD_DB_CONFIG missing required [mariadb] keys (need user,password,name and either socket or host+port)")
    
    if shutil.which("mariadb-check") is None:
        _fail("mariadb-check not found in PATH (required for db maintenance)")
    
    client_bin = shutil.which("mariadb") or shutil.which("mysql")
    if client_bin is None:
        _fail("neither 'mariadb' nor 'mysql' client found in PATH (required for connection test)")
    
    def _cnf_valid(path: Path) -> bool:
        cp = configparser.ConfigParser()
        try:
            ok = cp.read(path)
        except Exception:
            return False
        if not ok or not cp.has_section("client"):
            return False
        sec = cp["client"]
        
        sock = (sec.get("socket") or "").strip()
        h = (sec.get("host") or "").strip()
        p = (sec.get("port") or "").strip()
        u = (sec.get("user") or "").strip()
        pw = (sec.get("password") or "").strip()
        if not (u and pw and (sock or (h and p))):
            return False
        if p:
            try:
                int(p)
            except Exception:
                return False
        return True
    
    def _write_cnf() -> None:
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        with open(target, "w", encoding="utf-8") as fh:
            fh.write("[client]\n")
            if socket:
                fh.write(f"socket={socket}\n")
            else:
                fh.write(f"host={host}\n")
                fh.write(f"port={port}\n")
            fh.write(f"user={user}\n")
            fh.write(f"password={password}\n")
        try:
            os.chmod(target, 0o600)
        except Exception:
            pass
    
    def _test():
        cmd = [
            client_bin,
            f"--defaults-extra-file={str(target)}",
            "--connect-timeout=5",
            "-D", name,
            "-e", "SELECT 1",
        ]
        return subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    
    if not _cnf_valid(target):
        print(f"[Maintenance] db_maint_cnf missing/invalid; generating: {target}")
        try:
            _write_cnf()
        except Exception as e:
            _fail(f"failed to write db_maint_cnf at {str(target)!r}: {e!r}")
    
    res = _test()
    if res.returncode != 0:
        print("[Maintenance] db_maint_cnf connection test failed; regenerating from TD_DB_CONFIG and retrying.")
        if res.stdout.strip():
            print(f"[Maintenance] stdout: {res.stdout.strip()}")
        if res.stderr.strip():
            print(f"[Maintenance] stderr: {res.stderr.strip()}")
        try:
            _write_cnf()
        except Exception as e:
            _fail(f"failed to rewrite db_maint_cnf at {str(target)!r}: {e!r}")
        res = _test()
    
    if res.returncode != 0:
        if res.stdout.strip():
            print(f"[Maintenance] stdout: {res.stdout.strip()}")
        if res.stderr.strip():
            print(f"[Maintenance] stderr: {res.stderr.strip()}")
        _fail(f"db_maint_cnf connection test failed for database {name!r} using {str(target)!r}")
    
    print(f"[Maintenance] db_maint_cnf OK: {str(target)} (connect test ok)")

def db_locked_message(source: str)  -> None:
    print(f"[{source}] - DB locked, waiting for access.", end="\n")
    time.sleep(1)

def run_processor(stop, queue, cfg, flags=None, refresh_event=None):
    """
    Runner for the message processor. Uses the legacy SA implementation.
    """
    global q, config, stop_event, refresh_dicts_event
    global db_name, item_ids, system_ids, station_ids
    global update_busy, dump_busy, live_busy, process_ack, live_ack
    global dataPath
    q = queue
    config = cfg
    refresh_dicts_event = refresh_event
    stop_event = stop
    if flags is not None:
        update_busy = flags.get("update_busy", update_busy)
        dump_busy = flags.get("dump_busy", dump_busy)
        live_busy = flags.get("live_busy", live_busy)
        process_ack = flags.get("process_ack", process_ack)
        live_ack = flags.get("live_ack", live_ack)
    if multiprocessing.current_process().name != "MainProcess":
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    init_db(cfg)
    print(f"[Processor] PID {os.getpid()} starting")
    
    # Each process has its own globals; rebuild lookup dicts locally.
    dataPath = os.environ.get('TD_CSV') or Path(tradeenv.TradeEnv().csvDir).resolve()
    try:
        db_name, item_ids, system_ids, station_ids = update_dicts()
        print(
            f"[Processor] dicts loaded: "
            f"items={len(item_ids)} systems={len(system_ids)} stations={len(station_ids)}"
        )
    except Exception as e:
        print(f"[Processor] ERROR: update_dicts failed: {e}")
        return
    
    process_messages_sa()
    print(f"[Processor] PID {os.getpid()} exiting")

def process_messages_sa():
    """
    Consume MarketPrice entries from the global deque `q` and write Station/StationItem updates
    via SQLAlchemy.
    
    MP / concurrency posture (critical behaviour):
      - NEVER drop a live update just because the station lock is busy.
      - Coalesce pending updates by station_id: keep only the newest update per station.
      - On station-lock contention / deadlock / lock-timeout, defer and retry later with backoff.
      - Continue to respect busy/ack + periodic maintenance behaviour from live listener.
    """
    
    global q, process_ack, update_busy, dump_busy, live_busy
    global config, db_name, item_ids, system_ids, station_ids
    
    import heapq
    import random
    import re
    import traceback
    from collections import Counter, deque
    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError
    
    try:
        from tradedangerous.db.locks import station_advisory_lock, station_lock_key
    except ImportError:
        # repo-local layout fallback (shouldn't be needed on server installs)
        from db.locks import station_advisory_lock, station_lock_key
    
    # --- SQL ---------------------------------------------------------------
    
    DELETE_STATION_ITEMS = text("DELETE FROM StationItem WHERE station_id = :station_id")
    
    INSERT_STATION_ITEM = text(
        "INSERT INTO StationItem ("
        " station_id, item_id, modified,"
        " demand_price, demand_units, demand_level,"
        " supply_price, supply_units, supply_level, from_live)"
        " VALUES (:station_id, :item_id, :modified,"
        " :demand_price, :demand_units, :demand_level,"
        " :supply_price, :supply_units, :supply_level, 1)"
    )
    
    UPDATE_ITEM_AVG = text("UPDATE Item SET avg_price = :avg_price WHERE item_id = :item_id")
    
    GET_STATION_BY_ID = text("SELECT station_id FROM Station WHERE station_id = :station_id")
    
    GET_OLD_STATION_INFO = text(
        "SELECT name, ls_from_star, blackmarket, max_pad_size, "
        "       market, shipyard, outfitting, rearm, refuel, repair, "
        "       planetary, type_id, system_id "
        "FROM Station WHERE station_id = :sid"
    )
    
    INSERT_NEW_STATION = text(
        "INSERT INTO Station ("
        " station_id, name, system_id, ls_from_star,"
        " blackmarket, max_pad_size, market, shipyard,"
        " modified, outfitting, rearm, refuel, repair,"
        " planetary, type_id)"
        " VALUES (:station_id, :name, :system_id, :ls_from_star,"
        " :blackmarket, :max_pad_size, :market, :shipyard,"
        " :modified, :outfitting, :rearm, :refuel, :repair,"
        " :planetary, :type_id)"
    )
    
    DELETE_STATION = text("DELETE FROM Station WHERE station_id = :sid")
    MOVE_STATION = text("UPDATE Station SET system_id = :system_id, name = :name WHERE station_id = :sid")
    MOVE_STATION_AND_ID = text(
        "UPDATE Station SET station_id = :new_sid, system_id = :system_id, name = :name WHERE station_id = :sid"
    )
    GET_SYSTEM_ID_BY_NAME = text("SELECT system_id FROM System WHERE UPPER(name) = :n")
    
    # --- Diagnostics -----------------------------------
    
    diag_enabled = bool(config.get("listener_diag") or config.get("diagnostics") or config.get("debug"))
    diag_stats_every = float(config.get("listener_diag_stats_every", 30.0) or 30.0)
    diag_path = None
    if diag_enabled:
        try:
            diag_path = Path(
                config.get("listener_diag_path")
                or (Path(config["export_path"]).resolve() / "listener_diag.jsonl")
            )
            diag_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            diag_enabled = False
            diag_path = None
    
    def _diag_write(payload: dict) -> None:
        if not diag_enabled or diag_path is None:
            return
        try:
            payload = dict(payload)
            payload.setdefault("ts_epoch", time.time())
            payload.setdefault("ts_utc", datetime.utcnow().isoformat(timespec="milliseconds") + "Z")
            with open(diag_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, default=str, ensure_ascii=False) + "\n")
        except Exception:
            # Diagnostics must never break the listener.
            pass
    
    def _diag_mysql_probe(station_id: int) -> dict:
        """
        Best-effort probe of advisory lock ownership and spansh-import lock state.
        NEVER raise.
        """
        out = {}
        try:
            with sa_session() as s:
                dialect = (s.get_bind().dialect.name or "").lower()
                out["dialect"] = dialect
                if dialect not in ("mysql", "mariadb"):
                    return out
                
                # Keep probes fast if we accidentally touch locks.
                try:
                    s.execute(text("SET SESSION innodb_lock_wait_timeout = 1"))
                except Exception:
                    pass
                
                try:
                    out["conn_id"] = int(s.execute(text("SELECT CONNECTION_ID()")).scalar() or 0)
                except Exception:
                    pass
                
                lk = station_lock_key(int(station_id))
                out["station_lock_key"] = lk
                try:
                    out["station_lock_used_by_conn_id"] = s.execute(
                        text("SELECT IS_USED_LOCK(:k)"), {"k": lk}
                    ).scalar()
                except Exception:
                    pass
                
                try:
                    # 1 if acquired, 0 if timeout, NULL on error
                    out["spansh_import_lock_used_by_conn_id"] = s.execute(
                        text("SELECT IS_USED_LOCK('td.spansh.import')")
                    ).scalar()
                except Exception:
                    pass
                
                try:
                    out["spansh_import_in_progress"] = bool(
                        s.execute(text("SELECT IS_FREE_LOCK('td.spansh.import')")).scalar() == 0
                    )
                except Exception:
                    pass
        
        except Exception:
            pass
        return out
    
    def _diag_duplicate_key_from_exc(e: BaseException) -> dict:
        """
        Parse MariaDB/MySQL duplicate-entry message, if present.
        Returns dict possibly containing: dup_key, dup_station_id, dup_item_id
        """
        out = {}
        msg = ""
        try:
            orig = getattr(e, "orig", None)
            msg = str(orig) if orig is not None else str(e)
        except Exception:
            msg = str(e)
        
        m = re.search(r"Duplicate entry '([^']+)'", msg)
        if not m:
            return out
        
        key = m.group(1)
        out["dup_key"] = key
        
        # Expected format in this project: "<station_id>-<item_id>"
        if "-" in key:
            left, right = key.split("-", 1)
            try:
                out["dup_station_id"] = int(left)
            except Exception:
                pass
            try:
                out["dup_item_id"] = int(right)
            except Exception:
                pass
        
        return out
    
    # --- Helpers -----------------------------------------------------------
    
    def _mysql_errno(exc: BaseException):
        orig = getattr(exc, "orig", None)
        args = getattr(orig, "args", None) if orig is not None else None
        if not args:
            return None
        try:
            return int(args[0])
        except Exception:
            return None
    
    def _spansh_import_in_progress(sess) -> bool:
        """
        True if the global Spansh-import advisory lock is currently HELD.
        This is a no-op unless spansh actually takes that lock.
        """
        try:
            dialect = (sess.get_bind().dialect.name or "").lower()
        except Exception:
            dialect = ""
        if dialect not in ("mysql", "mariadb"):
            return False
        row = sess.execute(text("SELECT IS_FREE_LOCK('td.spansh.import')")).first()
        return bool(row and row[0] == 0)
    
    def _parse_modified(ts: str) -> datetime:
        # EDDN timestamps can be ISO-ish with Z, T, and/or fractional seconds.
        ts = (ts or "").replace("T", " ").replace("Z", "").replace("+00:00", "")
        try:
            return datetime.fromisoformat(ts)
        except Exception:
            if "." in ts:
                ts = ts.split(".", 1)[0]
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    
    def resolve_system_id(sess, system_name_upper: str):
        sid = system_ids.get(system_name_upper)
        if sid:
            return sid
        row = sess.execute(GET_SYSTEM_ID_BY_NAME, {"n": system_name_upper}).first()
        return int(row[0]) if row else None
    
    # Existing DB retry knobs (deadlock/lock-timeout)
    max_retries = int(config.get("listener_db_max_retries", 5) or 5)
    backoff_min = float(config.get("listener_db_backoff_min", 0.05) or 0.05)
    backoff_cap = float(config.get("listener_db_backoff_cap", 0.25) or 0.25)
    
    # Station-lock defer posture (separate from DB deadlock knobs)
    lock_backoff_min = float(config.get("listener_station_lock_backoff_min", 0.25) or 0.25)
    lock_backoff_cap = float(config.get("listener_station_lock_backoff_cap", 5.0) or 5.0)
    
    # How aggressively we drain `q` into the coalescer each tick
    drain_cap = int(config.get("listener_coalesce_drain_cap", 5000) or 5000)
    
    # --- Coalesce + defer state -------------------------------------------
    
    # pending_by_station: station_id -> staged dict (latest wins)
    pending_by_station: dict[int, dict] = {}
    
    # ready queue of station_ids to attempt now
    ready_ids: deque[int] = deque()
    ready_set: set[int] = set()
    
    # deferred schedule (min-heap of (ready_at_epoch, station_id))
    deferred_heap: list[tuple[float, int]] = []
    deferred_at: dict[int, float] = {}
    
    # attempt counters per station (for exponential backoff)
    station_attempts: dict[int, int] = {}
    
    # periodic stats
    next_diag_stats_at = time.time() + diag_stats_every
    next_rate_at = time.time() + 60.0
    processed_since_rate = 0
    
    def _enqueue_ready(station_id: int) -> None:
        if station_id in pending_by_station and station_id not in deferred_at and station_id not in ready_set:
            ready_ids.append(station_id)
            ready_set.add(station_id)
    
    def _defer_station(station_id: int, reason: str, err: BaseException | None = None) -> None:
        """
        Defer retry of this station, keeping only latest pending update (coalesced).
        """
        if station_id not in pending_by_station:
            station_attempts.pop(station_id, None)
            deferred_at.pop(station_id, None)
            return
        
        # Increment per-station attempts (bounded exponent to avoid silly sleep times)
        a = station_attempts.get(station_id, 0) + 1
        station_attempts[station_id] = a
        exp = min(a, 12)
        
        if reason == "lock":
            base = lock_backoff_min
            cap = lock_backoff_cap
        else:
            base = backoff_min
            cap = backoff_cap
        
        delay = min(cap, base * (2 ** (exp - 1))) + random.uniform(0.0, 0.02)
        ready_at = time.time() + delay
        
        # Cancel any existing ready-queue presence
        if station_id in ready_set:
            # can't efficiently remove from deque; just mark absent and skip on pop
            ready_set.discard(station_id)
        
        deferred_at[station_id] = ready_at
        heapq.heappush(deferred_heap, (ready_at, station_id))
        
        # Emit diagnostics for every deferral (this is the point of Step 1)
        errno = None
        err_s = None
        if err is not None:
            try:
                errno = _mysql_errno(err)
            except Exception:
                errno = None
            try:
                err_s = repr(err)
            except Exception:
                err_s = None
        
        staged = pending_by_station.get(station_id) or {}
        _diag_write({
            "event": "defer",
            "reason": reason,
            "station_id": int(station_id),
            "attempt": int(a),
            "delay_s": float(delay),
            "errno": errno,
            "error": err_s,
            "system": staged.get("system"),
            "station": staged.get("station"),
            "modified_dt": staged.get("modified_dt"),
            "commodities_total": staged.get("commodities_total"),
            "rows_built": staged.get("rows_built"),
        })
        
        if config.get("debug"):
            msg = f"Deferred {station_id} ({reason}) attempt={a} delay={delay:.2f}s"
            if err is not None and reason != "lock":
                msg += f" err={err!r}"
            print(msg)
    
    def _stage_entry(entry) -> dict | None:
        """
        Convert a MarketPrice entry into a staged dict suitable for DB write.
        Returns None if the entry should be ignored.
        """
        if isinstance(entry, dict):
            getter = entry.get
        else:
            getter = lambda k, default=None: getattr(entry, k, default)
        
        system = (getter("system", "") or "").upper()
        station = (getter("station", "") or "").upper()
        station_id = getter("market_id", None)
        if station_id is None:
            return None
        station_id = int(station_id)
        
        ts_raw = getter("timestamp", "") or ""
        try:
            modified_dt = _parse_modified(ts_raw)
        except Exception:
            _diag_write({
                "event": "stage_drop",
                "reason": "bad_timestamp",
                "station_id": int(station_id),
                "system": system,
                "station": station,
                "timestamp_raw": ts_raw,
            })
            return None
        
        commodities = getter("commodities", None) or []
        if not isinstance(commodities, list) or not commodities:
            _diag_write({
                "event": "stage_drop",
                "reason": "no_commodities",
                "station_id": int(station_id),
                "system": system,
                "station": station,
                "timestamp_raw": ts_raw,
            })
            return None
        
        if config.get("debug"):
            print(f'Processing: {system}/{station} timestamp:{modified_dt}')
        
        # Build StationItem rows and avg_price updates
        item_rows = []
        avg_by_item = {}
        
        # Heavy instrumentation support: trace how commodities map to item_id
        item_sources_by_id: dict[int, list[dict]] = {}
        ignored_zero_price = 0
        ignored_unknown_item = 0
        unknown_items_sample: list[str] = []
        
        for c in commodities:
            try:
                # Live behaviour: ignore entries where both prices are zero
                if (c.get('sellPrice') or 0) == 0 and (c.get('buyPrice') or 0) == 0:
                    ignored_zero_price += 1
                    continue
                
                name = (c.get('name') or "")
                item_edid = db_name.get(name.lower())
                if not item_edid:
                    ignored_unknown_item += 1
                    if len(unknown_items_sample) < 12 and name:
                        unknown_items_sample.append(name)
                    if config.get('debug'):
                        print(f"Ignoring item: {name}")
                    continue
                
                item_id = item_ids.get(item_edid) or int(item_edid)
                item_id_i = int(item_id)
                
                dem_br = c.get('demandBracket')
                stk_br = c.get('stockBracket')
                
                # Some clients can emit duplicate commodity rows where both brackets are blank/0.
                # Treat these as non-market (cargo-ish) and ignore them.
                if dem_br in ("", None, 0, "0") and stk_br in ("", None, 0, "0"):
                    continue
                
                row = {
                    "station_id": station_id,
                    "item_id": item_id_i,
                    "modified": modified_dt,
                    "demand_price": int(c.get('sellPrice') or 0),
                    "demand_units": int(c.get('demand') or 0),
                    "demand_level": (-1 if dem_br in ("", None) else int(dem_br)),
                    "supply_price": int(c.get('buyPrice') or 0),
                    "supply_units": int(c.get('stock') or 0),
                    "supply_level": (-1 if stk_br in ("", None) else int(stk_br)),
                }
                item_rows.append(row)
                
                item_sources_by_id.setdefault(item_id_i, []).append({
                    "name": name,
                    "sellPrice": c.get("sellPrice"),
                    "buyPrice": c.get("buyPrice"),
                    "demand": c.get("demand"),
                    "stock": c.get("stock"),
                    "demandBracket": c.get("demandBracket"),
                    "stockBracket": c.get("stockBracket"),
                    "meanPrice": c.get("meanPrice"),
                })
                
                mp = c.get('meanPrice')
                if mp is not None:
                    mp_i = int(mp)
                    if mp_i > 0:
                        avg_by_item[item_id_i] = mp_i
            except Exception:
                continue
        
        if not item_rows:
            _diag_write({
                "event": "stage_drop",
                "reason": "no_rows_built",
                "station_id": int(station_id),
                "system": system,
                "station": station,
                "timestamp_raw": ts_raw,
                "commodities_total": len(commodities),
                "ignored_zero_price": ignored_zero_price,
                "ignored_unknown_item": ignored_unknown_item,
                "unknown_items_sample": unknown_items_sample,
            })
            return None
        
        # Stable ordering (reduces deadlock chance)
        item_rows.sort(key=lambda r: r["item_id"])
        avg_rows = [{"item_id": iid, "avg_price": avg_by_item[iid]} for iid in sorted(avg_by_item.keys())]
        
        # Precompute dup info for later error-path logging
        counts = Counter([r["item_id"] for r in item_rows])
        dup_item_ids = [iid for iid, cnt in counts.items() if cnt > 1]
        
        staged = {
            "station_id": station_id,
            "system": system,
            "station": station,
            "modified_dt": modified_dt,
            "timestamp_raw": ts_raw,
            "item_rows": item_rows,
            "avg_rows": avg_rows,
            "entry": entry,
            "from_megaship": f"MEGASHIP/{station}",
            "commodities_total": len(commodities),
            "rows_built": len(item_rows),
            "ignored_zero_price": ignored_zero_price,
            "ignored_unknown_item": ignored_unknown_item,
            "unknown_items_sample": unknown_items_sample,
            "item_sources_by_id": item_sources_by_id,
            "dup_item_ids_in_batch": dup_item_ids,
        }
        
        if dup_item_ids:
            _diag_write({
                "event": "stage_warn",
                "reason": "duplicate_item_ids_in_batch",
                "station_id": int(station_id),
                "system": system,
                "station": station,
                "modified_dt": modified_dt,
                "dup_item_ids": dup_item_ids[:200],
                "dup_item_ids_count": len(dup_item_ids),
                "commodities_total": len(commodities),
                "rows_built": len(item_rows),
            })
        
        return staged
    
    def _drain_queue_into_coalescer() -> None:
        force_every_n = int(os.environ.get("TDL_TEST_FORCE_DEFER_EVERY_N", "0") or 0)
        force_counter = (
            getattr(_drain_queue_into_coalescer, "_force_defer_counter", 0)
            if force_every_n > 0
            else 0
        )
        
        import queue as pyqueue
        
        drained = 0
        while drained < drain_cap and (stop is None or not stop.is_set()):
            try:
                if hasattr(q, "popleft"):
                    entry = q.popleft()
                else:
                    entry = q.get_nowait()
            except (IndexError, pyqueue.Empty):
                break
            drained += 1
            
            staged = _stage_entry(entry)
            if not staged:
                continue
            
            sid = staged["station_id"]
            prev = pending_by_station.get(sid)
            
            # Coalesce: keep newest (by modified_dt)
            if prev is None or staged["modified_dt"] >= prev["modified_dt"]:
                # Preserve pending force-defer marker if a newer stage replaces an older one
                if prev is not None and prev.get("force_defer_once"):
                    staged["force_defer_once"] = True
                
                if force_every_n > 0:
                    force_counter += 1
                    if (force_counter % force_every_n) == 0:
                        staged["force_defer_once"] = True
                    setattr(_drain_queue_into_coalescer, "_force_defer_counter", force_counter)
                
                pending_by_station[sid] = staged
                
                # IMPORTANT:
                # If this station is currently deferred (e.g. Spansh holds the station lock),
                # do NOT reset attempts — that causes retry-thrash on busy stations.
                # Only reset attempts when we're not already in a deferred posture.
                if sid not in deferred_at:
                    station_attempts.pop(sid, None)
            
            _enqueue_ready(sid)
    
    def _release_due_deferrals() -> None:
        now = time.time()
        while deferred_heap and deferred_heap[0][0] <= now:
            ready_at, sid = heapq.heappop(deferred_heap)
            if deferred_at.get(sid) != ready_at:
                continue
            deferred_at.pop(sid, None)
            _enqueue_ready(sid)
    
    # --- Main loop ---------------------------------------------------------
    
    rate_every = float(config.get("listener_processor_rate_every", 60.0) or 60.0)
    rate_window_start = time.time()
    rate_count = 0
    next_rate_at = rate_window_start + rate_every
    
    stop = globals().get("stop_event")
    while (stop is None or not stop.is_set()):
        # Periodic stats snapshot (helps spot “stuck” stations)
        if diag_enabled and time.time() >= next_diag_stats_at:
            next_diag_stats_at = time.time() + diag_stats_every
            _diag_write({
                "event": "loop_stats",
                "pending_stations": len(pending_by_station),
                "ready_len": len(ready_ids),
                "deferred_len": len(deferred_heap),
                "deferred_map_len": len(deferred_at),
                "attempts_tracked": len(station_attempts),
            })
        
        # Respect busy flags (threaded parity)
        def _flag_is_set(flag) -> bool:
            try:
                return bool(flag.is_set())
            except Exception:
                return bool(flag)
        
        def _ack_is_set() -> bool:
            return _flag_is_set(process_ack)
        
        def _ack_set(val: bool) -> None:
            global process_ack
            if hasattr(process_ack, "set") and hasattr(process_ack, "clear"):
                if val:
                    process_ack.set()
                else:
                    process_ack.clear()
            else:
                process_ack = bool(val)
        
        def _busy_is_set() -> bool:
            return (
                _flag_is_set(update_busy)
                or _flag_is_set(dump_busy)
            )
        
        if _busy_is_set():
            if not _ack_is_set() and (config.get("verbose") or config.get("debug")):
                print("Message processor acknowledging busy signal.")
            _ack_set(True)
            while _busy_is_set() and (stop is None or not stop.is_set()):
                if stop is not None:
                    stop.wait(1)
                else:
                    time.sleep(1)
            if stop is not None and stop.is_set():
                break
            _ack_set(False)
            if config.get("verbose") or config.get("debug"):
                print("Busy signal off, message processor resuming.")
        
        # Dict refresh signalling (post-Spansh)
        refresh = globals().get("refresh_dicts_event")
        if refresh is not None and refresh.is_set():
            quick_test = os.environ.get("TDL_REFRESH_QUICK_TEST") == "1"
            try:
                refresh.clear()
            except Exception:
                pass
            try:
                new_db_name, new_item_ids, new_system_ids, new_station_ids = update_dicts()
                db_name = new_db_name
                item_ids = new_item_ids
                system_ids = new_system_ids
                station_ids = new_station_ids
                print(
                    f"[Processor] dicts refreshed: "
                    f"items={len(item_ids)} systems={len(system_ids)} stations={len(station_ids)}"
                )
            except Exception as e:
                print(f"[Processor] ERROR: dict refresh failed: {e!r}")
                if not quick_test:
                    try:
                        refresh.set()
                    except Exception:
                        pass
            if quick_test:
                print("[Processor] Quick-test complete; requesting shutdown.")
                try:
                    if stop_event is not None:
                        stop_event.set()
                except Exception:
                    pass
        
        # Pull new listener entries into the coalescer, then release due retries.
        _drain_queue_into_coalescer()
        _release_due_deferrals()
        
        # Rate-limited throughput/queue diagnostics (minimal; helps debug stalls)
        
        # Rate-limited throughput/queue diagnostics (minimal; helps debug stalls)
        if time.time() >= next_rate_at:
            now = time.time()
            elapsed = max(0.001, now - rate_window_start)
            per_min = (rate_count / elapsed) * 60.0
            
            qdepth = None
            try:
                if hasattr(q, "qsize"):
                    qdepth = q.qsize()
                elif hasattr(q, "__len__"):
                    qdepth = len(q)
            except Exception:
                qdepth = None
            
            if diag_enabled:
                _diag_write({
                    "event": "throughput",
                    "stations_attempted": int(rate_count),
                    "per_min": float(per_min),
                    "queue_depth": qdepth,
                    "pending_stations": len(pending_by_station),
                    "ready_len": len(ready_ids),
                    "deferred_len": len(deferred_heap),
                })
            
            if config.get("verbose") or config.get("debug"):
                print(
                    f"[Processor] rate={per_min:.1f}/min "
                    f"attempted={rate_count} qdepth={qdepth} "
                    f"pending={len(pending_by_station)} ready={len(ready_ids)} deferred={len(deferred_heap)}"
                )
            
            rate_window_start = now
            rate_count = 0
            next_rate_at = now + rate_every
        
        # Nothing ready? sleep until next deferral or a short poll.
        if not ready_ids:
            if deferred_heap:
                wait = max(0.05, deferred_heap[0][0] - time.time())
                if stop is not None:
                    stop.wait(min(1.0, wait))
                else:
                    time.sleep(min(1.0, wait))
            else:
                if stop is not None:
                    stop.wait(0.5)
                else:
                    time.sleep(0.5)
            continue
        
        # Pop next station_id (skip stale entries removed from ready_set)
        sid = ready_ids.popleft()
        if sid not in ready_set:
            continue
        ready_set.discard(sid)
        rate_count += 1
        
        staged = pending_by_station.get(sid)
        if not staged:
            continue
        
        system = staged["system"]
        station = staged["station"]
        station_id = staged["station_id"]
        modified_dt = staged["modified_dt"]
        item_rows = staged["item_rows"]
        avg_rows = staged["avg_rows"]
        entry = staged["entry"]
        from_megaship = staged["from_megaship"]
        
        attempt_no = int(station_attempts.get(station_id, 0) + 1)
        _diag_write({
            "event": "attempt_start",
            "station_id": int(station_id),
            "attempt": attempt_no,
            "system": system,
            "station": station,
            "modified_dt": modified_dt,
            "commodities_total": staged.get("commodities_total"),
            "rows_built": staged.get("rows_built"),
            "dup_item_ids_in_batch": (staged.get("dup_item_ids_in_batch") or [])[:200],
        })
        
        try:
            discard = False
            with station_advisory_lock(
                _ENGINE,
                int(station_id),
                timeout_seconds=float(os.environ.get("TDL_LISTENER_LOCK_TIMEOUT_SECONDS", "0.2") or 0.2),
                max_retries=int(os.environ.get("TDL_LISTENER_LOCK_MAX_RETRIES", "4") or 4),
                backoff_start_seconds=float(os.environ.get("TDL_LISTENER_LOCK_BACKOFF_START_SECONDS", "0.05") or 0.05),
            ) as s:
                if staged.pop("force_defer_once", None):
                    _diag_write({
                        "event": "lock_busy",
                        "station_id": int(station_id),
                        "attempt": attempt_no,
                        "system": system,
                        "station": station,
                        "modified_dt": modified_dt,
                        "probe": {"forced": True},
                    })
                    raise RuntimeError("station lock busy")
                
                if s is None:
                    # Self-identifying lock contention
                    probe = _diag_mysql_probe(int(station_id))
                    _diag_write({
                        "event": "lock_busy",
                        "station_id": int(station_id),
                        "attempt": attempt_no,
                        "system": system,
                        "station": station,
                        "modified_dt": modified_dt,
                        "probe": probe,
                    })
                    raise RuntimeError("station lock busy")
                
                # Ensure station exists or can be added, but only if we know system_id
                exists = s.execute(GET_STATION_BY_ID, {"station_id": int(station_id)}).first()
                if not exists:
                    sys_id = resolve_system_id(s, system)
                    if sys_id is None:
                        print(
                            f"WARNING: system not found for station insert; skipping: "
                            f"{system}/{station} (market_id={station_id})"
                        )
                        discard = True
                    else:
                        maybe_old = station_ids.get(from_megaship)
                        if maybe_old:
                            if config.get('verbose'):
                                print(f'Megaship station, updating system to {system}')
                            if int(maybe_old) != int(station_id):
                                s.execute(
                                    MOVE_STATION_AND_ID,
                                    {
                                        "new_sid": int(station_id),
                                        "system_id": int(sys_id),
                                        "name": station,
                                        "sid": int(maybe_old),
                                    },
                                )
                                station_ids[from_megaship] = int(station_id)
                                station_ids[f'{system}/{station}'] = int(station_id)
                            else:
                                s.execute(
                                    MOVE_STATION,
                                    {"system_id": int(sys_id), "name": station, "sid": int(maybe_old)}
                                )
                                station_ids[f'{system}/{station}'] = int(station_id)
                        else:
                            if config.get('verbose'):
                                print(f'Not found in Stations: {system}/{station}, inserting into DB.')
                            s.execute(INSERT_NEW_STATION, {
                                "station_id": int(station_id),
                                "name": station,
                                "system_id": int(sys_id),
                                "ls_from_star": 999999,
                                "blackmarket": '?',
                                "max_pad_size": '?',
                                "market": 'Y',
                                "shipyard": '?',
                                "modified": modified_dt,
                                "outfitting": '?',
                                "rearm": '?',
                                "refuel": '?',
                                "repair": '?',
                                "planetary": '?',
                                "type_id": 0,
                            })
                            station_ids[f'{system}/{station}'] = int(station_id)
                
                if not discard:
                    # Migrate old station_id mapping if needed (legacy parity)
                    old_sid = station_ids.get(f'{system}/{station}')
                    if old_sid and int(old_sid) != int(station_id):
                        res = s.execute(GET_OLD_STATION_INFO, {"sid": int(old_sid)}).first()
                        if res:
                            (nm, ls, bm, mps, mk, sy, of, ra, rf, rp, pl, ti, old_sys_id) = res
                            s.execute(DELETE_STATION, {"sid": int(old_sid)})
                            s.execute(INSERT_NEW_STATION, {
                                "station_id": int(station_id),
                                "name": nm,
                                "system_id": int(old_sys_id),
                                "ls_from_star": ls,
                                "blackmarket": bm,
                                "max_pad_size": mps,
                                "market": mk,
                                "shipyard": sy,
                                "modified": modified_dt,
                                "outfitting": of,
                                "rearm": ra,
                                "refuel": rf,
                                "repair": rp,
                                "planetary": pl,
                                "type_id": ti,
                            })
                            station_ids[f'{system}/{station}'] = int(station_id)
                    
                    # Deadlock posture: update Items first (stable order), then StationItem
                    if avg_rows and not _spansh_import_in_progress(s):
                        s.execute(UPDATE_ITEM_AVG, avg_rows)
                    
                    # Snapshot semantics (as live): replace station market rows
                    s.execute(DELETE_STATION_ITEMS, {"station_id": int(station_id)})
                    s.execute(INSERT_STATION_ITEM, item_rows)
                    # station_advisory_lock commits on normal exit before releasing lock
            
            if discard:
                # Permanent skip (unknown system). Remove pending so we don't retry forever.
                _diag_write({
                    "event": "discard",
                    "reason": "unknown_system",
                    "station_id": int(station_id),
                    "attempt": attempt_no,
                    "system": system,
                    "station": station,
                    "modified_dt": modified_dt,
                })
                pending_by_station.pop(station_id, None)
                station_attempts.pop(station_id, None)
                deferred_at.pop(station_id, None)
                continue
            
            # Success: clear pending + attempts
            pending_by_station.pop(station_id, None)
            station_attempts.pop(station_id, None)
            deferred_at.pop(station_id, None)
            
            processed_since_rate += 1
            
            _diag_write({
                "event": "success",
                "station_id": int(station_id),
                "system": system,
                "station": station,
                "modified_dt": modified_dt,
                "rows_built": staged.get("rows_built"),
            })
            
            if config.get('verbose'):
                if isinstance(entry, dict):
                    software = entry.get("software") or ""
                    version = entry.get("version") or ""
                    msg = (
                        f"Updated {system}/{station}, station_id:'{station_id}', "
                        f"from {software} v{version}"
                    )
                    print(msg)
                else:
                    msg = (
                        f"Updated {entry.system}/{entry.station}, station_id:'{station_id}', "
                        f"from {entry.software} v{entry.version}"
                    )
                    print(msg)
            else:
                print(f'Updated {system}/{station}')
        
        except DBAPIError as e:
            errno = _mysql_errno(e)
            dup_info = {}
            if errno == 1062:
                dup_info = _diag_duplicate_key_from_exc(e)
                
                # Race-safe Station insert handling:
                # If another worker inserted the Station row after our existence check,
                # we'll see a duplicate PRIMARY KEY (dup_key == station_id) here.
                # In that case, re-queue immediately without backoff/defer.
                if (
                    dup_info.get("dup_key") == str(station_id)
                    and dup_info.get("dup_item_id") is None
                ):
                    try:
                        with sa_session() as s2:
                            exists_now = s2.execute(
                                GET_STATION_BY_ID, {"station_id": int(station_id)}
                            ).first()
                        if exists_now:
                            _diag_write({
                                "event": "dup_station_race",
                                "station_id": int(station_id),
                                "attempt": attempt_no,
                                "system": system,
                                "station": station,
                                "modified_dt": modified_dt,
                                "errno": errno,
                                "dup_info": dup_info,
                            })
                            station_attempts.pop(station_id, None)
                            deferred_at.pop(station_id, None)
                            _enqueue_ready(station_id)
                            continue
                    except Exception:
                        # Fall through to normal error handling
                        pass
            
            # Always emit a structured error event
            _diag_write({
                "event": "db_error",
                "station_id": int(station_id),
                "attempt": attempt_no,
                "system": system,
                "station": station,
                "modified_dt": modified_dt,
                "errno": errno,
                "error": repr(e),
                "dup_info": dup_info,
                "dup_item_ids_in_batch": (staged.get("dup_item_ids_in_batch") or [])[:500],
                "commodities_total": staged.get("commodities_total"),
                "rows_built": staged.get("rows_built"),
            })
            
            if errno == 1062:
                # Deep dump: show duplicated item_ids in the staged batch and their sources
                dup_ids = staged.get("dup_item_ids_in_batch") or []
                src = staged.get("item_sources_by_id") or {}
                dump = {}
                for iid in dup_ids[:200]:
                    try:
                        dump[str(iid)] = src.get(iid, [])[:50]
                    except Exception:
                        continue
                
                probe = _diag_mysql_probe(int(station_id))
                _diag_write({
                    "event": "db_error_detail_1062",
                    "station_id": int(station_id),
                    "attempt": attempt_no,
                    "system": system,
                    "station": station,
                    "modified_dt": modified_dt,
                    "dup_info": dup_info,
                    "dup_sources_by_item_id": dump,
                    "probe": probe,
                    "traceback": traceback.format_exc(limit=50),
                })
            
            if errno in (1213, 1205):
                _defer_station(station_id, "deadlock", e)
                continue
            
            print(f"ERROR processing {system}/{station} (market_id={station_id}): {e!r}")
            _defer_station(station_id, "db", e)
            continue
        
        except Exception as e:
            if "station lock busy" in str(e):
                _defer_station(station_id, "lock", e)
                continue
            
            # Catch-all error diagnostics
            _diag_write({
                "event": "error",
                "station_id": int(station_id),
                "attempt": attempt_no,
                "system": system,
                "station": station,
                "modified_dt": modified_dt,
                "error": repr(e),
                "traceback": traceback.format_exc(limit=50),
            })
            
            print(f"ERROR processing {system}/{station} (market_id={station_id}): {e!r}")
            _defer_station(station_id, "error", e)
            continue
    
    print("Message processor (SA) reporting shutdown.")

def run_live_exporter(stop, cfg, flags=None):
    """
    Emit listings-live.csv from StationItem WHERE from_live = 1.
    
    Live export is read-only and must not pause the DB processor.
    """
    if multiprocessing.current_process().name != "MainProcess":
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        init_db(cfg)
    except Exception as e:
        print(f"[Live] Exporter failed to initialise DB: {e}")
        return
    
    listings_file = (Path(cfg['export_path']).resolve() / Path("listings-live.csv"))
    listings_tmp = listings_file.with_suffix(".tmp")
    print(f"[Live] PID {os.getpid()} Starting")
    print(f"[Live] Live listings will be exported to:")
    print(f"[Live]         {listings_file}")
    
    now = time.time()
    while not stop.is_set():
        while time.time() < now + (cfg['export_live_every_x_min'] * _minute):
            if stop.is_set():
                break
            time.sleep(1)
        
        if stop.is_set():
            break
        
        start = datetime.now()
        print(f"[Live] Exporter starting export. {start}")
        
        SELECT_LIVE = text("""
            SELECT
                station_id,
                item_id,
                demand_price  AS sell_price,
                demand_units  AS demand,
                demand_level  AS demand_bracket,
                supply_price  AS buy_price,
                supply_units  AS supply,
                supply_level  AS supply_bracket,
                modified
            FROM StationItem
            WHERE from_live = 1
            ORDER BY station_id, item_id
""")
        
        try:
            with sa_session() as s:
                result = s.execute(SELECT_LIVE.execution_options(stream_results=True))
                with open(str(listings_tmp), "w") as f:
                    f.write("id,station_id,commodity_id,supply,supply_bracket,buy_price,sell_price,demand,demand_bracket,collected_at\n")
                    lineNo = 1
                    for row in result:
                        if stop.is_set():
                            break
                        station_id, commodity_id = str(row[0]), str(row[1])
                        sell_price, demand, demand_bracket = str(row[2]), str(row[3]), str(row[4])
                        buy_price, supply, supply_bracket = str(row[5]), str(row[6]), str(row[7])
                        ts = str(row[8]).split('.')[0]
                        collected_at = str(timegm(datetime.strptime(ts, '%Y-%m-%d %H:%M:%S').timetuple()))
                        listing = (f"{station_id},{commodity_id},{supply},{supply_bracket},{buy_price},{sell_price},"
                                   f"{demand},{demand_bracket},{collected_at}")
                        f.write(f"{lineNo},{listing}\n")
                        lineNo += 1
        except Exception as e:
            print(e)
            try:
                listings_tmp.unlink(missing_ok=True)
            except Exception:
                pass
            now = time.time()
            continue
        
        if stop.is_set():
            try:
                listings_tmp.unlink(missing_ok=True)
            except Exception:
                pass
            print("Export aborted, received shutdown signal.")
            break
        
        while listings_file.exists():
            try:
                listings_file.unlink()
            except:
                if stop is not None and hasattr(stop, "is_set") and stop.is_set():
                    break
                time.sleep(1)
        listings_tmp.rename(listings_file)
        print(f"[Live] Export completed in {datetime.now() - start}")
        now = time.time()
    
    try:
        listings_tmp.unlink(missing_ok=True)
    except Exception:
        pass
    print("[Live] Exporter reporting shutdown.")

def export_live_sa(stop, cfg, flags=None):
    run_live_exporter(stop, cfg, flags)

def run_dump_exporter(stop, cfg):
    """
    Emit listings.csv from all StationItem rows.
    
    Milestone 4: This exporter must NOT use busy/ack. Live ingestion/export
    continues concurrently with Spansh + dump export.
    """
    listings_file = (Path(cfg['export_path']).resolve() / Path("listings.csv"))
    listings_tmp = listings_file.with_suffix(".tmp")
    print(f'[Dump] Listings will be exported to: \n\t{listings_file}')
    
    start = datetime.now()
    
    SELECT_ALL = text("""
        SELECT
            station_id,
            item_id,
            demand_price  AS sell_price,
            demand_units  AS demand,
            demand_level  AS demand_bracket,
            supply_price  AS buy_price,
            supply_units  AS supply,
            supply_level  AS supply_bracket,
            modified
        FROM StationItem
        ORDER BY station_id, item_id
    """)
    
    try:
        with sa_session() as s:
            result = s.execute(SELECT_ALL.execution_options(stream_results=True))
            with open(str(listings_tmp), "w") as f:
                f.write("id,station_id,commodity_id,supply,supply_bracket,buy_price,sell_price,demand,demand_bracket,collected_at\n")
                lineNo = 1
                for row in result:
                    if stop is not None and hasattr(stop, "is_set") and stop.is_set():
                        break
                    station_id, commodity_id = str(row[0]), str(row[1])
                    sell_price, demand, demand_bracket = str(row[2]), str(row[3]), str(row[4])
                    buy_price, supply, supply_bracket = str(row[5]), str(row[6]), str(row[7])
                    ts = str(row[8]).split('.')[0]
                    collected_at = str(timegm(datetime.strptime(ts, '%Y-%m-%d %H:%M:%S').timetuple()))
                    listing = (f'{station_id},{commodity_id},{supply},{supply_bracket},{buy_price},{sell_price},'
                               f'{demand},{demand_bracket},{collected_at}')
                    f.write(f'{lineNo},{listing}\n'); lineNo += 1
    except Exception as e:
        print("[Dump] Aborting export:"); print(e)
        try:
            listings_tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return False
    
    if (stop is not None and hasattr(stop, "is_set") and stop.is_set()):
        listings_tmp.unlink(missing_ok=True)
        print("[Dump] Export aborted, received shutdown signal.")
        return False
    else:
        while listings_file.exists():
            try:
                listings_file.unlink()
            except:
                if stop is not None and hasattr(stop, "is_set") and stop.is_set():
                    break
                time.sleep(1)
        if stop is not None and hasattr(stop, "is_set") and stop.is_set():
            listings_tmp.unlink(missing_ok=True)
            print("[Dump] Export aborted, received shutdown signal.")
            return False
        listings_tmp.rename(listings_file)
        print(f'[Dump] Export completed in {datetime.now() - start}')
        return True

def update_dicts():
    # We'll use this to get the fdev_id from the 'symbol', AKA commodity['name'].lower()
    db_name = dict()
    edcd_source = 'https://raw.githubusercontent.com/EDCD/FDevIDs/master/commodity.csv'
    edcd_csv = request.urlopen(edcd_source)
    edcd_dict = csv.DictReader(codecs.iterdecode(edcd_csv, 'utf-8'))
    for line in iter(edcd_dict):
        db_name[line['symbol'].lower()] = line['id']
    
    # Rare items are in a different file.
    edcd_rare_source = 'https://raw.githubusercontent.com/EDCD/FDevIDs/master/rare_commodity.csv'
    edcd_rare_csv = request.urlopen(edcd_rare_source)
    edcd_rare_dict = csv.DictReader(codecs.iterdecode(edcd_rare_csv, 'utf-8'))
    for line in iter(edcd_rare_dict):
        db_name[line['symbol'].lower()] = line['id']
    
    # We'll use this to get the item_id from the fdev_id because it's faster than a database lookup.
    item_ids = dict()
    
    # Rare items don't have an EDDB item_id, so we'll just store them by the fdev_id
    for line in iter(edcd_rare_dict):
        item_ids[line['id']] = line['id']
    
    with open(str(dataPath / Path("Item.csv")), "r", encoding = "utf8") as fh:
        items = csv.DictReader(fh, quotechar = "'")
        # Older versions of TD don't have fdev_id as a unique key, newer versions do.
        if 'fdev_id' in next(iter(items)).keys():
            iid_key = 'fdev_id'
        else:
            iid_key = 'unq:fdev_id'
        fh.seek(0)
        next(iter(items))
        for item in items:
            item_ids[item[iid_key]] = int(item['unq:item_id'])
    
    # We're using these for the same reason.
    system_names = dict()
    system_ids = dict()
    with open(str(dataPath / Path("System.csv")), "r", encoding = "utf8") as fh:
        systems = csv.DictReader(fh, quotechar = "'")
        for system in systems:
            system_names[int(system['unq:system_id'])] = system['name'].upper()
            system_ids[system['name'].upper()] = int(system['unq:system_id'])
    station_ids = dict()
    megaship_types = [19, 24]
    with open(str(dataPath / Path("Station.csv")), "r", encoding = "utf8") as fh:
        stations = csv.DictReader(fh, quotechar = "'")
        for station in stations:
            # Mobile stations can move between systems. The mobile stations
            # have the following data in their entry in stations.jsonl:
            # "type_id":19,"type":"Megaship"
            # Except for that one Orbis station.
            # And now Fleet Carriers, they're type 24.
            if int(station['type_id']) in megaship_types or int(station['unq:station_id']) == 42041:
                full_name = "MEGASHIP"
            else:
                full_name = system_names[int(station['system_id@System.system_id'])]
            full_name += "/" + station['name'].upper()
            station_ids[full_name] = int(station['unq:station_id'])
    
    del system_names
    
    return db_name, item_ids, system_ids, station_ids

update_busy = False
process_ack = False
live_ack = False
live_busy = False
dump_busy = False
spansh_busy = False

refresh_dicts_event = None

q = deque()

import argparse

def bootstrap_runtime():
    """
    Recreate the legacy top-level initialization but *without* starting threads.
    Returns the thread objects. Safe to call only in normal threaded runs.
    """
    global dataPath, config, tdb, eddbPath, db_name, item_ids, system_ids, station_ids
    
    # Paths & config
    dataPath = os.environ.get('TD_CSV') or Path(tradeenv.TradeEnv().csvDir).resolve()
    config = load_config()
    ensure_db_maint_cnf_ready(config)
    init_db(config)
    
    summary = ensure_fresh_db(
        backend=_ENGINE.dialect.name,
        engine=_ENGINE,
        data_dir=dataPath,    # or _DATA_DIR if you've resolved it separately
        metadata=None,
        rebuild=False,
    )
    
    if config.get("debug"):
        print(
            f"DB bootstrap: action={summary['action']} "
            f"sane={summary['sane']} "
            f"reason={summary.get('reason','ok')} "
            f"backend={summary['backend']}"
        )
    
    if config['verbose']:
        print("Loading TradeDB")
    tdb = tradedb.TradeDB(load = False)
    
    validate_config()
    if config['verbose']:
        print("Config loaded")
    
    # Ensure export folder exists
    try:
        Path(config['export_path']).mkdir()
    except FileExistsError:
        pass
    
    if config['verbose']:
        print(f"[supervisor] PID {os.getpid()} starting")
        print("Initializing processes")
    
    global stop_event, q
    global update_busy, dump_busy, spansh_busy, live_busy, process_ack, live_ack, refresh_dicts_event
    ctx = multiprocessing.get_context("spawn")
    stop_event = ctx.Event()
    q = ctx.Queue()
    
    update_busy = ctx.Event()
    dump_busy = ctx.Event()
    spansh_busy = ctx.Event()
    live_busy = ctx.Event()
    process_ack = ctx.Event()
    live_ack = ctx.Event()
    refresh_dicts_event = ctx.Event()
    flags = {
        "update_busy": update_busy,
        "dump_busy": dump_busy,
        "spansh_busy": spansh_busy,
        "live_busy": live_busy,
        "process_ack": process_ack,
        "live_ack": live_ack,
    }
    
    listener_thread = ctx.Process(target=run_listener, args=(stop_event, q, config))
    process_thread  = ctx.Process(target=run_processor, args=(stop_event, q, config, flags, refresh_dicts_event))
    update_thread = ctx.Process(target=run_update, args=(stop_event, config, flags, refresh_dicts_event))
    live_thread = ctx.Process(target=run_live_exporter, args=(stop_event, config, flags))
    
    if config['verbose']:
        print("Updating dicts")
    try:
        db_name, item_ids, system_ids, station_ids = update_dicts()
    except Exception as e:
        print(str(e))
    
    if config['verbose']:
        print("Startup process completed.")
    
    return update_thread, listener_thread, process_thread, live_thread

def print_stationitem_stats():
    TOTAL = text("SELECT COUNT(*) FROM StationItem")
    LIVE  = text("SELECT COUNT(*) FROM StationItem WHERE from_live = 1")
    LATEST = text("SELECT MAX(modified) FROM StationItem")
    with sa_session() as s:
        total = s.execute(TOTAL).scalar_one()
        live  = s.execute(LIVE).scalar_one()
        latest = s.execute(LATEST).scalar_one()
    print(f"[stats] StationItem total={total} live={live} latest_modified={latest}")

def export_live_once_sa():
    listings_file = (Path(config['export_path']).resolve() / Path("listings-live.csv"))
    listings_tmp = listings_file.with_suffix(".tmp")
    # listings-live.csv
    SELECT_LIVE = text("""
        SELECT
            station_id,
            item_id,
            demand_price  AS sell_price,
            demand_units  AS demand,
            demand_level  AS demand_bracket,
            supply_price  AS buy_price,
            supply_units  AS supply,
            supply_level  AS supply_bracket,
            modified
        FROM StationItem
        WHERE from_live = 1
        ORDER BY station_id, item_id
    """)
    start = datetime.now()
    with sa_session() as s:
        result = s.execute(SELECT_LIVE.execution_options(stream_results=True))
        with open(str(listings_tmp), "w") as f:
            f.write("id,station_id,commodity_id,supply,supply_bracket,buy_price,sell_price,demand,demand_bracket,collected_at\n")
            lineNo = 1
            for row in result:
                station_id, commodity_id = str(row[0]), str(row[1])
                sell_price, demand, demand_bracket = str(row[2]), str(row[3]), str(row[4])
                buy_price, supply, supply_bracket = str(row[5]), str(row[6]), str(row[7])
                ts_val = row[8]
                ts_str = ts_val.strftime("%Y-%m-%d %H:%M:%S") if hasattr(ts_val, "strftime") else str(ts_val).split(".")[0]
                collected_at = str(timegm(datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").timetuple()))
                listing = (f"{station_id},{commodity_id},{supply},{supply_bracket},{buy_price},{sell_price},"
                           f"{demand},{demand_bracket},{collected_at}")
                f.write(f"{lineNo},{listing}\n"); lineNo += 1
    while listings_file.exists():
        try:
            listings_file.unlink()
        except:
            time.sleep(0.5)
    listings_tmp.rename(listings_file)
    print(f"[export-live-now] wrote {listings_file} in {datetime.now() - start}")

def export_dump_once_sa():
    # listings.csv (full dump)
    SELECT_ALL = text("""
        SELECT
            station_id,
            item_id,
            demand_price  AS sell_price,
            demand_units  AS demand,
            demand_level  AS demand_bracket,
            supply_price  AS buy_price,
            supply_units  AS supply,
            supply_level  AS supply_bracket,
            modified
        FROM StationItem
        ORDER BY station_id, item_id
    """)
    listings_file = (Path(config['export_path']).resolve() / Path("listings.csv"))
    listings_tmp = listings_file.with_suffix(".tmp")
    start = datetime.now()
    with sa_session() as s:
        result = s.execute(SELECT_ALL.execution_options(stream_results=True))
        with open(str(listings_tmp), "w") as f:
            f.write("id,station_id,commodity_id,supply,supply_bracket,buy_price,sell_price,demand,demand_bracket,collected_at\n")
            lineNo = 1
            for row in result:
                station_id, commodity_id = str(row[0]), str(row[1])
                sell_price, demand, demand_bracket = str(row[2]), str(row[3]), str(row[4])
                buy_price, supply, supply_bracket = str(row[5]), str(row[6]), str(row[7])
                ts_val = row[8]
                ts_str = ts_val.strftime("%Y-%m-%d %H:%M:%S") if hasattr(ts_val, "strftime") else str(ts_val).split(".")[0]
                collected_at = str(timegm(datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").timetuple()))
                listing = (f"{station_id},{commodity_id},{supply},{supply_bracket},{buy_price},{sell_price},"
                           f"{demand},{demand_bracket},{collected_at}")
                f.write(f"{lineNo},{listing}\n"); lineNo += 1
    while listings_file.exists():
        try:
            listings_file.unlink()
        except:
            time.sleep(0.5)
    listings_tmp.rename(listings_file)
    print(f"[export-dump-now] wrote {listings_file} in {datetime.now() - start}")

def purge_old_stationitems_sa(days: int = 30, batch_size: int = 200_000):
    """
    Delete StationItem rows older than `days`, in batches.
    NOTE: Purge only. No analyze/optimize/vacuum here.
    """
    if not isinstance(days, int) or days <= 0:
        return {"deleted": 0, "cutoff": None, "skipped": True}
    
    cutoff = datetime.utcnow() - timedelta(days=days)
    total = 0
    
    with sa_session() as s:
        lim = int(batch_size)
        if lim <= 0:
            raise ValueError("batch_size must be a positive integer")
        DELETE_BATCH = text(f"""
            DELETE FROM StationItem
            WHERE modified < :cutoff
            LIMIT {lim}
        """)
        while True:
            res = s.execute(DELETE_BATCH, {"cutoff": cutoff})
            s.commit()
            deleted = getattr(res, "rowcount", 0)
            if not deleted:
                break
            total += deleted
            print(f"[purge] deleted {deleted:,} (total {total:,}) ...")
    
    print(f"[purge] completed: deleted {total:,} rows older than {cutoff} (>{days}d).")
    return {"deleted": total, "cutoff": cutoff.isoformat(), "skipped": False}

def parse_args():
    p = argparse.ArgumentParser(prog="tradedangerous_listener")
    p.add_argument("--no-update", action="store_true",
                   help="Skip the update process (no Spansh imports).")
    p.add_argument("--export-live-now", action="store_true",
                   help="Run a single listings-live.csv export and exit.")
    p.add_argument("--export-dump-now", action="store_true",
                   help="Run a single full listings.csv export and exit.")
    p.add_argument("--stats", action="store_true",
                   help="Print StationItem counts (before any one-shot export).")
    return p.parse_args()

def main():
    args = parse_args()
    
    # One-shot modes (no heavy bootstrap, no threads)
    if args.stats or args.export_live_now or args.export_dump_now:
        # need config for export_path; load minimal config here
        global config
        if 'config' not in globals() or not isinstance(config, dict):
            config = load_config()
        init_db(config)
        if args.stats:
            print_stationitem_stats()
        if args.export_live_now:
            export_live_once_sa()
        if args.export_dump_now:
            export_dump_once_sa()
        return
    
    # Normal threaded run: bootstrap then start threads
    update_thread, listener_thread, process_thread, live_thread = bootstrap_runtime()
    
    print("Press CTRL-C at any time to quit gracefully.")
    try:
        if config['verbose']:
            if args.no_update:
                print("Skipping update process (--no-update)")
            else:
                print("Starting update process")
        if not args.no_update:
            update_thread.start()
        
        if config['verbose']:
            print("Starting listener thread")
        listener_thread.start()
        
        if config['verbose']:
            print("Starting processor thread")
        process_thread.start()
        
        time.sleep(1)
        if config['verbose']:
            print("Starting live exporter process")
        live_thread.start()
        
        # Maintenance scheduling (supervisor-controlled)
        last_purge_ts = float(config.get("last_purge", 0) or 0)
        if last_purge_ts <= 0:
            last_purge_ts = time.time()
        last_optimize_ts = float(config.get("last_db_maint", 0) or 0)
        if last_optimize_ts <= 0:
            last_optimize_ts = time.time()
        maintenance_active = False
        last_deferral_log = 0.0
        while True:
            now = time.time()
            purge_every_x_hour = int(config.get("purge_every_x_hour", 0) or 0)
            purge_retention_days = int(config.get("purge_retention_days", 0) or 0)
            db_maint_every_x_days = int(config.get("db_maint_every_x_days", 0) or 0)
            purge_due = (
                purge_every_x_hour > 0
                and purge_retention_days > 0
                and (now - last_purge_ts) >= (purge_every_x_hour * _hour)
            )
            optimize_due = (
                db_maint_every_x_days > 0
                and (now - last_optimize_ts) >= (db_maint_every_x_days * 86400)
            )
            maintenance_kind = None
            if optimize_due:
                maintenance_kind = "optimize"
            elif purge_due:
                maintenance_kind = "purge"
            if maintenance_kind and not maintenance_active:
                spansh_active = (
                    spansh_busy is not False
                    and hasattr(spansh_busy, "is_set")
                    and spansh_busy.is_set()
                )
                if spansh_active:
                    if (now - last_deferral_log) >= 60.0:
                        print(f"[Maintenance] Deferring {maintenance_kind}; Spansh is active.")
                        last_deferral_log = now
                else:
                    maintenance_active = True
                    started = time.time()
                    print(f"[Maintenance] Starting {maintenance_kind} window")
                    spansh_active = (
                        spansh_busy is not False
                        and hasattr(spansh_busy, "is_set")
                        and spansh_busy.is_set()
                    )
                    if spansh_active:
                        print(f"[Maintenance] Spansh became active as maintenance started; proceeding to stop update.")
                    if stop_event is not None:
                        stop_event.set()
                    for proc, name in (
                        (update_thread, "update"),
                        (listener_thread, "listener"),
                        (process_thread, "processor"),
                        (live_thread, "live-export"),
                    ):
                        if not proc or getattr(proc, "pid", None) is None:
                            continue
                        
                        if name == "update":
                            spansh_active = (
                                spansh_busy is not False
                                and hasattr(spansh_busy, "is_set")
                                and spansh_busy.is_set()
                            )
                            if spansh_active:
                                print("[Maintenance] Spansh active during maintenance shutdown; terminating update process.")
                        
                        try:
                            proc.join(5.0)
                        except Exception:
                            pass
                        try:
                            if proc.is_alive():
                                print(f"[Maintenance] {name} did not exit; terminating.")
                                proc.terminate()
                                proc.join(10.0)
                        except Exception:
                            pass
                        try:
                            if proc.is_alive() and hasattr(proc, "kill"):
                                print(f"[Maintenance] {name} still alive; killing.")
                                proc.kill()
                                proc.join(5.0)
                        except Exception:
                            pass
                        
                        if name == "update":
                            try:
                                if (
                                    spansh_busy is not False
                                    and hasattr(spansh_busy, "clear")
                                    and not proc.is_alive()
                                ):
                                    spansh_busy.clear()
                            except Exception:
                                pass
                    
                    if maintenance_kind == "purge":
                        try:
                            result = purge_old_stationitems_sa(
                                days=purge_retention_days,
                                batch_size=200_000,
                            )
                            last_purge_ts = time.time()
                            deleted = int(result.get("deleted", 0) or 0)
                            print(f"[Maintenance] Purge deleted {deleted} rows.")
                        except Exception as e:
                            print(f"[Maintenance] ERROR during purge: {e}")
                            last_purge_ts = time.time()
                            print("[Maintenance] Purge failed; will retry next scheduled cycle.")
                    else:
                        res = perform_db_maintenance_sa()
                        if getattr(res, "returncode", 1) != 0:
                            print("[Maintenance] ERROR: mariadb-check failed; supervisor exiting.")
                            raise SystemExit(res.returncode or 2)
                        last_optimize_ts = time.time()
                        last_purge_ts = last_optimize_ts
                    
                    updates = {
                        "last_purge": int(last_purge_ts),
                    }
                    if maintenance_kind == "optimize":
                        updates["last_db_maint"] = int(last_optimize_ts)
                    
                    try:
                        update_config_keys(updates)
                    except Exception as e:
                        print(f"[Maintenance] WARNING: failed to write maintenance timestamps: {e!r}")
                    
                    print(f"[Maintenance] Completed {maintenance_kind} in {time.time() - started:.1f}s")
                    maintenance_active = False
                    if stop_event is not None and hasattr(stop_event, "clear"):
                        stop_event.clear()
                    update_thread, listener_thread, process_thread, live_thread = bootstrap_runtime()
                    if not args.no_update:
                        update_thread.start()
                    listener_thread.start()
                    process_thread.start()
                    live_thread.start()
                    print("[Maintenance] Completed and system restarted")
            try:
                if stop_event is not None and stop_event.is_set() and not maintenance_active:
                    raise KeyboardInterrupt
            except Exception:
                pass
            time.sleep(1)
    
    except KeyboardInterrupt:
        try:
            if stop_event is not None and stop_event.is_set():
                print("[supervisor] Stop requested, stopping.")
            else:
                print("CTRL-C detected, stopping.")
        except Exception:
            print("CTRL-C detected, stopping.")
        print("Please wait for all processes to report they are finished, in case they are currently active.")
        try:
            stop_event.set()
        except Exception:
            pass
        
        def _shutdown_proc(proc, name, join_timeout=2.0):
            pid = getattr(proc, "pid", None)
            if pid is None:
                return
            
            try:
                proc.join(join_timeout)
            except Exception:
                pass
            
            try:
                if proc.is_alive():
                    print(f"[supervisor] {name} still running; terminating.")
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                    try:
                        proc.join(5.0)
                    except Exception:
                        pass
                
                if proc.is_alive():
                    print(f"[supervisor] {name} still running; killing.")
                    try:
                        if hasattr(proc, "kill"):
                            proc.kill()
                        else:
                            os.kill(int(proc.pid), signal.SIGKILL)
                    except Exception:
                        pass
                    try:
                        proc.join(5.0)
                    except Exception:
                        pass
            except Exception:
                pass
        
        try:
            _shutdown_proc(update_thread, "update")
            _shutdown_proc(listener_thread, "listener")
            _shutdown_proc(process_thread, "processor")
            _shutdown_proc(live_thread, "live exporter")
        except KeyboardInterrupt:
            print("CTRL-C again, forcing termination.")
            for proc in (update_thread, listener_thread, process_thread, live_thread):
                try:
                    if getattr(proc, "pid", None) is not None and proc.is_alive():
                        proc.terminate()
                except Exception:
                    pass

if __name__ == "__main__":
    main()
