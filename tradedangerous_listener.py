#!/usr/bin/env python3

from __future__ import generators
import os
import json
import time
import zlib
import zmq
import threading
from datetime import datetime, timedelta
import csv
import codecs
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
    import plugins.eddblink_plug
    import plugins.spansh_plug
    # New SQLAlchemy DB API (repo-local)
    from db import load_config, make_engine_from_config, get_session_factory
except ImportError:
    from tradedangerous import cli as trade, cache, tradedb, tradeenv, transfers, plugins, commands
    from tradedangerous.plugins import eddblink_plug, spansh_plug
    from tradedangerous.db import load_config, make_engine_from_config, get_session_factory

from urllib import request
from calendar import timegm
from pathlib import Path
from collections import defaultdict, namedtuple, deque, OrderedDict
from packaging.version import Version

_CFG = load_config()
_ENGINE = make_engine_from_config(_CFG)
_SessionFactory: sessionmaker = get_session_factory(_ENGINE)
_minute = 60
_hour = 3600
_SPANSH_FILE = "galaxy_stations.json"
_SOURCE_URL = f'https://downloads.spansh.co.uk/{_SPANSH_FILE}'
# print(f'Spansh import plugin source file is: {_SOURCE_URL}')

@contextmanager
def sa_session() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy Session bound to the TD engine (one per use)."""
    with _SessionFactory() as s:
        yield s
        
# Backend Specific Maintenance Routines
def perform_db_maintenance_sa() -> None:
    """
    Run light, safe maintenance depending on the active backend.

    - SQLite:
        * VACUUM
        * PRAGMA optimize
    - MariaDB:
        * ANALYZE TABLE on hot tables (stats refresh)
        * OPTIMIZE TABLE StationItem (reclaims space / defrag; quick on InnoDB if little to do)

    Runs in its own transaction scope; prints concise status/errors and returns.
    """
    dialect = _ENGINE.dialect.name.lower()
    try:
        with sa_session() as s:
            if dialect == "sqlite":
                # SQLite: these are safe and beneficial for long-running writers
                s.execute(text("VACUUM"))
                s.execute(text("PRAGMA optimize"))
                print("DB maintenance (SQLite): VACUUM + PRAGMA optimize completed.")
                return

            if dialect in ("mysql", "mariadb"):
                # Keep it light: refresh stats on key tables; optional OPTIMIZE on StationItem
                # NOTE: These statements are auto-committing in MySQL/MariaDB.
                tables = ("StationItem", "Station", "Item")
                for tbl in tables:
                    s.execute(text(f"ANALYZE TABLE {tbl}"))
                # StationItem is the write-hot table; OPTIMIZE it occasionally
                s.execute(text("OPTIMIZE TABLE StationItem"))
                print("DB maintenance (MariaDB): ANALYZE {StationItem,Station,Item} + OPTIMIZE StationItem completed.")
                return

            # Other backends: no-ops for now
            print(f"DB maintenance: backend '{dialect}' has no maintenance routine defined (skipped).")

    except Exception as e:
        # Non-fatal: just log and proceed
        print("Error performing DB maintenance:")
        print("-----------------------------")
        print(e)
        print("-----------------------------")


# Copyright (C) Oliver 'kfsone' Smith <oliver@kfs.org> 2015
#
# Conditional permission to copy, modify, refactor or use this
# code is granted so long as attribution to the original author
# is included.
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
    
    def get_batch(self, queue):
        """
        Greedily collect deduped prices from the firehose over a
        period of between minBatchTime and maxBatchTime, with
        built-in auto-reconnection if there is nothing from the
        firehose for a period of time.
        
        As json data is decoded, it is stored in self.lastJsData.
        
        Validated market list messages are added to the queue.
        """
        while go:
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
                queue.append(entry[0])
        self.disconnect()
        print("Listener reporting shutdown.")

# End of 'kfsone' code.

# We do this because the Listener object must be in the same thread that's running get_batch().
def get_messages():
    listener = Listener()
    listener.get_batch(q)


def check_update():
    """
    Checks for updates to the spansh dump.
    If TDL_SKIP_SPANSH=1 is set in the environment, we skip the costly
    spansh import/export step but still exercise the same busy/ack + dump path.
    """
    global update_busy, dump_busy, process_ack, live_ack, db_name, item_ids, system_ids, station_ids
    
    tdb = tradedb.TradeDB(load = False)
    db = tdb.getDB()
    
    update_file = Path(tdb.tdenv.tmpDir, _SPANSH_FILE)
    
    # Convert the number from the "check_update_every_x_min" setting, which is in minutes,
    # into easily readable hours and minutes.
    h, m = divmod(config['check_update_every_x_min'], 60)
    next_check = ""
    if h > 0:
        next_check = str(h) + " hour"
        if h > 1:
            next_check += "s"
        if m > 0:
            next_check += ", "
    if m > 0:
        next_check += str(m) + " minute"
        if m > 1:
            next_check += "s"
    
    now = round(time.time(), 0) - config['check_update_every_x_min']
    
    startup = True
    
    while go:
        # Trigger daily source update if the dumps have updated since last run.
        # Otherwise, go to sleep for {config['check_update_every_x_min']} minutes before checking again.
        if time.time() >= now + (config['check_update_every_x_min'] * _minute) or startup:
            startup = False
            response = None
            tryLeft = 10
            while tryLeft != 0:
                try:
                    response = request.urlopen(_SOURCE_URL)
                    tryLeft = 0
                except:
                    tryLeft -= 1
            
            if not response:
                print("Error attempting to check for update, no response from server.")
                continue
            
            url_time = response.getheader("Last-Modified")
            last_modified = datetime.strptime(url_time, "%a, %d %b %Y %H:%M:%S %Z").timestamp()
            
            if not config['last_update'] or config['last_update'] < last_modified:
                local_mod_time = 0 if not update_file.exists() else update_file.stat().st_mtime
                
                if config['verbose']:
                    print(f'local_mod_time: {local_mod_time}, last_modified: {last_modified}')
                
                # Download only if not skipping Spansh and local file older/new
                skip_spansh = os.environ.get("TDL_SKIP_SPANSH") == "1"
                if not skip_spansh:
                    if local_mod_time < last_modified:
                        if update_file.exists():
                            update_file.unlink()
                        print(f'Downloading prices from remote URL: {_SOURCE_URL}')
                        try:
                            transfers.download(tdb.tdenv, _SOURCE_URL, update_file)
                        except Exception as e:  # pylint: disable=broad-exception-caught
                            tdb.tdenv.WARN("Problem with download:\n    URL: {}\n    Error: {}", _SOURCE_URL, str(e))
                            return False
                        print(f'Download complete, saved to local file: "{update_file}"')
                        os.utime(update_file, (last_modified, last_modified))
                
                maxage = ((datetime.now() - datetime.fromtimestamp(config.get("last_update", 0))) + timedelta(hours = 1))/timedelta(1)
                options = '-'
                if config['debug']:
                    options += 'w'
                if config['verbose']:
                    options += 'vv'
                if options == '-':
                    options = ''
                
                # TD will fail with an error if the database is in use while it's trying
                # to do its thing, so we need to make sure that neither of the database
                # editing methods are doing anything before running.
                update_busy = True
                print("Spansh update available, waiting for busy signal acknowledgement before proceeding.")
                while not (process_ack and live_ack):
                    rep = 0
                    if config['debug']:
                        print(f'Still waiting for acknowledgment. ({rep})', end = '\r')
                        rep = rep + 1
                    time.sleep(1)
                
                print("Busy signal acknowledged, performing update.")
                try:
                    if skip_spansh:
                        print("TEST MODE: TDL_SKIP_SPANSH=1 → skipping Spansh import/export; proceeding directly to listings.csv dump.")
                    else:
                        # Run the real Spansh import/export
                        trade.main((
                            'trade.py', 'import', '-P', 'spansh',
                            '-O', f'file={update_file},maxage={maxage},skip_stationitems=1',
                            options,
                        ))
                        # trade.main(('trade.py', 'export', '--path', f'{config["export_path"]}')) # Why export twice?
                        
                        # Since there's been an update, we need to redo all this.
                        if config['verbose']:
                            print("Updating dictionaries...")
                        db_name, item_ids, system_ids, station_ids = update_dicts()
                        
                        # Only advance last_update when we actually processed the new dump
                        config['last_update'] = last_modified
                        if config['debug']:
                            print(f"last_update: {config['last_update']}, last_modified: {last_modified}")
                        
                        with open("tradedangerous-listener-config.json", "w") as config_file:
                            json.dump(config, config_file, indent = 4)
                    
                    now = round(time.time(), 0)
                
                except Exception as e:
                    print("Error when running update:")
                    print(e)
                    update_busy = False
                    continue
                
                if config['verbose']:
                    print("Update complete (or skipped in TEST MODE), turning off busy signal.")
                dump_busy = True
                update_busy = False
                
                if config['debug']:
                    print("Beginning full listings export...")
                # SA exporter
                print("[update] calling export_dump_sa()")
                export_dump_sa()
                print("[update] export_dump_sa() finished")
            
            else:
                print(f'No update, checking again in {next_check}.')
                now = round(time.time(), 0)
        
        if config['debug'] and ((round(time.time(), 0) - now) % 3600 == 0):
            print(f'Update checker is sleeping: {(int(now + (config["check_update_every_x_min"] * _minute) - round(time.time(), 0)) / 60)} minutes remain until next check.')
        time.sleep(1)
    
    print("Update checker reporting shutdown.")



def check_server():
    """
    Checks for updates on the server.
    Only runs when program configured as client.
    """
    global update_busy, db_name, item_ids, system_ids, station_ids
    
    # Convert the number from the "check_update_every_x_min" setting, which is in minutes,
    # into easily readable hours and minutes.
    h, m = divmod(config['check_update_every_x_min'], 60)
    next_check = ""
    if h > 0:
        next_check = str(h) + " hour"
        if h > 1:
            next_check += "s"
        if m > 0:
            next_check += ", "
    if m > 0:
        next_check += str(m) + " minute"
        if m > 1:
            next_check += "s"
    
    now = round(time.time(), 0) - config['check_update_every_x_min']
    localModded = 0
    
    BASE_URL = plugins.eddblink_plug.BASE_URL
    LISTINGS = "listings-live.csv"
    listings_path = Path(LISTINGS)
    url = BASE_URL + LISTINGS
    
    while go:
        # Trigger update if the server files have updated.
        # Otherwise, go to sleep for {config['check_update_every_x_min']} minutes before checking again.
        if time.time() >= now + (config['check_update_every_x_min'] * _minute):
            
            response = 0
            tryLeft = 10
            while tryLeft != 0:
                try:
                    response = request.urlopen(url)
                    tryLeft = 0
                except:
                    tryLeft -= 1
            
            if not response:
                print("Error attempting to check for update, no response from server.")
                continue
            
            url_time = request.urlopen(url).getheader("Last-Modified")
            dumpModded = datetime.strptime(url_time, "%a, %d %b %Y %H:%M:%S %Z").timestamp()
            
            # Now that we have the Unix epoch time of the dump file, get the same from the local file.
            if Path.exists(eddbPath / listings_path):
                localModded = (eddbPath / listings_path).stat().st_mtime
            
            if localModded < dumpModded:
                # TD will fail with an error if the database is in use while it's trying
                # to do its thing, so we need to make sure that neither of the database
                # editing methods are doing anything before running.
                update_busy = True
                print("Update available, waiting for busy signal acknowledgement before proceeding.")
                while not process_ack:
                    rep = 0
                    if config['debug']:
                        print(f'Still waiting for acknowledgment. ({rep})', end = '\r')
                        rep = rep + 1
                    time.sleep(1)
                print("Busy signal acknowledged, performing update.")
                options = config['client_options']
                try:
                    trade.main(('trade.py', 'import', '-P', 'eddblink', '-O', options))
                    
                    # Since there's been an update, we need to redo all this.
                    db_name, item_ids, system_ids, station_ids = update_dicts()
                    
                    print("Update complete, turning off busy signal.")
                    update_busy = False
                    now = round(time.time(), 0)
                
                except Exception as e:
                    print("Error when running update:")
                    print(e)
            
            else:
                print(f'No update, checking again in {next_check}.')
                now = round(time.time(), 0)
        
        if config['debug'] and ((round(time.time(), 0) - now) % 60 == 0):
            print("Update checker is sleeping: "
                    + str(int(now + (config['check_update_every_x_min'] * _minute) - round(time.time(), 0)))
                    + " minutes remain until next check.")
        time.sleep(1)
    
    # If not go:
    print("Update checker reporting shutdown.")

def load_config():
    """
    Loads the settings from 'tradedangerous-listener-configuration.json'.
    If the config_file does not exist or is missing any settings,
    the default will be used for any missing setting,
    and the config_file will be updated to include all settings,
    preserving the existing (if any) settings' current values.
    """
    
    write_config = False
    # Initialize config with default settings.
    # NOTE: Whitespace added for readability.
    config = OrderedDict([                                                                          \
                            ('side', 'client'),                                                     \
                            ('verbose', True),                                                      \
                            ('debug', False),                                                       \
                            ('last_update', 0),                                                     \
                            ('client_options', "clean"),                                            \
                            ('check_update_every_x_min', 1440),                                       \
                            ('export_live_every_x_min', 5),                                         \
                            ('export_dump_every_x_hour', 24),                                       \
                            ('db_maint_every_x_hour', 12),                                          \
                            ('export_path', './tmp'),                                              \
                            ('whitelist',                                                           \
                                [                                                                   \
                                    OrderedDict([('software', 'E:D Market Connector [Windows]')]),  \
                                    OrderedDict([('software', 'E:D Market Connector [Mac OS]')]),   \
                                    OrderedDict([('software', 'E:D Market Connector [Linux]')]),    \
                                    OrderedDict([('software', 'EDDiscovery')])                      \
                                ]                                                                   \
                            )                                                                       \
                        ])
    
    # Load the settings from the configuration file if it exists.
    if Path.exists(Path("tradedangerous-listener-config.json")):
        with open("tradedangerous-listener-config.json", "r") as fh:
            try:
                temp = json.load(fh, object_pairs_hook = OrderedDict)
                # For each setting in config,
                # if file setting exists and isn't the default,
                # overwrite config setting with file setting.
                for setting in config:
                    if setting in temp:
                        if config[setting] != temp[setting]:
                            config[setting] = temp[setting]
                    else:
                        # If any settings don't exist in the config_file, need to update the file.
                        write_config = True
            except:
                # If, for some reason, there's an error trying to load
                # the config_file, treat it as if it doesn't exist.
                write_config = True
    else:
        # If the config_file doesn't exist, need to make it.
        write_config = True
    
    # Write the current configuration to the file, if needed.
    if write_config:
        with open("tradedangerous-listener-config.json", "w") as config_file:
            json.dump(config, config_file, indent = 4)
    
    # We now have a config that has valid values for all the settings, and a
    # matching config_file so the settings are preserved for the next run.
    return config


def validate_config():
    """
    Checks to make sure the loaded config contains valid values.
    If it finds any invalid, it marks that as such in the config_file
    so the default value is used on reload, and then reloads the config.
    """
    global config
    valid = True
    with open("tradedangerous-listener-config.json", "r") as fh:
        config_file = fh.read()
    
    # For each of these settings, if the value is invalid, mark the key.
    
    # 'side' == 'client' || 'server'
    config['side'] = config['side'].lower()
    if config['side'] != 'server' and config['side'] != 'client':
        valid = False
        config_file = config_file.replace('"side"', '"side_invalid"')
    
    # 'verbose' == True || False
    if not isinstance(config["verbose"], bool):
        valid = False
        config_file = config_file.replace('"verbose"', '"verbose_invalid"')
    
    # 'debug' == True || False
    if not isinstance(config["debug"], bool):
        valid = False
        config_file = config_file.replace('"debug"', '"debug_invalid"')
    
    # 'client_options' : eddblink options (`trade -P eddblink -O help`)
    # (Only used when `config['side'] == 'client'`)
    # For this one, rather than completely replace invalid values with
    # the default, check to see if any of the values are valid and keep
    # those, prepending the default values to the setting if they
    # aren't already in the setting.
    if isinstance(config['client_options'], str):
        options = config['client_options'].split(',')
        valid_options = ""
        cmdenv = commands.CommandIndex().parse
        plugin_options = plugins.load(cmdenv(['trade', 'import', '--plug', 'eddblink', '-O', 'help']).plug, "ImportPlugin").pluginOptions.keys()
        
        for option in options:
            if option in plugin_options:
                if valid_options != "":
                    valid_options += ","
                valid_options += option
            else:
                valid = False
        
        if not valid:
            if valid_options.find("clean") == -1:
                valid_options = f'clean,{valid_options}'
            config_file = config_file.replace(config['client_options'], valid_options)
    else:
        valid = False
        config_file = config_file.replace('"client_options"', '"client_options_invalid"')
    
    # 'check_update_every_x_min' >= 1 && <= 1440 (1 day)
    if isinstance(config['check_update_every_x_min'], int):
        if config['check_update_every_x_min'] < 1 or config['check_update_every_x_min'] > 1440:
            valid = False
            config_file = config_file.replace('"check_update_every_x_min"', '"check_update_every_x_min_invalid"')
    else:
        valid = False
        config_file = config_file.replace('"check_update_every_x_min"', '"check_update_every_x_min_invalid"')
    
    # 'export_dump_every_x_hour' >= 1 && <= 24 (1 day)
    # (Only used when `config['side'] == 'server'`)
    if isinstance(config['export_dump_every_x_hour'], int):
        if config['export_dump_every_x_hour'] < 1 or config['export_dump_every_x_hour'] > 24:
            valid = False
            config_file = config_file.replace('"export_dump_every_x_hour"', '"export_dump_every_x_hour_invalid"')
    else:
        valid = False
        config_file = config_file.replace('"export_dump_every_x_hour"', '"export_dump_every_x_hour_invalid"')
    
    # 'export_live_every_x_min' >= 1 && <= 720 (12 hours)
    # (Only used when `config['side'] == 'server'`)
    if isinstance(config['export_live_every_x_min'], int):
        if config['export_live_every_x_min'] < 1 or config['export_live_every_x_min'] > 720:
            valid = False
            config_file = config_file.replace('"export_live_every_x_min"', '"export_live_every_x_min_invalid"')
    else:
        valid = False
        config_file = config_file.replace('"export_live_every_x_min"', '"export_live_every_x_min_invalid"')
    
    # 'db_maint_every_x_hour' >= 1 && <= 240 (10 days)
    if isinstance(config['db_maint_every_x_hour'], (int, float)):
        if config['db_maint_every_x_hour'] < 1 or config['db_maint_every_x_hour'] > 240:
            valid = False
            config_file = config_file.replace('"db_maint_every_x_hour"', '"db_maint_every_x_hour_invalid"')
    else:
        valid = False
        config_file = config_file.replace('"db_maint_every_x_hour"', '"db_maint_every_x_hour_invalid"')
    
    # 'export_path': location (absolute or relative) of folder to save the exported listings files
    # (Only used when `config['side'] == 'server'`)
    if not Path.exists(Path(config['export_path'])):
        valid = False
        config_file = config_file.replace('"export_path"', '"export_path_invalid"')
    
    if not valid:
        # Before we reload the config to set the invalid values back to default,
        # we need to write the changes we made to the file.
        with open("tradedangerous-listener-config.json", "w") as fh:
            fh.write(config_file)
        config = load_config()

def db_locked_message(source: str)  -> None:
    print(f"[{source}] - DB locked, waiting for access.", end="\n")
    time.sleep(1)

def process_messages_sa():
    """
    Consume MarketPrice entries and upsert rows via SQLAlchemy (threaded mode).
    Mirrors legacy behavior on unknown systems/stations: log and skip, keep running.
    Performs periodic DB maintenance and respects busy flags.
    """
    global process_ack, update_busy, dump_busy, live_busy, db_name, item_ids, system_ids, station_ids

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
    GET_SYSTEM_ID_BY_NAME = text("SELECT system_id FROM System WHERE UPPER(name) = :n")

    # Maintenance cadence
    maintenance_deadline = time.time() + (config['db_maint_every_x_hour'] * _hour)

    def resolve_system_id(sess, system_name_upper: str):
        """Try dict first, then DB lookup by name (case-insensitive)."""
        sid = system_ids.get(system_name_upper)
        if sid:
            return sid
        row = sess.execute(GET_SYSTEM_ID_BY_NAME, {"n": system_name_upper}).first()
        return row[0] if row else None

    while go:
        # Respect busy flags (threaded parity)
        if update_busy or dump_busy or live_busy:
            print("Message processor acknowledging busy signal.")
            process_ack = True
            while (update_busy or dump_busy or live_busy) and go:
                time.sleep(1)
            if not go:
                break
            process_ack = False
            print("Busy signal off, message processor resuming.")

        # Maintenance
        if time.time() >= maintenance_deadline:
            try:
                print(f'Performing database maintenance tasks. {str(datetime.now())}')
                perform_db_maintenance_sa()
                print(f'Database maintenance tasks completed. {str(datetime.now())}')
            except Exception as e:
                print("Error performing maintenance:", e)
            maintenance_deadline = time.time() + (config['db_maint_every_x_hour'] * _hour)

        # Next message
        try:
            entry = q.popleft()
        except IndexError:
            time.sleep(1)
            continue

        system = entry.system.upper()
        station = entry.station.upper()
        station_id = entry.market_id
        modified = entry.timestamp.replace('T', ' ').replace('Z', '')
        commodities = entry.commodities

        if config['debug']:
            print(f'Processing: {system}/{station} timestamp:{modified}')

        # Prepare item rows
        item_rows, avg_rows = [], []
        for commodity in commodities:
            if commodity['sellPrice'] == 0 and commodity['buyPrice'] == 0:
                continue
            item_edid = db_name.get(commodity['name'].lower())
            if not item_edid:
                if config['debug']:
                    print(f"Ignoring item: {commodity['name']}")
                continue
            item_id = item_ids.get(item_edid) or int(item_edid)
            item_rows.append({
                "station_id": station_id, "item_id": item_id, "modified": modified,
                "demand_price": commodity['sellPrice'], "demand_units": commodity['demand'],
                "demand_level": (commodity['demandBracket'] if commodity['demandBracket'] != '' else -1),
                "supply_price": commodity['buyPrice'], "supply_units": commodity['stock'],
                "supply_level": (commodity['stockBracket'] if commodity['stockBracket'] != '' else -1),
            })
            avg_rows.append({"avg_price": commodity['meanPrice'], "item_id": item_id})

        from_megaship = f"MEGASHIP/{station}"

        try:
            with sa_session() as s:
                with s.begin():
                    # Ensure station exists or can be added, but only if we know system_id
                    exists = s.execute(GET_STATION_BY_ID, {"station_id": station_id}).first()
                    if not exists:
                        sys_id = resolve_system_id(s, system)
                        if sys_id is None:
                            # Legacy parity: we cannot create Station without a system_id → log and skip.
                            print(f"WARNING: system not found for station insert; skipping: {system}/{station} (market_id={station_id})")
                            continue

                        maybe_old = station_ids.get(from_megaship)
                        if maybe_old:
                            if config['verbose']:
                                print(f'Megaship station, updating system to {system}')
                            s.execute(MOVE_STATION, {"system_id": sys_id, "name": entry.station, "sid": maybe_old})
                        else:
                            if config['verbose']:
                                print(f'Not found in Stations: {system}/{station}, inserting into DB.')
                            s.execute(INSERT_NEW_STATION, {
                                "station_id": station_id, "name": entry.station, "system_id": sys_id,
                                "ls_from_star": 999999, "blackmarket": '?', "max_pad_size": '?',
                                "market": 'Y', "shipyard": '?', "modified": modified, "outfitting": '?',
                                "rearm": '?', "refuel": '?', "repair": '?', "planetary": '?', "type_id": 0,
                            })
                            station_ids[f'{system}/{station}'] = station_id

                    # Migrate old station_id mapping if needed (parity with legacy)
                    old_sid = station_ids.get(f'{system}/{station}')
                    if old_sid and old_sid != station_id:
                        res = s.execute(GET_OLD_STATION_INFO, {"sid": old_sid}).first()
                        if res:
                            (nm, ls, bm, mps, mk, sy, of, ra, rf, rp, pl, ti, old_sys_id) = res
                            s.execute(DELETE_STATION, {"sid": old_sid})
                            s.execute(INSERT_NEW_STATION, {
                                "station_id": station_id, "name": nm, "system_id": old_sys_id,
                                "ls_from_star": ls, "blackmarket": bm, "max_pad_size": mps, "market": mk,
                                "shipyard": sy, "modified": modified, "outfitting": of, "rearm": ra, "refuel": rf,
                                "repair": rp, "planetary": pl, "type_id": ti,
                            })

                    # Replace market rows
                    s.execute(DELETE_STATION_ITEMS, {"station_id": station_id})
                    if item_rows:
                        s.execute(INSERT_STATION_ITEM, item_rows)
                    if avg_rows:
                        s.execute(UPDATE_ITEM_AVG, avg_rows)

            if config['verbose']:
                print(f"Updated {system}/{station}, station_id:'{station_id}', from {entry.software} v{entry.version}")
            else:
                print(f'Updated {system}/{station}')

        except Exception as e:
            # Keep processing on any unexpected DB error; log and continue
            print(f"ERROR processing {system}/{station} (market_id={station_id}): {e}")

    print("Message processor (SA) reporting shutdown.")



def export_live_sa():
    """
    Emit listings-live.csv from StationItem WHERE from_live = 1.
    Preserves existing busy-signal behaviour.
    """
    global live_ack, live_busy, process_ack, dump_busy, update_busy

    listings_file = (Path(config['export_path']).resolve() / Path("listings-live.csv"))
    listings_tmp = listings_file.with_suffix(".tmp")
    print(f'Live listings will be exported to: \n\t{listings_file}')

    now = time.time()
    while go:
        while time.time() < now + (config['export_live_every_x_min'] * _minute):
            if not go:
                break
            if dump_busy or update_busy:
                print("Live listings exporter acknowledging busy signal.")
                live_ack = True
                while (dump_busy or update_busy) and go:
                    time.sleep(1)
                if not go:
                    break
                live_ack = False
                print("Busy signal off, live listings exporter resuming.")
                now = time.time()
            time.sleep(1)

        if not go:
            break

        start = datetime.now()
        print(f'Live listings exporter sending busy signal. {start}')
        live_busy = True
        while not process_ack:
            if not go:
                break
            time.sleep(0.05)
        print("Busy signal acknowledged, getting live listings for export.")

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


        try:
            with sa_session() as s:
                result = s.execute(SELECT_LIVE.execution_options(stream_results=True))
                with open(str(listings_tmp), "w") as f:
                    f.write("id,station_id,commodity_id,supply,supply_bracket,buy_price,sell_price,demand,demand_bracket,collected_at\n")
                    lineNo = 1
                    for row in result:
                        if not go:
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
            print(e)
            live_busy = False
            continue

        if config['verbose']:
            print('Live listings exporter finished with database, releasing lock.')
        live_busy = False

        if not go:
            listings_tmp.unlink(missing_ok=True)
            print("Export aborted, received shutdown signal.")
            break

        while listings_file.exists():
            try:
                listings_file.unlink()
            except:
                time.sleep(1)
        listings_tmp.rename(listings_file)
        print(f'Export completed in {datetime.now() - start}')
        now = time.time()

    print("Live listings exporter reporting shutdown.")


def export_dump_sa():
    """
    Emit listings.csv from all StationItem rows; reset from_live=0 beforehand.
    Preserves existing busy-signal behaviour.
    """
    global dump_busy, process_ack, live_ack

    listings_file = (Path(config['export_path']).resolve() / Path("listings.csv"))
    listings_tmp = listings_file.with_suffix(".tmp")
    print(f'Listings will be exported to: \n\t{listings_file}')

    start = datetime.now()
    print(f'Listings exporter sending busy signal. {start}')
    dump_busy = True

    while not (process_ack and live_ack):
        if not go:
            break
        time.sleep(1)

    print("Busy signal acknowledged, getting listings for export.")

    UPDATE_CLEAR_LIVE = text("UPDATE StationItem SET from_live = 0")
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
            with s.begin():
                s.execute(UPDATE_CLEAR_LIVE)
            result = s.execute(SELECT_ALL.execution_options(stream_results=True))
            with open(str(listings_tmp), "w") as f:
                f.write("id,station_id,commodity_id,supply,supply_bracket,buy_price,sell_price,demand,demand_bracket,collected_at\n")
                lineNo = 1
                for row in result:
                    if not go:
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
        print("Aborting export:"); print(e)
        dump_busy = False
        return

    if config['verbose']:
        print('Listings exporter finished with database, releasing lock.')
    dump_busy = False

    if not go:
        listings_tmp.unlink(missing_ok=True)
        print("Export aborted, received shutdown signal.")
    else:
        while listings_file.exists():
            try:
                listings_file.unlink()
            except:
                time.sleep(1)
        listings_tmp.rename(listings_file)
        print(f'Export completed in {datetime.now() - start}')


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

go = True
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
    if config['client_options'] == 'clean' or not Path(dataPath, 'TradeDangerous.db').exists():
        print("Initial run")
        trade.main(('trade.py', 'import', '-P', 'eddblink', '-O', 'clean,solo'))
        config['client_options'] = 'all'
        with open("tradedangerous-listener-config.json", "w") as config_file:
            json.dump(config, config_file, indent=4)

    if config['verbose']:
        print("Loading TradeDB")
    tdb = tradedb.TradeDB(load = False)

    # eddblink data path (for client mode)
    eddb_inst = plugins.eddblink_plug.ImportPlugin(tdb, tradeenv.TradeEnv())
    globals()['eddbPath'] = eddb_inst.dataPath

    validate_config()
    if config['verbose']:
        print("Config loaded")

    # Ensure export folder exists
    try:
        Path(config['export_path']).mkdir()
    except FileExistsError:
        pass

    if config['verbose']:
        print("Initializing threads")

    # Thread targets: use the SA variants we refactored
    listener_thread = threading.Thread(target=get_messages)
    process_thread  = threading.Thread(target=process_messages_sa)

    if config['side'] == 'client':
        update_thread = threading.Thread(target=check_server)
    else:
        update_thread = threading.Thread(target=check_update)

    live_thread = threading.Thread(target=export_live_sa)

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
    UPDATE_CLEAR_LIVE = text("UPDATE StationItem SET from_live = 0")
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
    listings_tmp  = listings_file.with_suffix(".tmp")
    start = datetime.now()
    with sa_session() as s:
        with s.begin():
            s.execute(UPDATE_CLEAR_LIVE)
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


def parse_args():
    p = argparse.ArgumentParser(prog="tradedangerous_listener")
    p.add_argument("--no-update", action="store_true",
                   help="Skip the update thread (no Spansh/client imports).")
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
                print("Skipping update thread (--no-update)")
            else:
                print("Starting update thread")
        if not args.no_update:
            update_thread.start()
            time.sleep(5)

        if config['verbose']:
            print("Starting listener thread")
        listener_thread.start()

        if config['verbose']:
            print("Starting processor thread")
        process_thread.start()

        if config['side'] == 'server':
            time.sleep(1)
            if config['verbose']:
                print("Starting live exporter thread")
            live_thread.start()
        else:
            global live_ack
            live_ack = True

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("CTRL-C detected, stopping.")
        print("Please wait for all processes to report they are finished, in case they are currently active.")
        global go
        go = False


if __name__ == "__main__":
    main()
