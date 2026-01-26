#!/usr/bin/env python3

from __future__ import generators

import argparse
import codecs
import csv
import json
import os
import threading
import time
import zlib
import zmq

from calendar import timegm
from collections import defaultdict, namedtuple, deque, OrderedDict
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator

from packaging.version import Version
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker


try:
    import cache
    import commands
    import trade
    import tradedb
    import tradeenv
    import transfers
    import plugins.eddblink_plug
    # New SQLAlchemy DB API (repo-local)
    from db import load_config as load_db_config
    from db import make_engine_from_config, get_session_factory, ensure_fresh_db, resolve_data_dir
except ImportError:
    from tradedangerous import cli as trade, cache, tradedb, tradeenv, transfers, plugins, commands
    from tradedangerous.plugins import eddblink_plug
    from tradedangerous.db import load_config as load_db_config
    from tradedangerous.db import make_engine_from_config, get_session_factory, ensure_fresh_db, resolve_data_dir

from urllib import request


_CFG = load_db_config()
_ENGINE = make_engine_from_config(_CFG)
_DATA_DIR = resolve_data_dir(_CFG)
_BACKEND = (_ENGINE.dialect.name or "").lower()
_SessionFactory: sessionmaker = get_session_factory(_ENGINE)

_minute = 60


@contextmanager
def sa_session() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy Session bound to the TD engine (one per use)."""
    with _SessionFactory() as s:
        yield s


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
    """
    
    uri = 'tcp://eddn.edcd.io:9500'
    supportedSchema = 'https://eddn.edcd.io/schemas/commodity/3'
    
    def __init__(
        self,
        zmqContext=None,
        minBatchTime=36.,  # seconds
        maxBatchTime=60.,  # seconds
        reconnectTimeout=30.,  # seconds
        burstLimit=500,
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
        
        events = self.subscriber.poll(timeout=timeout)
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
            
            supportedSchema = self.supportedSchema
            sub = self.subscriber
            
            batch = defaultdict(list)
            
            bursts = 0
            if self.wait_for_data(softCutoff, hardCutoff):
                for _ in range(self.burstLimit):
                    self.lastJsData = None
                    zdata = None
                    try:
                        zdata = sub.recv(flags=zmq.NOBLOCK, copy=False)
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
                    if len(whitelist_match) == 0:
                        if config['debug'] or config['verbose']:
                            print(f'{system}/{station} update rejected from client not on whitelist: {software} v{swVersion}')
                        continue
                    
                    if whitelist_match[0].get("minversion"):
                        if Version(swVersion) < Version(whitelist_match[0].get("minversion")):
                            if config['debug']:
                                print(f'{system}/{station} rejected with: {software} v{swVersion}')
                            continue
                    
                    timestamp = timestamp.replace("T", " ").replace("+00:00", "")
                    
                    if '.' in timestamp and config['debug']:
                        print(f'Client {software}, v{swVersion}, uses microseconds.')
                        for key in header:
                            if "timestamp" in key:
                                print(f'{key}: {header[key]}')
                        for key in message:
                            if "timestamp" in key:
                                print(f'{key}: {message[key]}')
                    
                    oldEntryList = batch[(system, station)]
                    if oldEntryList:
                        if oldEntryList[0].timestamp > timestamp:
                            continue
                    else:
                        oldEntryList.append(None)
                    
                    oldEntryList[0] = MarketPrice(
                        system, station, market_id, commodities,
                        timestamp,
                        uploader, software, swVersion,
                    )
            
            if bursts >= self.burstLimit:
                softCutoff = min(softCutoff, time.time() + 0.5)
            
            for entry in batch.values():
                queue.append(entry[0])
        self.disconnect()
        print("Listener reporting shutdown.")


def get_messages():
    listener = Listener()
    listener.get_batch(q)


def check_server():
    """
    Checks for updates on the server.
    Only runs for the client listener.
    """
    global update_busy, db_name, item_ids, system_ids, station_ids, process_ack
    
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
            
            url_time = response.getheader("Last-Modified")
            dumpModded = datetime.strptime(url_time, "%a, %d %b %Y %H:%M:%S %Z").timestamp()
            
            if Path.exists(eddbPath / listings_path):
                localModded = (eddbPath / listings_path).stat().st_mtime
            
            if localModded < dumpModded:
                update_busy = True
                print("Update available, waiting for busy signal acknowledgement before proceeding.")
                while not process_ack:
                    rep = 0
                    if config['debug']:
                        print(f'Still waiting for acknowledgment. ({rep})', end='\r')
                        rep = rep + 1
                    time.sleep(1)
                print("Busy signal acknowledged, performing update.")
                options = config['client_options']
                try:
                    trade.main(('trade.py', 'import', '-P', 'eddblink', '-O', options))
                    
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
    
    print("Update checker reporting shutdown.")


def load_config():
    """
    Loads the settings from 'tradedangerous-listener-config.json'.
    
    This client listener follows the client path unconditionally.
    If the config file is missing settings, defaults are filled and the file updated.
    """
    write_config = False
    config = OrderedDict([
        ('side', 'client'),
        ('verbose', True),
        ('debug', False),
        ('client_options', "clean"),
        ('check_update_every_x_min', 1440),
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
    
    if Path.exists(Path("tradedangerous-listener-config.json")):
        with open("tradedangerous-listener-config.json", "r") as fh:
            try:
                temp = json.load(fh, object_pairs_hook=OrderedDict)
                for setting in config:
                    if setting in temp:
                        if config[setting] != temp[setting]:
                            config[setting] = temp[setting]
                    else:
                        write_config = True
            except Exception:
                write_config = True
    else:
        write_config = True
    
    # Force client side for this dedicated client listener.
    config['side'] = 'client'
    
    if write_config:
        with open("tradedangerous-listener-config.json", "w") as config_file:
            json.dump(config, config_file, indent=4)
    
    return config



def validate_config():
    """
    Checks to make sure the loaded config contains valid values.
    If it finds invalid, it marks that as such in the config_file,
    so the default value is used on reload, and then reloads the config.
    """
    global config
    valid = True
    with open("tradedangerous-listener-config.json", "r") as fh:
        config_file = fh.read()
    
    config['side'] = 'client'
    
    if not isinstance(config["verbose"], bool):
        valid = False
        config_file = config_file.replace('"verbose"', '"verbose_invalid"')
    
    if not isinstance(config["debug"], bool):
        valid = False
        config_file = config_file.replace('"debug"', '"debug_invalid"')
    
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
    
    if isinstance(config['check_update_every_x_min'], int):
        if not (1 <= config['check_update_every_x_min'] <= 1440):
            valid = False
            config_file = config_file.replace('"check_update_every_x_min"', '"check_update_every_x_min_invalid"')
    else:
        valid = False
        config_file = config_file.replace('"check_update_every_x_min"', '"check_update_every_x_min_invalid"')
    
    if not valid:
        with open("tradedangerous-listener-config.json", "w") as fh:
            fh.write(config_file)
        config = load_config()


def update_dicts():
    """
    Build lookup dicts from CSVs under dataPath.
    Returns (db_name, item_ids, system_ids, station_ids)
    """
    data_dir = Path(os.environ.get('TD_CSV') or Path(tradeenv.TradeEnv().csvDir)).resolve()
    db_name = str(Path(data_dir).name)
    
    item_ids = dict()
    with open(str(data_dir / Path("Item.csv")), "r", encoding="utf8") as fh:
        items = csv.DictReader(fh, quotechar="'")
        first_row = next(iter(items))
        if 'fdev_id' in first_row:
            iid_key = 'fdev_id'
        else:
            iid_key = 'unq:fdev_id'
        fh.seek(0)
        next(iter(items))
        for item in items:
            item_ids[item[iid_key]] = int(item['unq:item_id'])
    
    system_names = dict()
    system_ids = dict()
    with open(str(data_dir / Path("System.csv")), "r", encoding="utf8") as fh:
        systems = csv.DictReader(fh, quotechar="'")
        for system in systems:
            system_names[int(system['unq:system_id'])] = system['name'].upper()
            system_ids[system['name'].upper()] = int(system['unq:system_id'])
    
    station_ids = dict()
    megaship_types = [19, 24]
    with open(str(data_dir / Path("Station.csv")), "r", encoding="utf8") as fh:
        stations = csv.DictReader(fh, quotechar="'")
        for station in stations:
            if int(station['type_id']) in megaship_types or int(station['unq:station_id']) == 42041:
                full_name = "MEGASHIP"
            else:
                full_name = system_names[int(station['system_id@System.system_id'])]
            full_name += "/" + station['name'].upper()
            station_ids[full_name] = int(station['unq:station_id'])
    
    del system_names
    
    return db_name, item_ids, system_ids, station_ids


def process_messages_sa():
    """
    Consume MarketPrice entries from the global deque `q` and write Station/StationItem updates
    via SQLAlchemy.
    
    Client listener posture:
      - Single writer thread (this thread).
      - No advisory locks.
      - Honour update_busy/process_ack so eddblink imports don't collide with DB writes.
    """
    global go, q
    global process_ack, update_busy
    global config, item_ids, system_ids, station_ids
    
    from sqlalchemy.exc import DBAPIError
    
    DELETE_STATION_ITEMS = text("DELETE FROM StationItem WHERE station_id = :station_id")
    
    INSERT_STATION_ITEM = text(
        "INSERT INTO StationItem ("
        " station_id, item_id, modified,"
        " demand_price, demand_units, demand_level,"
        " supply_price, supply_units, supply_level, from_live)"
        " VALUES (:station_id, :item_id, :modified,"
        " :demand_price, :demand_units, :demand_level,"
        " :supply_price, :supply_units, :supply_level, 0)"
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
    
    def _safe_int(v, default=0):
        try:
            return int(v)
        except Exception:
            return default
    
    def _bool_from_yn(v):
        if v in (True, False):
            return v
        if v is None:
            return False
        if isinstance(v, str):
            return v.strip().lower() in ("y", "yes", "true", "1")
        return bool(v)
    
    def _norm_station_key(system_name: str, station_name: str) -> str:
        return f"{system_name.upper()}/{station_name.upper()}"
    
    process_ack = True
    
    while go:
        if update_busy:
            process_ack = True
            time.sleep(0.25)
            continue
        
        if not q:
            time.sleep(0.05)
            continue
        
        mp = q.popleft()
        if mp is None:
            continue
        
        system = (mp.system or "").upper()
        station = (mp.station or "").upper()
        ts = mp.timestamp
        
        try:
            ts_dt = datetime.strptime(ts.split(".")[0], "%Y-%m-%d %H:%M:%S")
        except Exception:
            try:
                ts_dt = datetime.strptime(ts.replace("T", " ").split(".")[0], "%Y-%m-%d %H:%M:%S")
            except Exception:
                ts_dt = datetime.utcnow()
        
        key = _norm_station_key(system, station)
        if key not in station_ids:
            if config.get('debug') or config.get('verbose'):
                print(f"Not found in Stations: {key}, inserting")
            station_id = None
        else:
            station_id = station_ids[key]
        
        if station_id is None:
            continue
        
        with sa_session() as s:
            try:
                with s.begin():
                    exists = s.execute(GET_STATION_BY_ID, {"station_id": station_id}).first()
                    
                    if not exists:
                        sys_id = system_ids.get(system)
                        if sys_id is None:
                            sys_id = s.execute(GET_SYSTEM_ID_BY_NAME, {"n": system}).scalar()
                        if not sys_id:
                            continue
                        
                        station_row = s.execute(GET_OLD_STATION_INFO, {"sid": station_id}).first()
                        if station_row:
                            name = station
                            ls_from_star = station_row[1]
                            blackmarket = station_row[2]
                            max_pad_size = station_row[3]
                            market = station_row[4]
                            shipyard = station_row[5]
                            outfitting = station_row[6]
                            rearm = station_row[7]
                            refuel = station_row[8]
                            repair = station_row[9]
                            planetary = station_row[10]
                            type_id = station_row[11]
                        else:
                            name = station
                            ls_from_star = 0
                            blackmarket = False
                            max_pad_size = '?'
                            market = True
                            shipyard = False
                            outfitting = False
                            rearm = False
                            refuel = True
                            repair = True
                            planetary = False
                            type_id = 1
                        
                        s.execute(INSERT_NEW_STATION, {
                            "station_id": station_id,
                            "name": name,
                            "system_id": sys_id,
                            "ls_from_star": ls_from_star,
                            "blackmarket": _bool_from_yn(blackmarket),
                            "max_pad_size": max_pad_size,
                            "market": _bool_from_yn(market),
                            "shipyard": _bool_from_yn(shipyard),
                            "modified": ts_dt,
                            "outfitting": _bool_from_yn(outfitting),
                            "rearm": _bool_from_yn(rearm),
                            "refuel": _bool_from_yn(refuel),
                            "repair": _bool_from_yn(repair),
                            "planetary": _bool_from_yn(planetary),
                            "type_id": _safe_int(type_id, 1),
                        })
                    else:
                        current = s.execute(GET_OLD_STATION_INFO, {"sid": station_id}).first()
                        if current:
                            current_system_id = current[12]
                            correct_system_id = system_ids.get(system)
                            if correct_system_id and current_system_id and int(current_system_id) != int(correct_system_id):
                                s.execute(MOVE_STATION, {"system_id": correct_system_id, "name": station, "sid": station_id})
                    
                    s.execute(DELETE_STATION_ITEMS, {"station_id": station_id})
                    
                    avg_sum = defaultdict(int)
                    avg_count = defaultdict(int)
                    best_rows = dict()
                    
                    for c in mp.commodities or []:
                        fdev_id = c.get("commodityId")
                        if fdev_id is None:
                            continue
                        if fdev_id not in item_ids:
                            continue
                        item_id = item_ids[fdev_id]
                        
                        buy_price = _safe_int(c.get("buyPrice"), 0)
                        sell_price = _safe_int(c.get("sellPrice"), 0)
                        demand = _safe_int(c.get("demand"), 0)
                        supply = _safe_int(c.get("stock"), 0)
                        demand_bracket = _safe_int(c.get("demandBracket"), 0)
                        supply_bracket = _safe_int(c.get("stockBracket"), 0)
                        
                        # Some clients can emit duplicate commodity rows where both brackets are blank/0.
                        # Treat these as non-market (cargo-ish) and ignore them.
                        if demand_bracket == 0 and supply_bracket == 0:
                            continue
                        
                        score = 0
                        if demand_bracket > 0:
                            score += 4
                        if supply_bracket > 0:
                            score += 4
                        if demand > 0:
                            score += 2
                        if supply > 0:
                            score += 2
                        if sell_price > 0:
                            score += 1
                        if buy_price > 0:
                            score += 1
                        
                        tie = demand + supply
                        prev = best_rows.get(item_id)
                        if prev is None or score > prev["score"] or (score == prev["score"] and tie > prev["tie"]):
                            best_rows[item_id] = {
                                "score": score,
                                "tie": tie,
                                "buy_price": buy_price,
                                "sell_price": sell_price,
                                "demand": demand,
                                "supply": supply,
                                "demand_bracket": demand_bracket,
                                "supply_bracket": supply_bracket,
                            }
                    
                    for item_id, row in best_rows.items():
                        s.execute(INSERT_STATION_ITEM, {
                            "station_id": station_id,
                            "item_id": item_id,
                            "modified": ts_dt,
                            "demand_price": row["sell_price"],
                            "demand_units": row["demand"],
                            "demand_level": row["demand_bracket"],
                            "supply_price": row["buy_price"],
                            "supply_units": row["supply"],
                            "supply_level": row["supply_bracket"],
                        })
                        
                        if row["buy_price"] > 0:
                            avg_sum[item_id] += row["buy_price"]
                            avg_count[item_id] += 1
                        if row["sell_price"] > 0:
                            avg_sum[item_id] += row["sell_price"]
                            avg_count[item_id] += 1
                    
                    for item_id, total_price in avg_sum.items():
                        cnt = avg_count[item_id]
                        if cnt <= 0:
                            continue
                        avg_price = int(total_price / cnt)
                        s.execute(UPDATE_ITEM_AVG, {"avg_price": avg_price, "item_id": item_id})


                if config.get('verbose'):
                    print(f"Updated {system}/{station}, station_id:'{station_id}', from {mp.software} v{mp.version}")
            
            except DBAPIError as e:
                if config.get('debug'):
                    print(f"[db] error for {system}/{station} station_id={station_id}: {e}")
                continue
            except Exception as e:
                if config.get('debug'):
                    print(f"[proc] error for {system}/{station} station_id={station_id}: {e}")
                continue
    
    print("Processor reporting shutdown.")


update_busy = False
process_ack = False

go = True
q = deque()


def bootstrap_runtime():
    """
    Client listener initialization (threaded).
    Returns the thread objects.
    """
    global dataPath, config, tdb, eddbPath, db_name, item_ids, system_ids, station_ids
    
    dataPath = os.environ.get('TD_CSV') or Path(tradeenv.TradeEnv().csvDir).resolve()
    config = load_config()
    
    summary = ensure_fresh_db(
        backend=_ENGINE.dialect.name,
        engine=_ENGINE,
        data_dir=dataPath,
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
    
    needs_bootstrap = (
        config['client_options'] == 'clean'
        or ensure_fresh_db(
            backend=_BACKEND,
            engine=_ENGINE,
            data_dir=_DATA_DIR,
            metadata=None,
            rebuild=False
        ).get("action") == "needs_rebuild"
    )
    
    if needs_bootstrap:
        print("Initial run")
        trade.main(('trade.py', 'import', '-P', 'eddblink', '-O', 'clean,solo'))
        config['client_options'] = 'all'
        with open("tradedangerous-listener-config.json", "w") as config_file:
            json.dump(config, config_file, indent=4)
    
    if config['verbose']:
        print("Loading TradeDB")
    tdb = tradedb.TradeDB(load=False)
    
    eddb_inst = plugins.eddblink_plug.ImportPlugin(tdb, tradeenv.TradeEnv())
    globals()['eddbPath'] = eddb_inst.dataPath
    
    validate_config()
    if config['verbose']:
        print("Config loaded")
    
    if config['verbose']:
        print("Initializing threads")
    
    listener_thread = threading.Thread(target=get_messages)
    process_thread = threading.Thread(target=process_messages_sa)
    update_thread = threading.Thread(target=check_server)
    
    if config['verbose']:
        print("Updating dicts")
    try:
        db_name, item_ids, system_ids, station_ids = update_dicts()
    except Exception as e:
        print(str(e))
    
    if config['verbose']:
        print("Startup process completed.")
    
    return update_thread, listener_thread, process_thread


def parse_args():
    p = argparse.ArgumentParser(prog="tradedangerous_listener_client")
    p.add_argument("--no-update", action="store_true",
                   help="Skip the server update checker thread (eddblink import).")
    return p.parse_args()


def main():
    args = parse_args()
    
    update_thread, listener_thread, process_thread = bootstrap_runtime()
    
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
        
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("CTRL-C detected, stopping.")
        print("Please wait for all processes to report they are finished, in case they are currently active.")
        global go
        go = False


if __name__ == "__main__":
    main()
