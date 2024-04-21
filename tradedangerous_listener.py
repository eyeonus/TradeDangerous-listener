#!/usr/bin/env python3

from __future__ import generators
import os
import json
import time
import zlib
import zmq
import threading
from datetime import datetime, timedelta
import sqlite3
import csv
import codecs
import sys
#from builtins import False

try:
    import cache
    import trade
    import tradedb
    import tradeenv
    import transfers
    import plugins.spansh_plug
except:
    from tradedangerous import cli as trade, cache, tradedb, tradeenv, transfers, plugins, commands
    from tradedangerous.plugins import spansh_plug

from urllib import request
from calendar import timegm
from pathlib import Path
from collections import defaultdict, namedtuple, deque, OrderedDict
from distutils.version import LooseVersion

_minute = 60
_hour = 3600

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
    
    Rather than individual upates, prices are captured across a window of
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
            
            if self.wait_for_data(softCutoff, hardCutoff):
                # When wait_for_data returns True, there is some data waiting,
                # possibly multiple messages. At this point we can afford to
                # suck down whatever is waiting in "nonblocking" mode until
                # we reach the burst limit or we get EAGAIN.
                bursts = 0
                for _ in range(self.burstLimit):
                    self.lastJsData = None
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
                            print(system + "/" + station + " update rejected from client not on whitelist: " + software + " v" + swVersion + "\n")
                        continue
                    # Upload software with version less than the defined minimum is ignored.
                    if whitelist_match[0].get("minversion"):
                        if LooseVersion(swVersion) < LooseVersion(whitelist_match[0].get("minversion")):
                            if config['debug']:
                                print(system + "/" + station + " rejected with:" + software + swVersion + "\n")
                            continue
                    # We've received real data.
                    
                    # Normalize timestamps
                    timestamp = timestamp.replace("T", " ").replace("+00:00", "")
                    
                    #Find the culprit!
                    if '.' in timestamp and config['debug']:
                        print("Client " + software + ", version " + swVersion + ", uses microseconds.")
                        for key in header:
                            if "timestamp" in key:
                                print(str(key) + ": " + str(header[key]))
                        for key in message:
                            if "timestamp" in key:
                                print(str(key) + ": " + str(message[key]))
                    
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


def db_execute(db, sql_cmd, args = None):
    cur = db.cursor()
    success = False
    result = None
    while go and not success:
        try:
            if args:
                result = cur.execute(sql_cmd, args)
            else:
                result = cur.execute(sql_cmd)
            success = True
        except sqlite3.OperationalError as e:
                if "locked" not in str(e):
                    success = True
                    raise sqlite3.OperationalError(e)
                else:
                    print("Database is locked, waiting for access.", end = "\n")
                    print(f'Error message: {e}')
                    time.sleep(1)
    return result


# We do this because the Listener object must be in the same thread that's running get_batch().
def get_messages():
    listener = Listener()
    listener.get_batch(q)


def check_update():
    """
    Checks for updates to the spansh dump.
    """
    global update_busy, process_ack, live_ack, db_name, item_ids, system_ids, station_ids
    
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
    dumpModded = 0
    localModded = 0
    
    # # The following values only need to be assigned once, no need to be in the while loop.
    # BASE_URL = plugins.eddblink_plug.BASE_URL
    # LISTINGS = "listings.csv"
    # listings_path = Path(LISTINGS)
    
    Months = {'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12}
    SOURCE_URL = 'https://downloads.spansh.co.uk/galaxy_stations.json'
    
    # request.urlopen(BASE_URL + LISTINGS)
    # url = BASE_URL + LISTINGS
    
    startup = True
    while go:
        # Trigger daily EDDB update if the dumps have updated since last run.
        # Otherwise, go to sleep for {config['check_update_every_x_min']} minutes before checking again.
        if time.time() >= now + (config['check_update_every_x_min'] * _minute) or startup:
            startup = False
            response = 0
            tryLeft = 10
            while tryLeft != 0:
                try:
                    response = request.urlopen(SOURCE_URL)
                    tryLeft = 0
                except:
                    tryLeft -= 1
            
            if not response:
                print("Error attempting to check for update, no response from server.")
                continue
            
            url_time = request.urlopen(SOURCE_URL).getheader("Last-Modified")
            # dumpModded = datetime.strptime(url_time, "%a, %d %b %Y %H:%M:%S %Z").timestamp()
            # # Now that we have the Unix epoch time of the dump file, get the same from the local file.
            # if Path.exists(eddbPath / listings_path):
            #     localModded = (eddbPath / listings_path).stat().st_mtime
            #
            #
            # if localModded < dumpModded:

            last_modified = datetime.strptime(url_time, "%a, %d %b %Y %H:%M:%S %Z").timestamp()
            
            if not config['last_update'] or config['last_update'] < last_modified:
                maxage=((datetime.now() - datetime.fromtimestamp(config["last_update"])) + timedelta(hours=1))/timedelta(1)
                options = '-'
                if config['debug']:
                    options += 'w'
                if config['verbose']:
                    options += 'vvv'
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
                        print("Still waiting for acknowledgment. (" + str(rep) + ")", end = '\r')
                        rep = rep + 1
                    time.sleep(1)
                
                print("Busy signal acknowledged, performing update.")
                try:
                    trade.main(('trade.py', 'import', '-P', 'spansh', '-O', f'listener,url={SOURCE_URL},maxage={maxage}', options))
                                        
                    for table in ["Category",
                                    "Item",
                                    "RareItem",
                                    "Ship",
                                    "ShipVendor",
                                    "Station",
                                    "System",
                                    "Upgrade",
                                    "UpgradeVendor",
                                    "FDevShipyard",
                                    "FDevOutfitting",
                                ]:
                        _, path = csvexport.exportTableToFile( tdb, tdenv, table )
                        if Path(f'{config["export_path"]}\{table}.csv').resolve() != dataPath.resolve():
                            os.copyfile(path, Path(f'{config["export_path"]}\{table}.csv').resolve())

                    if config['debug']:
                        print("Updating dictionaries...")
                    # Since there's been an update, we need to redo all this.
                    db_name, item_ids, system_ids, station_ids = update_dicts()
                    
                    config['last_update'] = last_modified
                    with open("tradedangerous-listener-config.json", "w") as config_file:
                        json.dump(config, config_file, indent = 4)
                    
                    now = round(time.time(), 0)
                    
                except Exception as e:
                    print("Error when running update:")
                    print(e)
                
                print("Update complete, turning off busy signal.")
                update_busy = False
                if config['side'] == 'server':
                    export_dump()
                
            else:
                print("No update, checking again in " + next_check + ".")
                now = round(time.time(), 0)
        
        if config['debug'] and ((round(time.time(), 0) - now) % 3600 == 0):
            print("Update checker is sleeping: " + str(int(now + (config['check_update_every_x_min'] * _minute) - round(time.time(), 0)) / 60) + " minutes remain until next check.")
        time.sleep(1)
    
    #If not go:
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
                            ('last_update', 0),                                                  \
                            ('check_update_every_x_min', 60),                                       \
                            ('export_live_every_x_min', 5),                                         \
                            ('export_dump_every_x_hour', 24),                                       \
                            ('db_maint_every_x_hour', 12),                                          \
                            ('export_path', './data/eddb'),                                         \
                            ('whitelist',                                                           \
                                [                                                                   \
                                    OrderedDict([ ('software', 'E:D Market Connector [Windows]') ]),\
                                    OrderedDict([ ('software', 'E:D Market Connector [Mac OS]')  ]),\
                                    OrderedDict([ ('software', 'E:D Market Connector [Linux]')   ]),\
                                    OrderedDict([ ('software', 'EDDiscovery')                    ]) \
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
    
    #The spansh import plugin doesn't need any options specified.
    if config.get('plugin_options'):
        valid = False
        config_file = config_file.replace('"plugin_options"', '"plugin_options_invalid"')
    
    # # 'plugin_options': For this one, rather than completely replace invalid
    # # values with the default, check to see if any of the values are valid
    # # and keep those, prepending the default values to the setting if they
    # # aren't already in the setting.
    #
    # if isinstance(config['plugin_options'], str):
    #     options = config['plugin_options'].split(',')
    #     valid_options = ""
    #     cmdenv = commands.CommandIndex().parse
    #     plugin_options = plugins.load(cmdenv(['trade', 'import', '--plug', 'eddblink', '-O', 'help']).plug, "ImportPlugin").pluginOptions.keys()
    #
    #     for option in options:
    #         if option in plugin_options:
    #             if valid_options != "":
    #                 valid_options += ","
    #             valid_options += option
    #         else:
    #             valid = False
    #
    #     if not valid:
    #         if valid_options.find("force") == -1:
    #             valid_options = "force," + valid_options
    #         if valid_options.find("skipvend") == -1:
    #             valid_options = "skipvend," + valid_options
    #         if valid_options.find("all") == -1:
    #             valid_options = "all," + valid_options
    #         config_file = config_file.replace(config['plugin_options'], valid_options)
    # else:
    #     valid = False
    #     config_file = config_file.replace('"plugin_options"', '"plugin_options_invalid"')
    
    # 'check_update_every_x_min' >= 1 && <= 1440 (1 day)
    if isinstance(config['check_update_every_x_min'], int):
        if config['check_update_every_x_min'] < 1 or config['check_update_every_x_min'] > 1440:
            valid = False
            config_file = config_file.replace('"check_update_every_x_min"', '"check_update_every_x_min_invalid"')
    else:
        valid = False
        config_file = config_file.replace('"check_update_every_x_min"', '"check_update_every_x_min_invalid"')
    
    # 'export_dump_every_x_hour' >= 1 && <= 24 (1 day)
    if isinstance(config['export_dump_every_x_hour'], int):
        if config['export_dump_every_x_hour'] < 1 or config['export_dump_every_x_hour'] > 24:
            valid = False
            config_file = config_file.replace('"export_dump_every_x_hour"', '"export_dump_every_x_hour_invalid"')
    else:
        valid = False
        config_file = config_file.replace('"export_dump_every_x_hour"', '"export_dump_every_x_hour_invalid"')
    
    # 'export_live_every_x_min' >= 1 && <= 720 (12 hours)
    if isinstance(config['export_live_every_x_min'], int):
        if config['export_live_every_x_min'] < 1 or config['export_live_every_x_min'] > 720:
            valid = False
            config_file = config_file.replace('"export_live_every_x_min"', '"export_live_every_x_min_invalid"')
    else:
        valid = False
        config_file = config_file.replace('"export_live_every_x_min"', '"export_live_every_x_min_invalid"')
    
    # 'db_maint_every_x_hour' >=1 && <= 240 (10 days)
    if isinstance(config['db_maint_every_x_hour'], (int, float)):
        if config['db_maint_every_x_hour'] < 1 or config['db_maint_every_x_hour'] > 240:
            valid = False
            config_file = config_file.replace('"db_maint_every_x_hour"', '"db_maint_every_x_hour_invalid"')
    else:
        valid = False
        config_file = config_file.replace('"db_maint_every_x_hour"', '"db_maint_every_x_hour_invalid"')
    
    # 'export_path': location (absolute or relative) of folder to save the exported listings files
    # (Listings export only performed when 'side' == 'server')
    if not Path.exists(Path(config['export_path'])):
        valid = False
        config_file = config_file.replace('"export_path"', '"export_path_invalid"')
    
    if not valid:
        # Before we reload the config to set the invalid values back to default,
        # we need to write the changes we made to the file.
        with open("tradedangerous-listener-config.json", "w") as fh:
            fh.write(config_file)
        config = load_config()


def process_messages():
    global process_ack, update_busy, dump_busy, live_busy
    
    tdb = tradedb.TradeDB(load = False)
    db = tdb.getDB()
    # Place the database into autocommit mode to avoid issues with
    # sqlite3 doing automatic transactions.
    db.isolation_level = None
    curs = db.cursor()
    
    # same SQL every time
    deleteStationItemEntry = "DELETE FROM StationItem WHERE station_id = ?"
    insertStationItemEntry = (
        "INSERT OR IGNORE INTO StationItem("
        " station_id, item_id, modified,"
        " demand_price, demand_units, demand_level,"
        " supply_price, supply_units, supply_level, from_live)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)"
    )
    updateItemAveragePrice = "UPDATE Item SET avg_price = ? WHERE item_id = ?"
    
    getOldStationInfo = (
        "SELECT name, ls_from_star,blackmarket, max_pad_size, "
        "market, shipyard, outfitting, rearm, refuel, repair, "
        "planetary, type_id from Station WHERE station_id = ?"
    )
    insertNewStation = (
        "INSERT OR IGNORE INTO Station("
        " station_id, name, system_id, ls_from_star,"
        " blackmarket, max_pad_size, market, shipyard,"
        " modified, outfitting, rearm, refuel, repair,"
        " planetary, type_id)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    removeOldStation = "DELETE FROM Station WHERE station_id = ?"
    moveStationToNewSystem = "UPDATE Station SET system_id = ? WHERE station_id = ?"
    
    # We want to perform some automatic DB maintenance when running for long periods.
    maintenance_time = time.time() + (config['db_maint_every_x_hour'] * _hour)
    
    while go:
        
        # We don't want the threads interfering with each other,
        # so pause this one if either the update checker or
        # listings exporter report that they're active.
        if update_busy or dump_busy or live_busy:
            print("Message processor acknowledging busy signal.")
            process_ack = True
            while (update_busy or dump_busy or live_busy) and go:
                time.sleep(1)
            # Just in case we caught the shutdown command while waiting.
            if not go:
                break
            process_ack = False
            print("Busy signal off, message processor resuming.")
        
        if time.time() >= maintenance_time:
            print("Performing server maintenance tasks." + str(datetime.now()))
            try:
                db_execute(db, "VACUUM")
                db_execute(db, "PRAGMA optimize")
                print("Server maintenance tasks completed. " + str(datetime.now()))
            except Exception as e:
                print("Error performing maintenance:")
                print("-----------------------------")
                print(e)
                print("-----------------------------")
            
            maintenance_time = time.time() + (config['db_maint_every_x_hour'] * _hour)
        
        # Either get the first message in the queue,
        # or go to sleep and wait if there aren't any.
        try:
            entry = q.popleft()
        except IndexError:
            time.sleep(1)
            continue
        
        # Get the station_id using the system and station names.
        system = entry.system.upper()
        station = entry.station.upper()
        market_id = entry.market_id
        modified = entry.timestamp.replace('T', ' ').replace('Z', '')
        commodities = entry.commodities
        
        #All the stations should be stored using the market_id.
        success = False
        while not success:
            try:
                exists = curs.execute("SELECT station_id FROM Station WHERE station_id = ?", (market_id,)).fetchone()
                success = True
            except sqlite3.OperationalError:
                print("Database is locked, waiting for access.", end = "\n")
                time.sleep(1)

        
        if not exists:
            station_id = station_ids.get(system + "/" + station)
            system_id = system_ids.get(system)
            if not station_id:
                # Mobile stations are stored in the dict a bit differently.
                station_id = station_ids.get("MEGASHIP/" + station)
                if station_id and system_id:
                    if config['verbose']:
                        print("Megaship station, updating system to " + system)
                    # Update the system the station is in, in case it has changed.
                    success = False
                    while not success:
                        try:
                            curs.execute("BEGIN IMMEDIATE")
                            curs.execute(moveStationToNewSystem, (system_id, station_id))
                            db.commit()
                            success = True
                        except sqlite3.IntegrityError:
                            if config['verbose']:
                                print("ERROR: Not found in Systems: " + system + "/" + station)
                            continue
                        except sqlite3.OperationalError:
                            print("Database is locked, waiting for access.", end = "\n")
                            time.sleep(1)
                else:
                    # If we can't find it by any of these means, it must be a 'new' station.
                    if config['verbose']:
                        print("Not found in Stations: " + system + "/" + station + ", inserting into DB.")
                    # Add the new Station with '?' for all unknowns.
                    success = False
                    while not success:
                        try:
                            curs.execute("BEGIN IMMEDIATE")
                            curs.execute(insertNewStation, (market_id, station, system_id, 1,
                                                            '?', '?', 'Y', '?', modified, '?',
                                                            '?', '?', '?', '?', 0))
                            db.commit()
                            success = True
                        except sqlite3.IntegrityError as e:
                            if config['verbose']:
                                print(e)
                            continue
                        except sqlite3.OperationalError:
                            print("Database is locked, waiting for access.", end = "\n")
                            time.sleep(1)
                    continue
                    station_ids[system + "/" + station] = market_id
            if station_id and (station_id != market_id):
                success = False
                while not success:
                    try:
                        curs.execute("BEGIN IMMEDIATE")
                        result = curs.execute(getOldStationInfo, (station_id,))
                        nm, ls, bm, mps, mk, sy, of, ra, rf, rp, pl, ti = result.fetchone()
                        
                        curs.execute(insertNewStation, (market_id, nm, system_id, ls, bm,
                                                        mps, mk, sy, modified,
                                                        of, ra, rf, rp, pl, ti))
                        
                        curs.execute(removeOldStation, (station_id,))
                        
                        db.commit()
                        success = True
                    except sqlite3.IntegrityError as e:
                        if config['verbose']:
                            print(e)
                        continue
                    except sqlite3.OperationalError as e:
                        print("Database is locked, waiting for access.", end = "\n")
                        print(e)
                        time.sleep(1)
        station_id = market_id
        
        start_update = datetime.now()
        
        itemList = []
        avgList = []
        for commodity in commodities:
            if commodity['sellPrice'] == 0 and commodity['buyPrice'] == 0:
                # Skip blank entries
                continue
            # Get fdev_id using commodity name from message.
            item_edid = db_name.get(commodity['name'].lower())
            if not item_edid:
                if config['verbose']:
                    print("Ignoring item: " + commodity['name'])
                continue
            # Some items, mostly recently added items, are found in db_name but not in item_ids
            # (This is entirely EDDB.io's fault.)
            item_id = item_ids.get(item_edid)
            if not item_id:
                item_id = int(item_edid)
            
            itemList.append((
                station_id, item_id, modified,
                commodity['sellPrice'], commodity['demand'],
                commodity['demandBracket'] if commodity['demandBracket'] != '' else -1,
                commodity['buyPrice'], commodity['stock'],
                commodity['stockBracket'] if commodity['stockBracket'] != '' else -1,
            ))
            # We only "need" to update the avg_price for the few items not included in
            # EDDB.io's API, but might as well do it for all of them.
            avgList.append((commodity['meanPrice'], item_id))
        
        success = False
        while not success:
            try:
                curs.execute("BEGIN IMMEDIATE")
                success = True
            except sqlite3.OperationalError:
                print("Database is locked, waiting for access.", end = "\n")
                time.sleep(1)
        
        curs.execute(deleteStationItemEntry, (station_id,))
        
        for item in itemList:
            try:
                curs.execute(insertStationItemEntry, item)
            except Exception as e:
                if config['debug']:
                    print("Error '" + str(e) + "' when inserting item:\n\t(Not in DB's Item table?) fdev_id: " + str(item[1]))
        
        for avg in avgList:
            try:
                curs.execute(updateItemAveragePrice, avg)
            except Exception as e:
                if config['debug']:
                    print("Error '" + str(e) + "' when inserting average:\n" + str(avg))
        
        success = False
        while not success:
            try:
                db.commit()
                success = True
            except sqlite3.OperationalError:
                print("Database is locked, waiting for access.", end = "\n")
                time.sleep(1)
        
        if config['verbose']:
            print("Updated " + system + "/" + station + ", station_id:'" + str(station_id) + "', from "+ entry.software + " v" + entry.version)
        else:
            print("Updated " + system + "/" + station)
    
    print("Message processor reporting shutdown.")


def fetchIter(cursor, arraysize = 1000):
    """
    An iterator that uses fetchmany to keep memory usage down
    and speed up the time to retrieve the results dramatically.
    """
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result


def export_live():
    """
    Creates a "listings-live.csv" file in "export_path" every X seconds,
    as defined in the configuration file.
    Only runs when program configured as server.
    """
    global live_ack, live_busy, process_ack, dump_busy, update_busy
    
    tdb = tradedb.TradeDB(load = False)
    db = tdb.getDB()
    listings_file = (Path(config['export_path']).resolve() / Path("listings-live.csv"))
    listings_tmp = listings_file.with_suffix(".tmp")
    print("Live listings will be exported to: \n\t" + str(listings_file))
    
    now = time.time()
    while go:
        # Wait until the time specified in the "export_live_every_x_min" config
        # before doing an export, watch for busy signal or shutdown signal
        # while waiting.
        while time.time() < now + (config['export_live_every_x_min'] * _minute):
            if not go:
                break
            if dump_busy or update_busy:
                print("Live listings exporter acknowledging busy signal.")
                live_ack = True
                while (dump_busy or update_busy) and go:
                    time.sleep(1)
                # Just in case we caught the shutdown command while waiting.
                if not go:
                    break
                live_ack = False
                print("Busy signal off, live listings exporter resuming.")
                now = time.time()
            
            time.sleep(1)
        
        now = time.time()
        # We may be here because we broke out of the waiting loop,
        # so we need to see if we lost go and quit the main loop if so.
        if not go:
            break
        
        start = datetime.now()
        
        print("Live listings exporter sending busy signal. " + str(start))
        live_busy = True
        # We don't need to wait for acknowledgement from the dump exporter,
        # because it waits for one from this, and this won't acknowledge
        # until it's finished exporting.
        while not (process_ack):
            if not go:
                break
        print("Busy signal acknowledged, getting live listings for export.")
        try:
            cursor = fetchIter(db_execute(db, "SELECT * FROM StationItem WHERE from_live = 1 ORDER BY station_id, item_id"))
            results = list(cursor)
        except sqlite3.DatabaseError as e:
            print(e)
            live_busy = False
            continue
        except AttributeError as e:
            print("Got Attribute error trying to fetch StationItems: " + str(e))
            print(cursor)
            live_busy = False
            continue
        
        print("Exporting 'listings-live.csv'. (Got listings in " + str(datetime.now() - start) + ")")
        with open(str(listings_tmp), "w") as f:
            f.write("id,station_id,commodity_id,supply,supply_bracket,buy_price,sell_price,demand,demand_bracket,collected_at\n")
            lineNo = 1
            for result in results:
                # If we lose go during export, we need to abort.
                if not go:
                    break
                station_id = str(result[0])
                commodity_id = str(result[1])
                sell_price = str(result[2])
                demand = str(result[3])
                demand_bracket = str(result[4])
                buy_price = str(result[5])
                supply = str(result[6])
                supply_bracket = str(result[7])
                collected_at = str(timegm(datetime.strptime(result[8].split('.')[0], '%Y-%m-%d %H:%M:%S').timetuple()))
                listing = station_id + "," + commodity_id + "," \
                         +supply + "," + supply_bracket + "," + buy_price + "," \
                         +sell_price + "," + demand + "," + demand_bracket + "," \
                         +collected_at
                f.write(str(lineNo) + "," + listing + "\n")
                lineNo += 1
        
        del results
        if config['verbose']:
            print('Live listings exporter finished with database, releasing lock.')
        live_busy = False
        
        # If we aborted the export because we lost go, listings_tmp is broken and useless, so delete it.
        if not go:
            listings_tmp.unlink()
            print("Export aborted, received shutdown signal.")
            break
        
        while listings_file.exists():
            try:
                listings_file.unlink()
            except:
                time.sleep(1)
        listings_tmp.rename(listings_file)
        print("Export completed in " + str(datetime.now() - start))
    
    print("Live listings exporter reporting shutdown.")


def export_dump():
    """
    Creates a "listings-live.csv" file in "export_path" every X seconds,
    as defined in the configuration file.
    Only runs when program configured as server.
    """
    global dump_busy, process_ack, live_ack
    
    tdb = tradedb.TradeDB(load = False)
    db = tdb.getDB()
    listings_file = (Path(config['export_path']).resolve() / Path("listings.csv"))
    listings_tmp = listings_file.with_suffix(".tmp")
    print("Listings will be exported to: \n\t" + str(listings_file))
    
    start = datetime.now()
    
    print("Listings exporter sending busy signal. " + str(start))
    dump_busy = True
    
    while not (process_ack and live_ack):
        if not go:
            break
        time.sleep(1)
    
    print("Busy signal acknowledged, getting listings for export.")
    success = False
    while not success:
        try:
            # Reset the live (i.e. since the last dump) flag for all StationItems
            db_execute(db, "UPDATE StationItem SET from_live = 0")
            db.commit()
            cursor = fetchIter(db_execute(db, "SELECT * FROM StationItem ORDER BY station_id, item_id"))
            results = list(cursor)
            success = True
        except sqlite3.OperationalError:
            print("(commit) Database is locked, waiting for access.", end = "\r")
            time.sleep(1)
        except sqlite3.DatabaseError as e:
            print("Aborting export:")
            print(e)
            dump_busy = False
            return
        except AttributeError as e:
            print("Aborting export:")
            print("Got Attribute error trying to fetch StationItems: " + str(e))
            print(cursor)
            dump_busy = False
            return
    
    print("Exporting 'listings.csv'. (Got listings in " + str(datetime.now() - start) + ")")
    with open(str(listings_tmp), "w") as f:
        f.write("id,station_id,commodity_id,supply,supply_bracket,buy_price,sell_price,demand,demand_bracket,collected_at\n")
        lineNo = 1
        for result in results:
            # If we lose go during export, we need to abort.
            if not go:
                break
            station_id = str(result[0])
            commodity_id = str(result[1])
            sell_price = str(result[2])
            demand = str(result[3])
            demand_bracket = str(result[4])
            buy_price = str(result[5])
            supply = str(result[6])
            supply_bracket = str(result[7])
            collected_at = str(timegm(datetime.strptime(result[8].split('.')[0], '%Y-%m-%d %H:%M:%S').timetuple()))
            listing = station_id + "," + commodity_id + "," \
                        + supply + "," + supply_bracket + "," + buy_price + "," \
                        + sell_price + "," + demand + "," + demand_bracket + "," \
                        + collected_at
            f.write(str(lineNo) + "," + listing + "\n")
            lineNo += 1
    
    del results
    if config['verbose']:
        print('Listings exporter finished with database, releasing lock.')
    dump_busy = False
    
    # If we aborted the export because we lost go, listings_tmp is broken and useless, so delete it.
    if not go:
        listings_tmp.unlink()
        print("Export aborted, received shutdown signal.")
    else:
        while listings_file.exists():
            try:
                listings_file.unlink()
            except:
                time.sleep(1)
        listings_tmp.rename(listings_file)
        print("Export completed in " + str(datetime.now() - start))

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
    
    with open(str(dataPath / Path("Item.csv")), "r", encoding="utf8") as fh:
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
    with open(str(dataPath / Path("System.csv")), "r", encoding="utf8") as fh:
        systems = csv.DictReader(fh, quotechar = "'")
        for system in systems:
            system_names[int(system['unq:system_id'])] = system['name'].upper()
            system_ids[system['name'].upper()] = int(system['unq:system_id'])
    station_ids = dict()
    with open(str(dataPath / Path("Station.csv")), "r", encoding="utf8") as fh:
        stations = csv.DictReader(fh, quotechar = "'")
        for station in stations:
            # Mobile stations can move between systems. The mobile stations
            # have the following data in their entry in stations.jsonl:
            # "type_id":19,"type":"Megaship"
            # Except for that one Orbis station.
            # And now Fleet Carriers, they're type 24.
            if int(station['type_id']) == 19 or int(station['type_id']) == 24 or int(station['unq:station_id']) == 42041:
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
config = load_config()
validate_config()

# Make sure the export folder exists
try:
    Path(config['export_path']).mkdir()
except FileExistsError:
    pass


# get and process trade data messages from EDDN
listener_thread = threading.Thread(target = get_messages)
process_thread = threading.Thread(target = process_messages)

# (client) check if server has updated
update_thread = threading.Thread(target = check_update)

# (server) perform updates according to config settings
# market data updated since last dump
live_thread = threading.Thread(target = export_live)
# performs dump, resetting all live flags
# dump_thread = threading.Thread(target = export_dump)

tdb = tradedb.TradeDB(load = False)

dataPath = os.environ.get('TD_CSV') or Path(tradeenv.TradeEnv().csvDir).resolve()
#eddbPath = plugins.eddblink_plug.ImportPlugin(tdb, tradeenv.TradeEnv()).dataPath.resolve()
try:
    db_name, item_ids, system_ids, station_ids = update_dicts()
except:
    pass

print("Press CTRL-C at any time to quit gracefully.")
try:
    listener_thread.start()
    
    update_thread.start()
    # Give the update checker enough time to see if an update is needed,
    # before starting the message processor and listings exporter.
    time.sleep(5)
    
    process_thread.start()
    
    if config['side'] == 'server':
        # dump_thread.start()
        time.sleep(1)
        live_thread.start()
    else:
        live_ack = True
    
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("CTRL-C detected, stopping.")
    print("Please wait for all processes to report they are finished, in case they are currently active.")
    go = False
