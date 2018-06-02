import json
import time
import zlib
import zmq
import threading
import trade
import tradedb
import tradeenv
import transfers
import urllib
import datetime
import sqlite3
import csv
import codecs

from calendar import timegm
from pathlib import Path
from collections import defaultdict, namedtuple, deque
from distutils.version import LooseVersion
from macpath import curdir


# Copyright (C) Oliver 'kfsone' Smith <oliver@kfs.org> 2015
#
# Conditional permission to copy, modify, refactor or use this
# code is granted so long as attribution to the original author
# is included.
class MarketPrice(namedtuple('MarketPrice', [
        'system',
        'station',
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
        zmqContext=None,
        minBatchTime=15,       # seconds
        maxBatchTime=45,       # seconds
        reconnectTimeout=30.,  # seconds
        burstLimit=500,
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

        timeout = (nextCutoff - now) * 1000     # milliseconds

        # Wait for an event
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
                        continue
                    # Upload software with version less than the defined minimum is ignored. 
                    if whitelist_match[0].get("minversion"):
                        if LooseVersion(swVersion) < LooseVersion(whitelist_match[0].get("minversion")):
                            continue
                    # We've received real data.

                    # Normalize timestamps
                    timestamp = timestamp.replace("T"," ").replace("+00:00","")

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
                        system, station, commodities,
                        timestamp,
                        uploader, software, swVersion,
                    )

                # For the edge-case where we wait 4.999 seconds and then
                # get a burst of data: stick around a little longer.
                if bursts >= self.burstLimit:
                    softCutoff = min(softCutoff, time.time() + 0.5)


                for entry in batch.values():
                    queue.append(entry[0])
        print("Shutting down listener.")
        self.disconnect()
        
# End of 'kfsone' code.

# We do this because the Listener object must be in the same thread that's running get_batch().
def get_messages():
    listener = Listener()
    listener.get_batch(q)

def check_update():
    global update_busy
    m, s = divmod(config['check_delay_in_sec'], 60)
    h, m = divmod(m, 60)
    next_check = ""
    if h > 0:
        next_check = str(h) + " hour"
        if h > 1:
            next_check += "s"
        if m > 0 or s > 0:
            next_check += ", "
    if m > 0:
        next_check += str(m) + " minute"
        if m > 1:
            next_check += "s"
        if s > 0:
            next_check += ", "                    
    if s > 0:
        next_check += str(s) + " second"
        if s > 1:
            next_check += "s"

    while go:
        now = time.time()
        commodities_path = Path('eddb') / Path('commodities.json')
        BASE_URL = "http://elite.ripz.org/files/"
        FALLBACK_URL = "https://eddb.io/archive/v5/"
        COMMODITIES = "commodities.json"
    
        if config['side'] == 'client':
            try:
                urllib.request.urlopen(BASE_URL + COMMODITIES)
                url = BASE_URL + COMMODITIES
            except:
                url = FALLBACK_URL + COMMODITIES
        else:
            url = FALLBACK_URL + COMMODITIES
        dumpModded = 0
        localModded = 0

        Months = {'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12}
        dDL = urllib.request.urlopen(url).getheader("Last-Modified").split(' ')
        dTL = dDL[4].split(':')

        dumpDT = datetime.datetime(int(dDL[3]), Months[dDL[2]], int(dDL[1]),\
            hour=int(dTL[0]), minute=int(dTL[1]), second=int(dTL[2]),\
            tzinfo=datetime.timezone.utc)
        dumpModded = timegm(dumpDT.timetuple())

        if Path.exists(dataPath / commodities_path):
            localModded = (dataPath / commodities_path).stat().st_mtime
        #Trigger daily EDDB update if the dumps have updated since last run.
        #Otherwise, go to sleep for an hour before checking again.
        if localModded < dumpModded:
            # TD will fail with an error if the database is in use while it's trying
            # to do its thing, so we need to make sure that neither of the database
            # editing methods are doing anything before running.
            update_busy = True
            print("EDDB update available, waiting for busy signal acknowledgement before proceeding.")
            while not (process_ack and export_ack):
                pass
            print("Busy signal acknowledged, performing EDDB dump update.")
            trade.main(('trade.py','import','-P','eddblink','-O','all,fallback' if config['side'] == "server" else 'all'))
            print("Update complete, turning off busy signal.")
            update_busy = False
        else:
            print("No update, checking again in "+ next_check + ".")
            while time.time() < now + config['check_delay_in_sec']:
                if not go:
                    print("Shutting down update checker.")
                    break
                
def load_config():
    if not Path.exists(Path("eddblink-listener-config.json")):
        print("Writing default configuration.")
        with open("eddblink-listener-config.json", "w") as config_file:
            config_file.writelines(['{\n',
                                    '    "check_delay_in_sec" : 3600,\n',
                                    '    "export_every_x_sec" : 300,\n',
                                    '    "side": "client",\n',
                                    '    "whitelist":\n',
                                    '    [\n',
                                    '        { "software":"E:D Market Connector [Windows]" },\n',
                                    '        { "software":"E:D Market Connector [Mac OS]" },\n',
                                    '        { "software":"E:D Market Connector [Linux]" },\n',
                                    '        { "software":"EDDiscovery" },\n',
                                    '        { "software":"eddi",\n',
                                    '            "minversion":"2.2" }\n',
                                    '    ]\n',
                                    '}\n'])
    with open("eddblink-listener-config.json", "rU") as fh:
        config = json.load(fh)
    return config

def process_messages():
    global process_ack
    tdb = tradedb.TradeDB(load=False)
    db = tdb.getDB()

    while go:
        if update_busy:
            print("Message processor acknowledging busy signal.")
            process_ack = True
            while update_busy:
                pass
            process_ack = False
            print("Busy signal off, message processor resuming.")

        try:
            entry = q.popleft()
        except IndexError:
            time.sleep(1)
            continue
        system = entry.system
        station = entry.station
        modified = entry.timestamp.replace('T',' ').replace('Z','')
        commodities= entry.commodities

        result = db.execute("""SELECT station_id FROM Station WHERE name = ? 
                                    AND system_id = (SELECT System.system_id FROM System WHERE System.name = ?)""",\
                                (station, system)).fetchone()
        if result:
            station_id = result[0]
        else:
            print("ERROR: Not found in Station table: " + system + "/" + station)
            continue
        
        print("Messages waiting: " + str(len(q)))
        print(str(datetime.datetime.now()) + " - Updating " + system + "/" + station + " with data updated at: " + modified + " UTC")
        for commodity in commodities:
            #Get item_id using commodity name from message.
            name = commodity['name'].lower()
            try:
                result = db.execute("SELECT item_id FROM Item WHERE name LIKE '%" + db_name[name] + "%'").fetchone()
                if result:
                    item_id = result[0]
                else:
                    print("ERROR: Not found in Item table: " + db_name[name])
                    continue
            except keyError:
                print("ERROR: Commodity not found: " + commodity['name'])
            demand_price = commodity['sellPrice']
            demand_units = commodity['demand']
            demand_level = commodity['demandBracket'] if commodity['demandBracket'] != '' else -1
            supply_price = commodity['buyPrice']
            supply_units = commodity['stock']
            supply_level = commodity['stockBracket'] if commodity['stockBracket'] != '' else -1            
            result = db.execute("SELECT station_id FROM StationItem WHERE station_id = ? AND item_id = ?", (station_id, item_id)).fetchone()
            if result:
                try:
                    db.execute("""UPDATE StationItem
                        SET modified = ?,
                         demand_price = ?, demand_units = ?, demand_level = ?,
                         supply_price = ?, supply_units = ?, supply_level = ?
                        WHERE station_id = ? AND item_id = ?""",
                        (modified, 
                         demand_price, demand_units, demand_level, 
                         supply_price, supply_units, supply_level,
                        station_id, item_id))
                except sqlite3.IntegrityError:
                    pass
            else:
                try:
                    db.execute("""INSERT INTO StationItem
                        (station_id, item_id, modified,
                         demand_price, demand_units, demand_level,
                         supply_price, supply_units, supply_level)
                        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? )""",
                        (station_id, item_id, modified,
                        demand_price, demand_units, demand_level,
                        supply_price, supply_units, supply_level))
                except sqlite3.IntegrityError:
                    pass

            success = False
            while not success:
                try:
                    db.commit()
                except sqlite3.DatabaseError:
                    time.sleep(1)
                    continue
                success = True
        print(str(datetime.datetime.now()) + " - Finished updating market data for " + system + "/" + station)
        
    print("Shutting down message processor.")

def export_listings():
    """
    Creates a "listings.csv" file in <TD install location>\data\eddb every X seconds as defined in the configuration file.
    For server use only.
    """
    global export_ack
    if config['side'] == 'server':
        tdb = tradedb.TradeDB(load=False)
        db = tdb.getDB()
        listings_file = dataPath / Path("eddb") / Path("listings.csv")

        while go:
        
            now = time.time()
            try:
                cur = db.execute("SELECT * FROM StationItem ORDER BY station_id, item_id")
            except sqlite3.DatabaseError:
                continue
        
            print("Exporting 'listings.csv'. " + str(datetime.datetime.now()))
            with open(str(listings_file), "w") as f:
                f.write("id,station_id,commodity_id,supply,supply_bracket,buy_price,sell_price,demand,demand_bracket,collected_at\n")
                lineNo = 1
                for result in cur:
                    station_id = str(result[0])
                    commodity_id = str(result[1])
                    sell_price = str(result[2])
                    demand = str(result[3])
                    demand_bracket = str(result[4])
                    buy_price = str(result[5])
                    supply = str(result[6])
                    supply_bracket = str(result[7])
                    collected_at = str(timegm(datetime.datetime.strptime(result[8],'%Y-%m-%d %H:%M:%S').timetuple()))
                    line = str(lineNo)
                    for insert in (station_id,commodity_id,supply,supply_bracket,buy_price,sell_price,demand,demand_bracket,collected_at):
                        line += "," + insert
                    f.write(line + "\n")
                    lineNo += 1
            print("Export complete. " + str(datetime.datetime.now()))

            while time.time() < now + config['export_every_x_sec']:
                if update_busy:
                    print("Listings exporter acknowledging busy signal.")
                    export_ack = True
                    while update_busy:
                        pass
                    export_ack = False
                    print("Busy signal off, listings exporter resuming.")

                if not go:
                    print("Shutting down Listings exporter.")
                    break
    else:
        export_ack = True


go = True
q = deque()
config = load_config()

update_busy = False
process_ack = False
export_ack = False

# We'll use this to convert the name of the items given in the EDDN messages into the names TD uses.
db_name = dict()
edmc_source = 'https://raw.githubusercontent.com/Marginal/EDMarketConnector/master/commodity.csv'
edmc_csv = urllib.request.urlopen(edmc_source)
edmc_dict = csv.DictReader(codecs.iterdecode(edmc_csv, 'utf-8'))
for line in iter(edmc_dict):
    db_name[line['symbol'].lower()] = line['name']
#A few of these don't match between EDMC and EDDB, so we fix them individually.
db_name['airelics'] = 'Ai Relics'
db_name['drones'] = 'Limpet'
db_name['liquidoxygen'] = 'Liquid Oxygen'
db_name['methanolmonohydratecrystals'] = 'Methanol Monohydrate'
db_name['coolinghoses'] = 'Micro-Weave Cooling Hoses'
db_name['nonlethalweapons'] = 'Non-lethal Weapons'
db_name['sap8corecontainer'] = 'Sap 8 Core Container'
db_name['trinketsoffortune'] = 'Trinkets Of Hidden Fortune'
db_name['wreckagecomponents'] = 'Salvageable Wreckage'

dataPath = Path(tradeenv.TradeEnv().dataDir).resolve()

print("Press CTRL-C at any time to quit gracefully.")
try:
    update_thread = threading.Thread(target=check_update)
    listener_thread = threading.Thread(target=get_messages)
    process_thread = threading.Thread(target=process_messages)
    export_thread = threading.Thread(target=export_listings)
    
    export_thread.start()
    update_thread.start()
    listener_thread.start()
    process_thread.start()
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("CTRL-C detected, stopping.")
    go = False