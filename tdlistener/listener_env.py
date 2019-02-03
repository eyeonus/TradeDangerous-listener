from collections import deque
from pathlib import Path
from urllib import request
import csv
import codecs

from tradedangerous import tradeenv

class ListenerEnv:
    def __init__(self):
        self.go = True
        self.q = deque()
        self.update_busy = False
        self.process_ack = False
        self.export_ack = False
        self.export_busy = False
        self.dataPath = Path(tradeenv.TradeEnv().dataDir).resolve()
        self.eddbPath = None
        self.config = None


        self.db_name = None
        self.item_ids = None
        self.system_ids = None
        self.station_ids = None

    def refresh_dicts(self):
        del self.db_name, self.item_ids, self.system_ids, self.station_ids
        self.db_name, self.item_ids, self.system_ids, self.station_ids = self._update_dicts()


    def _update_dicts(self):
        # We'll use this to get the fdev_id from the 'symbol', AKA commodity['name'].lower()
        db_name = dict()
        edmc_source = 'https://raw.githubusercontent.com/Marginal/EDMarketConnector/master/commodity.csv'
        edmc_csv = request.urlopen(edmc_source)
        edmc_dict = csv.DictReader(codecs.iterdecode(edmc_csv, 'utf-8'))
        for line in iter(edmc_dict):
            db_name[line['symbol'].lower()] = line['id']

        # We'll use this to get the item_id from the fdev_id because it's faster than a database lookup.
        item_ids = dict()
        with open(str(self.dataPath / Path("Item.csv")), "r") as fh:
            items = csv.DictReader(fh, quotechar="'")
            # Older versions of TD don't have fdev_id as a unique key, newer versions do.
            if 'fdev_id' in next(iter(items)).keys():
                iid_key = 'fdev_id'
            else:
                iid_key = 'unq:fdev_id'
            fh.seek(0)
            next(iter(items))
            for item in items:
                item_ids[item[iid_key]] =  int(item['unq:item_id'])

        # We're using these for the same reason.
        system_names = dict()
        system_ids = dict()
        with open(str(self.dataPath / Path("System.csv")), "r") as fh:
            systems = csv.DictReader(fh, quotechar="'")
            for system in systems:
                system_names[int(system['unq:system_id'])] = system['name'].upper()
                system_ids[system['name'].upper()] = int(system['unq:system_id'])
        station_ids = dict()
        with open(str(self.dataPath / Path("Station.csv")), "r") as fh:
            stations = csv.DictReader(fh, quotechar="'")
            for station in stations:
                # Mobile stations can move between systems. The mobile stations
                # have the following data in their entry in stations.jsonl:
                # "type_id":19,"type":"Megaship"
                # Except for that one Orbis station.
                if int(station['type_id']) == 19 or int(station['unq:station_id']) == 42041:
                    full_name = "MEGASHIP"
                else:
                    full_name = system_names[int(station['system_id@System.system_id'])]
                full_name += "/" + station['name'].upper()
                station_ids[full_name] = int(station['unq:station_id'])

        del system_names

        return db_name, item_ids, system_ids, station_ids