# Copyright (C) Oliver 'kfsone' Smith <oliver@kfs.org> 2015
#
# Conditional permission to copy, modify, refactor or use this
# code is granted so long as attribution to the original author
# is included.

from __future__ import generators
import time
import zlib
import json
from collections import defaultdict
from distutils.version import LooseVersion

import zmq

from .customtypes import MarketPrice


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
        env,
        config,
        zmqContext=None,
        minBatchTime=36.,       # seconds
        maxBatchTime=60.,       # seconds
        reconnectTimeout=30.,  # seconds
        burstLimit=500,
    ):
        assert burstLimit > 0

        self.env = env
        self.config = config

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
        while self.env.go:
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
                    whitelist_match = list(filter(lambda x: x.get('software').lower() == software.lower(), self.config['whitelist']))
                    # Upload software not on whitelist is ignored.
                    if len(whitelist_match) == 0:
                        if self.config['debug']:
                            with self.env.debugPath.open('a', encoding = "utf-8") as fh:
                                fh.write(system + "/" + station + " rejected with:" + software + swVersion +"\n")
                        continue
                    # Upload software with version less than the defined minimum is ignored.
                    if whitelist_match[0].get("minversion"):
                        if LooseVersion(swVersion) < LooseVersion(whitelist_match[0].get("minversion")):
                            if self.config['debug']:
                                with self.env.debugPath.open('a', encoding = "utf-8") as fh:
                                    fh.write(system + "/" + station + " rejected with:" + software + swVersion +"\n")
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