# EDDBlink-listener
An EDDN listener, designed to work in conjunction with the EDDBlink plugin for Trade Dangerous, and of course, TD itself.

# Notes
This program requires both TD and EDDBlink to be installed on the same machine as this in order to work.

At this point in time, this program must be in the same folder as TD's "trade.py".

Once I've got all the standard features taken care of, I may add the ability to install this anywhere, with a prompt to get the path to trade.py so it'll still work.

To run this for yourself, nothing needs to be done. The configuration file will be generated automatically at first launch. If you wish to, you may make changes to the configuration, doing so will require stopping and restarting the program before the changes take effect.

To run as a server, change the "side" value in the configuration file to "server".
Doing so will make the EDDB dump update checker look at EDDB.io directly, rather than  Tromador's mirror (elite.ripz.org), as well as turning on the prices exporter, which will cause the file "listings.csv" to be created from the TD DB in the "data\eddb\" folder periodically, determined by the "export_every_x_sec" setting in the configuration file.