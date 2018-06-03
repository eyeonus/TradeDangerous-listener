# EDDBlink-listener
An EDDN listener, designed to work in conjunction with the EDDBlink plugin ( https://github.com/eyeonus/EDDBlink ) for Trade Dangerous, and of course, TD itself.

# Notes
This program requires both TD and EDDBlink to be installed on the same machine as this in order to work.

At this point in time, this program must be in the same folder as TD's "trade.py".

Once I've got all the standard features taken care of, I may add the ability to install this anywhere, with a prompt to get the path to trade.py so it'll still work, if people really want that.

# Standard features
- Listens to the Elite Dangerous Data Network (EDDN) for market updates from whitelisted sources and updates TD's database. The whitelist can be configured, default allowed clients are E:D Market Connector, EDDiscovery, and EDDI (These three (especially EDMC)  account for ~97% of all messages on the EDDN).

- Automatically checks for updates from EDDB.io (server-side) or Tromador's mirror (client-side) and runs the EDDBlink plugin with the options 'all,skipvend,force' when it detects one. (Instances of this running as server will additionally run with the 'fallback' option to download the updates directly from EDDB.io instead of the mirror. Since the only server expected to be running is Tromador's, this makes perfect sense.) Delay between checks is 1 hour by default, can be changed in the configuration file, under the setting "check_delay_in_sec".

- If configured as server, will automatically export the currently stored prices listings from TD's database in the file "listings.csv", which will be located in the folder "<TD install>\data\eddb". The duration between subsequent exports is 5 minutes by default, can be configured in the configuration file, under the setting "export_every_x_sec".

# Running
The configuration file is automatically created with default settings on first run. If you wish to, you may make changes to the configuration, doing so will require stopping and restarting the program before the changes take effect.

To run this for yourself, nothing needs to be done.

To run as a server, change the "side" value in the configuration file to "server".

# Configuration file
The configuration file, by default, looks like the following:

```
{
    "check_delay_in_sec" : 3600,
    "export_every_x_sec" : 300,
    "side": "client",
    "whitelist":
    [
        { "software":"E:D Market Connector [Windows]" },
        { "software":"E:D Market Connector [Mac OS]" },
        { "software":"E:D Market Connector [Linux]" },
        { "software":"EDDiscovery" },
        { "software":"eddi",
            "minversion":"2.2" }
    ]
}
```
A note on the whitelist:
- Software entries /without/ a minversion mean messages from any version of that program will be accepted.
- Software entries /with/ a minversion mean messages from a version lower than minversion will not be accepted, but those >= minversion will.

If you wish, you may copy this as "eddblink-listener-config.json" in the same folder as the program itself and make any changes to it before running the program, in order to avoid having to run it, waiting for the default config file to be created, stopping it, making the changes, and then running the program again.