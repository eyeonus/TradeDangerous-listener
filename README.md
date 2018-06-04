# EDDBlink-listener
An EDDN listener, designed to work in conjunction with the EDDBlink plugin ( https://github.com/eyeonus/EDDBlink ) for Trade Dangerous, and of course, TD itself.

# Notes
- This program requires both TD and EDDBlink to be installed on the same machine as this in order to work.

- At this point in time, this program must be in the same folder as TD's "trade.py".

- EDDN is a 0MQ network, so this program uses the 'zmq' module. You may need to install zmq before this will run by running 'pip install zmq' in a Command Prompt / Terminal.

# Features
- Listens to the Elite Dangerous Data Network (EDDN) for market updates from whitelisted sources and updates TD's database. The whitelist can be configured, default allowed clients are E:D Market Connector, EDDiscovery, and EDDI (These three (especially EDMC)  account for ~97% of all messages on the EDDN).

- Automatically checks for updates from EDDB.io (server-side) or Tromador's mirror (client-side) and runs the EDDBlink plugin with the options 'all,skipvend,force' when it detects one. (Instances of this running as server will additionally run with the 'fallback' option to download the updates directly from EDDB.io instead of the mirror. Since the only server expected to be running is Tromador's, this makes perfect sense.) Delay between checks is 1 hour by default, can be changed in the configuration file, under the setting "check_delay_in_sec".

- If configured as server, will automatically export the currently stored prices listings from TD's database in the file "listings.csv", which will be located in the folder named in the "export_path" setting, which defaults to "<TD install>\data\eddb". The duration between subsequent exports is 5 minutes by default, and can be configured in the configuration file, under the setting "export_every_x_sec".

# Running
Running the program is simple: open a Command Prompt (Windows) / Terminal (Linux/OSX), go to the folder this program is located at, and type 'python eddblink-listener.py". You'll know you did it right when you see "Press CTRL-C at any time to quit gracefully." Once you see that, you can simply minimize the window and let it do its thing.

To close the program in a way that will definitely not muck up the database, press CTRL-C. This will send a "keyboard interrupt", also known as SIGINT, to the program, letting it know you want it to stop, and it will shut down all its processes cleanly.
(If you're on a Mac and CTRL-C doesn't work, try '⌘-.' (Command-period).)

Closing the program any other way, such as closing the terminal window, can potentially lead to a corrupt database.

# Configuration file
The configuration file is automatically created with default settings on first run. If you wish to, you may make changes to the configuration, doing so will require stopping and restarting the program before the changes take effect.

To run this for yourself, nothing needs to be done to the configuration file.

To run as a server, change the "side" setting from "client" to "server".

The configuration file, by default, looks like the following:

```
{
    "side": "client",
    "check_delay_in_sec" : 3600,
    "export_every_x_sec" : 300,
    "export_path": "./data/eddb",
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