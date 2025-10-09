# TradeDangerous-listener
A listener for TradeDangerous, designed to work in conjunction with Trade Dangerous (https://github.com/eyeonus/Trade-Dangerous).

Python version 3.10+ required.

# Notes
- This program requires TD to be installed on the same machine as this in order to work.

- This program will automatically run the spansh import plugin, so there's no /need/ for you to ever do it yourself.

- If TD was not installed via pip, this program must be in the same folder as TD's "trade.py". IF TD was installed via pip, this file can be anywhere desired.

- EDDN is a 0MQ network, so this program uses the 'zmq' module. You may need to install zmq before this will run by running 'pip install pyzmq' in a Command Prompt / Terminal.

# Features
- Listens to the Elite Dangerous Data Network (EDDN) for market updates from whitelisted sources and updates TD's database. The whitelist can be configured, default allowed clients are E:D Market Connector, EDDiscovery, and EDDI (These three (especially EDMC)  account for ~97% of all messages on the EDDN).

- Automatically checks for updates from [spansh](https://spansh.co.uk/dumps) (server-side) and runs the spansh plugin when it detects one. Delay between checks is 1 hour by default, can be changed in the configuration file, under the setting "check_update_every_x_sec". **See the README.md for EDDBlink plugin for more information on available run options.**

- If configured as server, will automatically export the currently stored prices listings from TD's database in the file "listings-live.csv", which will be located in the folder named in the "export_path" setting, which defaults to "\<TD install\>/data/eddb". The duration between subsequent exports is 5 minutes by default, and can be configured in the configuration file, under the setting "export_every_x_sec".

# Running
Running the program is simple: open a Command Prompt (Windows) / Terminal (Linux/OSX), go to the folder this program is located at, and type 'python eddblink_listener.py". You'll know you did it right when you see "Press CTRL-C at any time to quit gracefully." Once you see that, you can simply minimize the window and let it do its thing.

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
    "verbose": true,
    "plugin_options": "all,skipvend,force",
    "check_delay_in_sec": 3600,
    "export_every_x_sec": 300,
    "export_path": "./data/eddb",
    "whitelist": [
        {
            "software": "E:D Market Connector [Windows]"
        },
        {
            "software": "E:D Market Connector [Mac OS]"
        },
        {
            "software": "E:D Market Connector [Linux]"
        },
        {
            "software": "EDDiscovery"
        },
        {
            "software": "eddi",
            "minversion": "2.2"
        }
    ]
}
```
A note on the whitelist:
- Software entries /without/ a minversion mean messages from any version of that program will be accepted.
- Software entries /with/ a minversion mean messages from a version lower than minversion will not be accepted, but those >= minversion will.

If you wish, you may copy this as "tradedangerous-listener-config.json" in the same folder as the program itself and make any changes to it before running the program, in order to avoid having to run it, waiting for the default config file to be created, stopping it, making the changes, and then running the program again.

# How it works

The TradeDangerous-listener program runs either three or four separate threads:
1) The actual listener, which is started as soon as the startup process is complete.
This is the thread that listens for messages and adds them to the queue.

2) The update checker, which is started right after the listener.
This is the method that runs the spansh plugin when it detects an update to the spansh dump has occurred.
Before it starts the updates, it signals that it needs the DB: "Spansh update available, waiting for busy signal acknowledgement before proceeding.".
It then waits for the listings exporter and message processor to signal they got the signal and are waiting for the update checker to complete, and then runs the update.
When it's finished, it signals completion to the exporter and processor, and they both unpause.

A note on the updating:
The spansh plugin only downloads the dump and parses it into a .prices file, the update method then locks the DB and performs the update. The DB is not locked until the update is ready, so normal operation continues while the spansh plugin does its thing.

3) The listings exporter, which is started 5 seconds after the update checker in order to give the checker enough time to check if it needs to update immediately.
This is not run when the listener is running as a client. In that case, it "permanently" (i.e. as long as the program is running) turns on the busy signal acknowledgement and shuts itself down.
When it's not currently active and gets a busy signal from the update checker, it acknowledges it, "Listings exporter acknowledging busy signal.", and pauses itself until it gets the no-longer-busy signal, "Busy signal off, listings exporter resuming."
When it begins exporting the listings, it sends a signal to the message processor that it needs the DB, "Listings exporter sending busy signal."
It doesn't need to send one to the update checker, because the update checker will wait for acknowledgement from the exporter, and the exporter won't give that until it's done exporting.
Once it gets acknowledgement from the message processor, it grabs all the listings that have been updated since the last dump, i.e., all the listings that have a "from_live" value of 1.
Once it's gotten them, it relinquishes the DB and turns off its busy signal, allowing the message processor to resume.
It then exports all the listings it got to the live listings file.

4) The message processor, which is started 5 seconds after the update checker, immediately after the listings exporter.
This is the method that actually puts the messages from the EDDN into the database.
If it receives a busy signal from either the update checker or the listings exporter, it pauses, "Message processor acknowledging busy signal."
When the busy signal(s) are turned off, it resumes from where it left off, "Busy signal off, message processor resuming."
When it is active, it pulls the first message from the queue being built up by the listener, does some processing, and inserts it into the DB, setting the "from_live" flag for each entry it inserts to 1.
If there are still messages in the queue, it immediately proceeds to process the next message.
If there are no messages in the queue remaining, it tells the DB to commit the changes it has made.

