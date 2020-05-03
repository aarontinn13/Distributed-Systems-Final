Distributed System Final Project
================================

Authors
--------
Aaron Tinn (atinn@uchicago.edu) and Winshen Liu (winshen@uchicago.edu)

    Ubuntu 18.04 / MacOS High Sierra version 10.13.6
    Python 3.7

Work description
-----------------
We partnered closely on all aspects of the project and believe our work was evenly distributed.
Details on how we broke down the work are below.

*Part 1: Key-value store*

Aaron implemented the centralized key-value store. Winshen tested and refactored it.

*Part 2: Leader election*

To enable leader election, Aaron first threaded the server to allow for several
parallel processes. He then added a forwarder to manage messaging across servers.

To implement leader election, Aaron wrote the election timeouts, heartbeat functionality,
candidates' request for votes, and followers sending votes. Aaron also implemented split
vote functionality so that elections restart if two servers become
candidates simultaneously.

Winshen added attributes for tracking server state and added client redirect
functionality so the client is automatically redirected to the leader. She also
helped test Aaron's leader election implementation and extended it to prevent
candidates with outdated logs from receiving votes and to enable log replication.

*Part 3: Log replication*

Winshen implemented leader calls to append log entries, replicate those entries,
commit majority-approved entries to state machines, and replicate commitments
once quorum is in agreement. Aaron tested this functionality thoroughly and refactored it.

*Report:*

Aaron drafted a thorough outline (~2 pages) and Winshen completed the sections
for the final draft (bringing the report to 5 pages).

Specs
------
    - Ubuntu 18.04 / MacOS High Sierra
    - Python 3.7

Files
-----
    forwarder_device.py - device that will mediate messages to and from servers
    client.py           - client that will PUT and GET to/from the database
    server.py           - server(s) that will store the database
    ubuntu_server.sh    - executable that will launch the servers (Ubuntu 18.04)
    mac_server.sh       - executable that will launch the servers (MAC OS X)

How to run
-----------
In Ubuntu:

    # launch the forwarder
    python forwarder_device.py

    # launch the servers, you can use the script below or open 5 terminals and run python server.py 500[0-4]
    ./ubuntu_server.sh (for ubuntu)

    # launch the forwarder device
    python forwarder_device.py

    # follow the prompt

    # launch the client in a separate command line window
    python client.py

    # follow the prompts

In Mac:

    # launch the forwarder
    python forwarder_device.py

    # launch each server in a new window by opening 5 terminals and running python server.py 500[0-4]
    python server.py 5000
    python server.py 5001
    python server.py 5002
    python server.py 5003
    python server.py 5004

    # The forwarder device must be turned on and is assumed to be alive throughout the servers.

    # launch the client in a separate window
    python client.py

    # follow the prompts
