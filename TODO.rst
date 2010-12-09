TODO
====
Create auto-reconnecting client
-------------------------------
The current pyrakoon.client.SocketClient is a mere proof-of-concept. It should
include reconnections, reconnecting to a master node, configuration file
parsing, and whatever else the default Arakoon Python client supports.

Fix pavement.py
---------------
There are several bugs in the current pavement.py script:

- The docs aren't distributed
- Tests aren't distributed

Write documentation
-------------------
A Sphinx skeleton is in place in doc/

Check Twisted exception behaviour
---------------------------------
