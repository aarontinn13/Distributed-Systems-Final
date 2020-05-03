#!/bin/bash

#create 10 servers
for i in {0..4}
do
   gnome-terminal -x bash -c "python server.py 500$i; exec bash"
done
