instructions to run (only tested on Linux):

 - firstly, run the name server with "python -m Pyro4.naming"
 - proceed to run once "python script.py" to automatically start 3 servers
 - start the frontend with "python frontend.py"
 - finally begin the client with "python client.py"

Adjustable parameters:
replica_manager.py - all at the top:

 - sleep timings:
   - REQ_SLEEP - how long to sleep in between polling when waiting to amass frontend's knowledge
   - BACKGROUND_SLEEP - delay between gossip messages (controls the whole loop)
 - Probabilities: fairly self explanatory
   - PROB_CRASH
   - PROB_OVERLOAD
   - PROB_REVERT - probability of recovering from a disaster
----------------------------------------------
 
description of features:

This distributed system provides a client program (text based) and frontend server for querying and updating information stored
by the replicas. The frontend is able to adapt to the RMs crashing/being under heavy load and can redirect the client.

The three replicas communicate with each other using 'gossip' messages to ensure that content stored
amongst them is relatively up to date. This is a 'lazy' system however and doesn't guarantee real time updates (replicas
can fall behind on updates). Vector timestamps are implemented (almost fully) as the book describes. The servers simulate
crashing and going offline with probabilties that can be altered.