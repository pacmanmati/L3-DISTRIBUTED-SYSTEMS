import time
import Pyro4
import subprocess
import sys

servers = []
try:
    for i in range(3): # create n servers
        servers.append(subprocess.Popen(["python", "replica_manager.py"]))
        print("Server being spawned")
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Closing servers")
    for proc in servers:
        proc.kill()
    sys.exit()
