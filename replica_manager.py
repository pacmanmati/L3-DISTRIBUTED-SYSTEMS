from enum import Enum, unique
import uuid
import threading
import Pyro4

@unique
class Status(Enum):
    ONLINE = 1
    OFFLINE = 2

@Pyro4.expose
class ReplicaManager:
    def __init__(self, filename, name, nameserver):
        self.name = name
        self.database = {} # movieID, userID : rating 
        self.read_file(filename)
        self.status = Status.ONLINE
        self.load = 0
        self.ns = nameserver
        self.replicas = None
        self.clock = {self.name: 0}
        self.value = []
        # create a thread to run outside the pyro requestLoop
        thread = threading.Thread(target=replica_loop(), args=[self])
        thread.start()
    def read_file(self, filename):
        with open(filename) as f:
            for line in f:
                sep_line = line.split(",")
                map(str.strip, sep_line)
                self.database[(sep_line[0],sep_line[1])] = sep_line[2]#sep_line[2:]
    def query(self, movieid):
        # error handling? what if movie doesnt exist or there are no ratings
        entries = []
        for key, value in self.database.items():
            if key[0] is movieid:
                entries.append([key, value])
        return entries
    def update(self, movieid, id, rating):
        self.database[movieid, id] = rating
    def queue_query(self):

    def queue_update(self):

    def gossip(self):
        # remap replicas in case of offline servers
        self.replicas = map_replicas()

    def map_replicas(self):
        ''' Remap all replicas using information from the NameServer '''
        replicas = {}
        rms = self.ns.list(metadata_all=["RM"])
        for k,v in rms.items():
            replicas[k] = Pyro4.Proxy(v)
        # remove this server from the replicas
        del self.replicas[name]
        return replicas

def replica_loop(self):
    while True:
        
    
with Pyro4.Daemon() as daemon:
    #name = input("Input a name for the server (it doesn't really matter, just has to be distinct - otherwise a connection to a server might be overwritten):")
    name = uuid.uuid4()
    ns = Pyro4.locateNS()
    rm = ReplicaManager("dataset/ratings.csv", name, ns)
    uri = daemon.register(rm)
    ns.register(name, uri, metadata=["RM"])
    print("registered")
    print(ns.list(return_metadata=True))
    #print(ns.extract_meta
    daemon.requestLoop()

