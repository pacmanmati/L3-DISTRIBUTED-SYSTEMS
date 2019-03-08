from enum import Enum, unique
import time
import uuid
import threading
import Pyro4
#import sys

BACKGROUND_SLEEP = 2.5
REQ_SLEEP = 1.5

@unique
class Status(Enum):
    ONLINE = 1
    OFFLINE = 2

@Pyro4.expose
class ReplicaManager:
    def __init__(self, filename, name, nameserver):
        self.name = name
        
        self.database = {} # movieID, userID : rating
        self.value_timestamp = init_timestamp()
        self.update_queue = []
        self.replica_timestamp = init_timestamp()
        self.executed_ops = []
        self.timestamp_table = []
        
        self.read_file(filename)
        self.status = Status.ONLINE
        self.load = 0
        self.ns = nameserver
        time.sleep(2) # sleep to make sure all servers are online before mapping
        self.replicas = map_replicas()
        # create a thread to run outside the pyro requestLoop
        thread = threading.Thread(target=replica_loop(), args=[self])
        thread.start()
        
    def init_timestamp(self):
        timestamp = {}
        rms = self.ns.list(metadata_all=["RM"])
        for k in rms.keys():
            timestamp[k] = 0
        return timestamp

    def merge_timestamp(self, t1, t2): # retain the values of t1 unless t2's are greater
    for k in t1.keys():
        if t1[k] < t2[k]:
            t1[k] = t2[k]
    return t1
    
    def read_file(self, filename):
        with open(filename) as f:
            for line in f:
                sep_line = line.split(",")
                map(str.strip, sep_line)
                self.database[(sep_line[0],sep_line[1])] = sep_line[2]#sep_line[2:]
    def map_replicas(self):
        ''' Remap all replicas using information from the NameServer '''
        replicas = {}
        rms = self.ns.list(metadata_all=["RM"])
        for k,v in rms.items():
            replicas[k] = Pyro4.Proxy(v)
        # remove this server from the replicas we will address
        del self.replicas[name]
        return replicas

    def do_updates(self):
        for update in update_queue:
            if update[0] not in self.executed_ops:
                while !timestamp_test(self.value_timestamp, update[2]): # while our rm is behind the update
                    time.sleep(REQ_SLEEP)
                update(update[1][0], update[1][1], update[1][2])
                #self.value_timestamp[self.name] += 1 # increment value timestamp
                self.value_timestamp = merge_timestamp(self.value_timestamp, update[3]) # merge timestamps
                self.executed_ops.append(update[0]) # add to executed operations

    def get_entries(self, movie_id):
        entries = []
        for key, value in self.database.items():
            if key[0] is movie_id:
                entries.append([key, value])
        return entries

    def timestamp_test(self, t1, t2): # checks to see if t1 is ahead of t2
        ahead = True
        for k in t1.keys():
            if t1[k] < t2[k]:
                ahead = False
                break
        return ahead
    
    def query(self, movie_id, timestamp):
        #while timestamp[self.name] > self.value_timestamp[self.name]: # while our replica is behind
        while !timestamp_test(self.value_timestamp, timestamp): # while value_timestamp is behind the frontend's timestamp
            time.sleep(QUERY_SLEEP)
        # once we're out, our query can be responded to
        return self.value_timestamp, get_entries(movie_id)
            
    def update(self, movie_id, user_id, rating):
        self.database[movie_id, user_id] = rating
        
    def queue_update(self, movie_id, user_id, rating, operation_id, timestamp):
        if operation_id not in executed_ops:
            self.replica_timestamp[self.name] += 1 # increment replica timestamp
            our_ts = timestamp.copy()
            our_ts[self.name] = self.replica_timestamp[self.name]
            update_queue.append((operation_id, (movie_id, user_id, rating), timestamp, our_ts))

    def merge_log(log_old, log_new): # merge log2 into log1
        
            
    def gossip(self, log, replica_timestamp):
        merge_log(self.log, log)

def replica_loop(self):
    while True: # until the end of time
        

    
with Pyro4.Daemon() as daemon:
    name = uuid.uuid4()
    #name = int(sys.argv[1])
    ns = Pyro4.locateNS()
    rm = ReplicaManager("dataset/ratings.csv", name, ns)
    uri = daemon.register(rm)
    ns.register(name, uri, metadata=["RM"])
    print("registered")
    print(ns.list(return_metadata=True))
    daemon.requestLoop()

