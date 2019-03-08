from enum import Enum, unique
import time
import uuid
import threading
import Pyro4
import random

BACKGROUND_SLEEP = 2.5
REQ_SLEEP = 1.5

@unique
class Status(Enum):
    ONLINE = 1
    OFFLINE = 2

@Pyro4.expose
class ReplicaManager:
    def init(self, filename, name, nameserver):
        self.name = name
        self.ns = nameserver

        self.database = {} # movieID, userID : rating
        self.value_timestamp = self.init_timestamp()
        self.update_queue = []
        self.replica_timestamp = self.init_timestamp()
        self.executed_ops = []
        self.timestamp_table = self.init_timestamp()

        self.read_file(filename)
        self.status = Status.ONLINE
        self.load = 0

        #self.replicas = self.map_replicas() # not needed
        thread = threading.Thread(target=self.replica_loop(), args=[]) # create a thread to run outside the pyro requestLoop
        thread.start()
        daemon.requestLoop()

        
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
        print(replicas)
        # remove this server from the replicas we will address
        del replicas[name]
        return replicas

    def do_updates(self):
        for update in self.update_queue:
            if update[0] not in self.executed_ops:
                #while !timestamp_test(self.value_timestamp, update[2]): # while our rm is behind the update
                if not timestamp_test(self.value_timestamp, update[2]): # while our rm is behind the update
                    continue # skip if it isn't stable
                #     time.sleep(REQ_SLEEP)
                # if update[0] in self.executed_ops:
                #     continue
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
        while not timestamp_test(self.value_timestamp, timestamp): # while value_timestamp is behind the frontend's timestamp
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
            update_queue.append((operation_id, (movie_id, user_id, rating), timestamp, our_ts, self.name))

    def eliminate_records():
        safe_to_remove = True
        safely_removable = []
        for k in self.value_timestamp.keys():
            for update in update_queue:
                if not timestamp_test(self.timestamp_table[k][update[4]], update[3][update[4]]):
                    safe_to_remove = False
                    break
                if safe_to_remove:
                    safely_removable.append(update)
        for update in safely_removable:
            update_queue.remove(update)

    def merge_log(log_old, log_new): # merge log2 into log1
        for update in log_new:
            if update not in log_old and not timestamp_test(self.replica_timestamp, update[3]):
                log_old.append(update)

    def apply_stable_updates():
        # first we extract the stable updates
        stable_updates = []
        for update in update_queue:
            if timestamp_test(self.value_timestamp, update[2]): # if update is stable
                stable_updates.append(update)
        # now we order the updates
        ordered_updates = []
        while len(stable_updates) is not 0:
            min_ts = stable_updates[0][2]
            min_upd = stable_updates[0]
            for i in range(len(stable_updates)):
                if stable_updates[i][2] <= min_ts:
                    min_ts = stable_updates[i][2]
                    min_ipd = stable_updates[i]
            # we have the min
            ordered_updates.append(min_upd) # [0] -> [...] increasing
            
    def gossip(self, log, replica_timestamp, name):
        merge_log(self.log, log)
        print("merged")
        merge_timestamp(self.replica_timestamp, replica_timestamp)
        apply_stable_updates()
        print("applied updates")
        self.timestamp_table[name] = replica_timestamp
        eliminate_records()
        print("eliminated records")
            
    def pick_random_gossip(self):
        others = []
        for k in self.value_timestamp.keys():
            if k is not self.name:
                others.append(k)
        selection = others[random.randrange(len(others))]
        print("my selection is",selection)
        lookup = self.ns.lookup(selection)
        return Pyro4.Proxy(lookup)
        
    def replica_loop(self):
        print("Server {} online".format(self.name))
        while self.status is Status.ONLINE: # until the end of time
            self.do_updates()
            print("done updates")
            self.pick_random_gossip().gossip(self.update_queue, self.replica_timestamp, self.name)
            print("returned")
            time.sleep(BACKGROUND_SLEEP) # less frequent gossips and loops


name = str(uuid.uuid4())
#name = int(sys.argv[1])
ns = Pyro4.locateNS()
#rm = ReplicaManager("dataset/ratings.csv", name, ns)
rm = ReplicaManager()
            
with Pyro4.Daemon() as daemon:
    uri = daemon.register(rm)
    ns.register(name, uri, metadata=["RM"])
    time.sleep(2)
    #print(ns.list(return_metadata=True))
    rm.init("dataset/ratings.csv", name, ns)



