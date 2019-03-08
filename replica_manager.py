from enum import Enum, unique
import time
import uuid
import threading
import Pyro4
import random

BACKGROUND_SLEEP = 2
REQ_SLEEP = 1

# probabilities

PROB_CRASH = 0.1
PROB_OVERLOAD = 0.2
PROB_REVERT = 0.4

@unique
class Status(Enum):
    ONLINE = 1
    OFFLINE = 2
    OVERLOADED = 3

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
        self.timestamp_table = self.init_timestamp_table()

        self.read_file(filename)
        self.status = Status.ONLINE

        threading.Thread(target=daemon.requestLoop).start()
        self.replica_loop()

    def get_status(self):
        return self.status
        
    def init_timestamp(self):
        timestamp = {}
        rms = self.ns.list(metadata_all=["RM"])
        for k in rms.keys():
            timestamp[k] = 0
        return timestamp

    def init_timestamp_table(self):
        timestamp = {}
        rms = self.ns.list(metadata_all=["RM"])
        for k in rms.keys():
            timestamp[k] = self.init_timestamp()
        return timestamp

    def merge_timestamp(self, t1, t2): # retain the values of t1 unless t2's are greater
        for k in t1.keys():
            if t1[k] < t2[k]:
                t1[k] = t2[k]
        return t1

    def get_name(self):
        return self.name
    
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
        #print(replicas)
        # remove this server from the replicas we will address
        del replicas[name]
        return replicas

    def do_updates(self):
        for update in self.update_queue:
            if update[0] not in self.executed_ops:
                #while !timestamp_test(self.value_timestamp, update[2]): # while our rm is behind the update
                if not self.timestamp_test(self.value_timestamp, update[2]): # while our rm is behind the update
                    continue # skip if it isn't stable
                self.update(update[1][0], update[1][1], update[1][2])
                #self.value_timestamp[self.name] += 1 # increment value timestamp
                self.value_timestamp = self.merge_timestamp(self.value_timestamp, update[3]) # merge timestamps
                self.executed_ops.append(update[0]) # add to executed operations

    def get_entries(self, movie_id):
        entries = []
        for key, value in self.database.items():
            if key[0] == movie_id.strip():
                entries.append([key[0], key[1], value])
        return entries

    def timestamp_test(self, t1, t2): # checks to see if t1 is ahead of t2
        ahead = True
        for k in t1.keys():
            if t1[k] < t2[k]:
                ahead = False
                break
        return ahead
    
    def query(self, movie_id, timestamp):
        print("{} being queried".format(self.name))
        #while timestamp[self.name] > self.value_timestamp[self.name]: # while our replica is behind
        while not self.timestamp_test(self.value_timestamp, timestamp): # while value_timestamp is behind the frontend's timestamp
            # print("own timestamp", self.value_timestamp)
            # print("fe timestamp", timestamp)
            print("{} is waiting on a value before it can respond".format(self.name))
            time.sleep(REQ_SLEEP)
        # once we're out, our query can be responded to
        return self.value_timestamp, self.get_entries(movie_id)
            
    def update(self, movie_id, user_id, rating):
        print("{} applying update: movie {} now has rating {} from user {}".format(self.name, movie_id, rating, user_id))
        self.database[(movie_id, user_id)] = rating
        
    def queue_update(self, movie_id, user_id, rating, operation_id, timestamp):
        if operation_id not in self.executed_ops:
            self.replica_timestamp[self.name] += 1 # increment replica timestamp
            self.timestamp_table[self.name] = self.replica_timestamp
            our_ts = timestamp.copy()
            our_ts[self.name] = self.replica_timestamp[self.name]
            self.update_queue.append((operation_id, (movie_id, user_id, rating), timestamp, our_ts, self.name))
            #return self.value_timestamp
            return our_ts

    def eliminate_records(self):
        safely_removable = []
        for update in self.update_queue:
            safe_to_remove = True
            for k in self.value_timestamp.keys():
                if self.timestamp_table[k][update[4]] < update[3][update[4]]:
                    safe_to_remove = False
                    break
                if safe_to_remove:
                    safely_removable.append(update)
        for update in reversed(safely_removable):
            #print("removing", update)
            try:
                self.update_queue.remove(update)
            except ValueError:
                print("value already removed")
                

    def merge_log(self, log_old, log_new): # merge log2 into log1
        for update in log_new:
            #if update not in log_old and not self.timestamp_test(self.replica_timestamp, update[3]):
            if self.timestamp_test(self.replica_timestamp, update[3]):
                log_old.append(update)
        return log_old

    def apply_stable_updates(self):
        # first we extract the stable updates
        stable_updates = []
        for update in self.update_queue:
            if self.timestamp_test(update[2], self.value_timestamp) and update not in self.executed_ops: # if update is stable
                stable_updates.append(update)
        # execute instructions in order
        while len(stable_updates) is not 0:
            saved_i = 0
            min_ts = stable_updates[0][2]
            for i in range(len(stable_updates)):
                if self.timestamp_test(min_ts, stable_updates[i][2]):
                    saved_i = i
                    min_ts = stable_updates[i][2]
            # we have the min, remove it from stable updates  and exec it
            update = stable_updates.pop(i)
            if update not in self.executed_ops:
                self.update(update[1][0], update[1][1], update[1][2]) # execute update
                self.value_timestamp = self.merge_timestamp(self.value_timestamp, update[3]) # merge timestamps
                self.executed_ops.append(update[0]) # add to executed operations        
            
    def gossip(self, log, replica_timestamp, name):
        if self.status is Status.ONLINE:
            print("{} is receiving gossip from {}".format(self.name, name))
            #print("starting merge")
            self.update_queue = self.merge_log(self.update_queue, log)
            #print("LENGTH OF LOG", len(self.update_queue), self.name)
            #print("merged log")
            #print("REPLICA TIMESTAMP {} BEFORE: {}".format(self.name, self.replica_timestamp))
            self.replica_timestamp = self.merge_timestamp(self.replica_timestamp, replica_timestamp)
            self.timestamp_table[self.name] = replica_timestamp
            #print("REPLICA TIMESTAMP {} AFTER: {}".format(self.name, self.replica_timestamp))
            #print("merged timestamp")
            self.apply_stable_updates()
            #print("applied updates")
            self.timestamp_table[name] = replica_timestamp
            #self.timestamp_table[self.name] = self.replica_timestamp
            #print("updated table")
            self.eliminate_records()
            #print("eliminated records")
            
    def pick_random_gossip(self):
        others = []
        for k in self.value_timestamp.keys():
            if k.strip() != self.name.strip():
                others.append(k)
        selection = others[random.randrange(len(others))]
        #print("my selection is {} i am {}".format(selection, self.name))
        lookup = self.ns.lookup(selection)
        return Pyro4.Proxy(lookup)

    def random_event(self):
        # if offline, there's a chance to go back online
        if random.random() < PROB_REVERT:
            print("{} has come back online".format(self.name))
            self.status = Status.ONLINE
        # randomly crash
        if random.random() < PROB_CRASH:
            print("{} has crashed".format(self.name))
            self.status = Status.OFFLINE
        elif random.random() < PROB_OVERLOAD:
            print("{} is overloaded".format(self.name))
            self.status = Status.OVERLOADED

        
    def replica_loop(self):
        print("Server {} online".format(self.name))
        while True: # until the end of time
            if len(self.update_queue) is not 0 or True:
                #print("{} doing replica loop".format(self.name))
                if self.status is not Status.ONLINE:
                    self.do_updates()
                    self.pick_random_gossip().gossip(self.update_queue, self.replica_timestamp, self.name)
                    # print("{} has {}".format(self.name, self.replica_timestamp))
                    # print("{} has {}".format(self.name, self.value_timestamp))
                self.random_event()
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



