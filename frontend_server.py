import uuid
import random
import Pyro4

@Pyro4.expose
class Frontend:
    def __init__(self, nameserver):
        self.ns = nameserver
        self.replicas = self.map_replicas()
    def get_replicas(self):
        return self.replicas
    def map_replicas(self):
        ''' Remap all replicas using information from the NameServer '''
        replicas = {}
        rms = self.ns.list(metadata_all=["RM"])
        for k,v in rms.items():
            replicas[k] = Pyro4.Proxy(v)
        return replicas
    def find_rm(self):
        reps = list(self.replicas.keys())
        selection = reps[random.randrange(len(reps))]
        print("my selection is",selection)
        lookup = self.ns.lookup(selection)
        return Pyro4.Proxy(lookup)

    def query(self, movie_id, timestamp):
        rm = self.find_rm()
        print("found rm", rm)
        timestamp, rating = rm.query(movie_id, timestamp)
        #print("Movie with id {} is rated {}".format(movie_id, rating))
        return timestamp, rm.get_name(), rating
        
    def update(self, movie_id, user_id, rating, timestamp):
        rm = self.find_rm()
        timestamp = rm.queue_update(movie_id, user_id, rating, uuid.uuid4(), timestamp)
        return timestamp, rm.get_name()
        
with Pyro4.Daemon() as daemon:
    ns = Pyro4.locateNS()
    frontend = Frontend(ns)
    uri = daemon.register(frontend)
    name = "frontend"
    ns.register(name, uri, metadata=["F"])
    print("registered")
    print(ns.list(return_metadata=True)) # debug
    daemon.requestLoop()
