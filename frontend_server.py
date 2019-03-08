import uuid
import Pyro4

@Pyro4.expose
class Frontend:
    def __init__(self, nameserver, daemon):
        self.daemon = daemon
        self.ns = nameserver
        self.replicas = self.map_replicas()
    def map_replicas(self):
        ''' Remap all replicas using information from the NameServer '''
        replicas = {}
        rms = self.ns.list(metadata_all=["RM"])
        for k,v in rms.items():
            replicas[k] = Pyro4.Proxy(v)
        return replicas
    def find_rm(self):
        ''' Returns an RM with minimal load '''
        min_load = 100
        ret_rm = None
        for k,v in self.replicas.items():
            load = v.load
            if load <= min_load:
                min_load = load
                min_load_rm = k
        return ret_rm if ret_rm != None else raise Exception("find_rm() returns None")

    def query(self, movie_id, timestamp):
        rm = find_rm()
        timestamp, rating = rm.query(movie_id, timestamp)
        print("Movie with id {} is rated {}".format(movie_id, rating))
        return timestamp, rm.name, rating
        
    def update(self, movie_id, user_id, rating, timestamp):
        rm = find_rm()
        timestamp = rm.queue_update(movie_id, user_id, rating, uuid.uuid4(), timestamp)
        return timestamp, rm.name
        
with Pyro4.Daemon() as daemon:
    ns = Pyro4.locateNS()
    frontend = Frontend(ns)
    uri = daemon.register(frontend)
    name = uuid.uuid4()
    ns.register(name, uri, metadata=["F"])
    print("registered")
    print(ns.list(return_metadata=True)) # debug
    daemon.requestLoop()
