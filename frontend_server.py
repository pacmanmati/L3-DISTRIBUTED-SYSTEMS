import uuid
import Pyro4

@Pyro4.expose
class Frontend:
    def __init__(self, nameserver):
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

    def query(self, rm, movieid):
        rm.queue_query(movieid)
    def update(self, rm, movieid, id, rating):
        rm.queue_update(movieid, id, rating)
        
with Pyro4.Daemon() as daemon:
    ns = Pyro4.locateNS()
    frontend = Frontend(ns)
    uri = daemon.register(frontend)
    name = uuid.uuid4()
    #name = input("Input a name for the server (it doesn't really matter, just has to be distinct - otherwise a connection to a server might be overwritten):")
    ns.register(name, uri, metadata=["F"])
    print("registered")
    print(ns.list(return_metadata=True))
    #daemon.requestLoop()
