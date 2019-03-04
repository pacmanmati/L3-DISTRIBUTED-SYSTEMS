import Pyro4

@Pyro4.expose
class Frontend:
    def __init__(self):
        print("frontend running")
    def find_rm(self):
        print("frontend running")        
    def query():
        print("frontend running")
    def update():
        print("frontend running")
        
with Pyro4.Daemon() as daemon:
    print(daemon.register(Frontend))
    #daemon.requestLoop()
