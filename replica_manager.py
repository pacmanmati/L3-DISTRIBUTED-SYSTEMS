from enum import Enum, unique
import Pyro4

# maybe represent load with an int / 100
# @unique
# class Load(Enum):
#     LOW = 1
#     MEDIUM = 2
#     HIGH = 3

@unique
class Status(Enum):
    ONLINE = 1
    OFFLINE = 2

class ReplicaManager:
    def __init__(self, filename):
        self.database = {} # movieID, userID : rating 
        self.read_file(filename)
        self.status = Status.ONLINE
        self.load = 0

    def read_file(self, filename):
        with open(filename) as f:
            for line in f:
                sep_line = line.split(",").strip()
                self.database[(sep_line[0],sep_line[1])] = sep_line[2]#sep_line[2:]

    def retrieve(movieid):
        # error handling? what if movie doesnt exist or there are no ratings
        entries = []
        for key, value in self.database.items():
            if key[0] is movieid:
                entries.append([key, value])
        return entries

    def update(movieid, id, rating):
        self.database[movieid, id] = rating
        
        
