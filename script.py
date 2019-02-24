from replica_manager import ReplicaManager
from client import Client
import Pyro4

client = Client()
rm = ReplicaManager("dataset/ratings.csv")
