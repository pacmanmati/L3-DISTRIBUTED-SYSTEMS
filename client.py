from enum import Enum, unique
import Pyro4

@unique
class Action(Enum):
    QUERY = 1
    UPDATE = 2
    NEW_ID = 3
    QUIT = 4

class Client:
    def __init__(self, frontend):

        self.frontend = frontend
        self.user_id = -1
        self.movie_id = -1
        self.rating = -1
        self.action = Action.NEW_ID
        self.run()
        self.timestamp = init_timestamp()

    def merge_timestamp(self, t1, t2): # retain the values of t1 unless t2's are greater
        for k in t1.keys():
            if t1[k] < t2[k]:
                t1[k] = t2[k]
        return t1

    def init_timestamp(self):
        timestamp = {}
        for k in self.frontend.replicas.keys():
            timestamp[k] = 0
        return timestamp
        
    def run(self):
        while self.action is not Action.QUIT:
            if self.action is Action.QUERY:
                ask_movie_id()
                timestamp, name, value = self.frontend.query(self.movie_id, self.timestamp)
                # merge timestamps
                self.timestamp = merge_timestamp(self.timestamp, timestamp)
            elif self.action is Action.UPDATE:
                ask_movie_id()
                ask_rating()
                timestamp, name = self.frontend.update(self.movie_id, self.user_id, self.rating, self.timestamp)
                # merge timestamps
                self.timestamp = merge_timestamp(self.timestamp, timestamp)
            elif self.action is Action.NEW_ID:
                print("Your new id is {}.".format(self.ask_id()))
            self.ask_action()

    def ask_action(self):
        while self.action < 1 or self.action > 4:
            self.action = Action(int(input("Select an action:\n" \
                                           "1 - Retrieve a movie rating,\n" \
                                           "2 - Submit a movie rating,\n" \
                                           "3 - Change user id,\n" \
                                           "4 - Quit.")))
        return self.action

    def ask_id(self):
        self.user_id = input("What is your user_id?").strip()
        return self.user_id

    def ask_movie_id(self):
        self.movie_id = input("Enter a movie_id:").strip()
        return self.movie_id

    def ask_rating(self):
        self.rating = input("Enter a rating (out of ten):").strip()
        return self.rating

# create a frontend proxy
frontend_uri = "PYROMETA:F"
frontend = Pyro4.Proxy(frontend_uri)

Client(frontend)
