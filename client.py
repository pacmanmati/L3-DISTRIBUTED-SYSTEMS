from enum import Enum, unique
import Pyro4

@unique
class Action(Enum):
    RETRIEVE = 1
    SUBMIT = 2
    NEW_ID = 3
    QUIT = 4

class Client:
    def __init__(self):
        self.id = -1
        self.movie_id = -1
        self.rating = -1
        self.action = Action.NEW_ID
        self.run()

    def run(self):
        while self.action is not Action.QUIT:
            self.ask_action()
            if self.action is Action.RETRIEVE:
                print("retrieving")
            elif self.action is Action.SUBMIT:
                print("submitting")
            elif self.action is Action.NEW_ID:
                print("Your new id is {}.".format(self.ask_id()))

    def ask_action(self):
        self.action = Action(int(input("Select an action:\n" \
        "1 - Retrieve a movie rating,\n" \
        "2 - Submit a movie rating,\n" \
        "3 - Change user id,\n" \
        "4 - Quit.")))
        return self.action

    def ask_id(self):
        self.id = input("What is your user_id?").strip()
        return self.id

    def ask_movieid(self):
        self.movie_id = input("Enter a movie_id:").strip()
        return self.movie_id

    def ask_rating(self):
        self.rating = input("Enter a rating (out of ten):").strip()
        return self.rating

# create a frontend proxy
frontend_uri = "PYROMETA:F"
frontend = Pyro4.Proxy(frontend_uri)
