from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pprint
uri = "mongodb+srv://pablvalenzuela:lascumbres2312@cluster0.cceutla.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    db = client.am
    users = db.users
    pprint.pprint(users.find_one())
except Exception as e:
    print(e)



