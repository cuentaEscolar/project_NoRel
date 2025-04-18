from pymongo import MongoClient #type:ignore

def get_session():
    ##MONGO
    client = MongoClient('mongodb://localhost:27017/')
    #db = client["intelligent_houses"]
    return client

def test_session(session):
    result = session.listCollections()
    print(result)
