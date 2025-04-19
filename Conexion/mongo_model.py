from pymongo import MongoClient #type:ignore
import mongo_script

def get_session():
    ##MONGO
    client = MongoClient('mongodb://localhost:27017/')
    db = client["intelligent_houses"]
    mongo_script.poblar_mongodb(db)
    return client

def test_session(session):
    result = session.listCollections()
    print(result)


