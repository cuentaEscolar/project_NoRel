from pymongo import MongoClient #type:ignore

def get_session():
    ##MONGO
    client = MongoClient('mongodb://localhost:27017/')
    return client







