from pymongo import MongoClient #type:ignore
import Conexion.mongo_script

def get_session():
    ##MONGO
    client = MongoClient('mongodb://localhost:27017/')
    return client

def base_populate(session ): 
    db = session["intelligent_houses"]
    error_res = mongo_script.poblar_mongodb(db)
    return error_res

def test_session(session):
    result = session.listCollections()
    print(result)


