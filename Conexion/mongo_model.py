from pymongo import MongoClient #type:ignore
import Conexion.mongo_script

def get_session():
    ##MONGO
    client = MongoClient('mongodb://localhost:27017/')
    return client

def base_populate(session ): 
    db = client["intelligent_houses"]
    error_res = mongo_script.poblar_mongodb(db)

def test_session(session):
    result = session.listCollections()
    print(result)


