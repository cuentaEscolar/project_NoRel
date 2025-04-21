from pymongo import MongoClient #type:ignore
import Conexion.mongo_script

def get_session():
    ##MONGO
    client = MongoClient('mongodb://localhost:27017/')
    return client

def base_populate(session): 
    db = session["intelligent_houses"]
    error_res = Conexion.mongo_script.poblar_mongodb(db)
    Conexion.mongo_script.crear_indices(db)
    return error_res

def test_session(session):
    result = session.listCollections()
    print(result)


