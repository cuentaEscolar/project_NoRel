from pymongo import MongoClient #type:ignore
import Conexion.mongo_script


def get_session():
    ##MONGO
    client = MongoClient('mongodb://localhost:27017/')
    return client

def get_database(session):
    db = session["intelligent_houses"]
    return db

def base_populate(session): 
    db = get_database(session)
    error_res = Conexion.mongo_script.poblar_mongodb(db)
    Conexion.mongo_script.crear_indices(db)
    return error_res

def test_session(session):
    result = session.list_collection_names()
    print(result)



