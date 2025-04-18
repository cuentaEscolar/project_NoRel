from Conexion import cassandra_model
from Conexion import mongo_model

if __name__ == "__main__":
    print("hi")
    cassandra_session = cassandra_model.get_session()
    cassandra_model.test_session(cassandra_session)
    mongo_session = mongo_model.get_session()
    mongo_model.test_session(mongo_session)

