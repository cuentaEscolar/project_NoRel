from Conexion import cassandra_model
from Conexion import mongo_model

from dataGen import generator


if __name__ == "__main__":
    cassandra_session = cassandra_model.get_session()
    cassandra_model.test_session(cassandra_session)
    generator.emit_cassandra_data_from_csv("../dataGen/mongodb_dispositivos.csv", cassandra_model.insert_data(cassandra_session)  )

    mongo_session = mongo_model.get_session()
    #mongo_model.test_session(mongo_session)
    print(type(mongo_session))

    # DGraph
    
    print("Datos subidos a DGraph")