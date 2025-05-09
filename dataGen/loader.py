from Conexion import cassandra_model
from Conexion import mongo_model
from Conexion.dgraph_connection import DgraphConnection #Conexion a Dgraph
from Conexion.dgraph_loader import load_data_to_dgraph # Carga de datos a Dgraph
from dataGen import generator
import os

if __name__ == "__main__":
    cassandra_session = cassandra_model.get_session()
    cassandra_model.test_session(cassandra_session)
    generator.emit_cassandra_data_from_csv("../dataGen/mongodb_dispositivos.csv", cassandra_model.insert_data(cassandra_session)  )

    mongo_session = mongo_model.get_session()
    #mongo_model.test_session(mongo_session)
    print(type(mongo_session))

    # DGraph
    dgraph_client = DgraphConnection.initialize_dgraph()
    data_gen_path = os.path.dirname(os.path.abspath(__file__))
    uids = load_data_to_dgraph(data_gen_path, None)
    print("Datos subidos a DGraph")