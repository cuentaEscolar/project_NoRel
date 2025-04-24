import logging
import os
import model
import csv
from cassandra.cluster import Cluster #type: ignore

from pymongo import MongoClient #type:ignore

from Conexion.dgraph_connection import DgraphConnection
from Conexion.dgraph_loader import load_data_to_dgraph
from Conexion.dgraph_model import set_schema

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('IoT.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)


#CONEXION A CASSANDRA
# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'IoT')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

def conexion_cassandra():
    ##CASSANDRA
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()
    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)
    return session

##CONEXION MONGO
def conexion_mongo():
    ##MONGO
    client = MongoClient('mongodb://localhost:27017/')
    db = client["intelligent_houses"]
    return db


def print_menu():
    pm_options = {
        0: "Populate data",
        1: "Show Cassandra menu",
        2: "Show Mongo db menu",
        3: "Show Dgraph menu",
        4: "Exit"
    }
    for key in pm_options.keys():
        print(key, '--', pm_options[key])


def print_cassandra_menu():
    cm_options = {
        1: "Filter the logs by date",
        2: "Filter the logs by time and device",
        3: "Filter the logs by time and unit",
        4: "Filter the logs by time, unit and value",
        5: "Filter the logs by time, device and unit",
        6: "Filter the logs by time, device, unit and value"
    }
    for key in cm_options.keys():
        print('    ', key, '--', cm_options[key])


def print_mongo_menu():
    mdb_options = {
        1: "Información usuario",
        2: "Configuracion on/off de dispositivo por tipo",
        3: "Dipsositivos por tipo y estado",
        4: "Configuraciones de un dispositivo",
        5: "Configuraciones por nombre",
        6: "Configuracione completas por id",
        7: "Configuraciones por fecha de modificacion",
        8: "Configuraciones por horario de encendido",
        9: "Cantidad de dispositivos por tipo",
        10: "Dispositivos por fecha de instalación",
        11: "Configuraciones por e"
    }
    for key in mdb_options.keys():
        print('    ', key, '--', mdb_options[key])


def print_dgraph_menu():
    dgm_options = {
        1: "Falta",
        2: "Falta",
        3: "Falta",
        4: "Falta"
    }
    for key in dgm_options.keys():
        print('    ', key, '--', dgm_options[key])



##CONEXION DGRAPH
def initialize_dgraph():
    try:
        logging.info("Initializing Dgraph connection")
        connection = DgraphConnection()
        client = connection.connect()
        
        # Intentar establecer el schema
        set_schema(client)
        logging.info("Dgraph initialization successful")
        return client
    except Exception as e:
        logging.error(f"Failed to initialize Dgraph: {e}")
        raise


def main():
    mongo_db = conexion_mongo()
    cassandra_session = conexion_cassandra()
    dgraph_client = initialize_dgraph()

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 0:
            #llamar funcion para poblar bases de datos de cassandra, mongo y dgraph
            return
        if option == 1:
            print_cassandra_menu()
            cass_option = int(input('Enter your choice: '))
        if option == 2:
            num_casa = int(input("Introduce tu número de casa: "))
            print_mongo_menu()
            mongo_option = int(input("Enter your choice: "))
        if option == 3:
            print_dgraph_menu()
            dgraph_option = int(input("Enter your choice: "))
        if option == 4:
            exit(0)
            
    

if __name__ == "__main__":
    main()
