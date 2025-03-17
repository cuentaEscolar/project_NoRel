import logging
import os
import model
from cassandra.cluster import Cluster #type: ignore

from pymongo import MongoClient #type:ignore




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
    db = client["intelligent_house"]
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
        1: "Logs sensor data in real-time for time-series analysis.",
        2: "Temperature",
        3: "Electric consumption",
        4: "Light Level"
    }
    for key in cm_options.keys():
        print('    ', key, '--', cm_options[key])


def print_mongo_menu():
    mdb_options = {
        1: "Temperature per room",
        2: "Electricity bill",
        3: "Devices that consume more energy",
        4: "Attempts to force locks"
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



def main():
    mongo_db = conexion_mongo()
    cassandra_session = conexion_cassandra()

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
            print_mongo_menu()
            mongo_option = int(input("Enter your choice: "))
        if option == 3:
            print_dgraph_menu()
            dgraph_option = int(input("Enter your choice: "))
        if option == 4:
            exit(0)
            
    

if __name__ == "__main__":
    main()