import logging
import os
import model
import csv
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
        1: "Get device on/off time settings",
        2: "Get device temperature settings",
        3: "Get history settings for a device",
        4: "Get full settings by device type"
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




def csv_a_mongo(archivo_csv, coleccion):
    with open(archivo_csv, "r", encoding="utf-8") as f:
        lector = csv.DictReader(f)
        datos = []
        for fila in lector:
            datos.append(fila)
        if datos:
            coleccion.insert_many(datos)
        print(f"Se importaron {len(datos)} registros desde {archivo_csv} a MongoDB")

def poblar_datos_mongo(db):
    csv_a_mongo("mongodb_dispositivos.csv", db["dispositivos"])
    csv_a_mongo("mongodb_configuraciones.csv", db["configuraciones"])



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