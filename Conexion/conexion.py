#!/usr/bin/env python3
import logging
import os
from cassandra.cluster import Cluster
from pymongo import MongoClient

# Configuración de logging para que no marque errores al ejecutar el programa
from cassandra.policies import DCAwareRoundRobinPolicy

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================== Conexiones a bases de datos ==================================================

def connect_cassandra():
    cluster_ips = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost').split(',')
    keyspace = os.getenv('CASSANDRA_KEYSPACE', 'investments')

    logger.info("Conectando a Cassandra...")

    # Necesario para que Cassandra funcione correctamente ya que marca error al ejecutarse
    # Solución extraída de https://stackoverflow.com/questions/53039520/dcawareroundrobinpolicy-vs-roundrobinpolicy
    cluster = Cluster(
        contact_points=cluster_ips,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),  # Política de balanceo
        protocol_version=5
    )
    
    session = cluster.connect()

    # cluster = Cluster(cluster_ips)
    # session = cluster.connect()
    
    # Crear keyspace si no existe
    replication_factor = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
        f"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}}"
    )
    session.set_keyspace(keyspace)
    
    return session

def connect_mongodb():
    logger.info("Conectando a MongoDB...")
    client = MongoClient('mongodb://localhost:27017/')
    return client['bookstore']

# ============================================== Menús y operaciones =================================================

def cassandra_menu(session):
    while True:
        print("\n=== MENU CASSANDRA ===")
        print("1. Crear")
        print("2. Mostrar")
        print("3. Actualizar")
        print("4. Eliminar")
        print("5. Volver al menú principal")
        
        choice = input("Selección: ")
        
        if choice == '1':
            # TODO: Implementar
            pass
        elif choice == '2':
            # TODO: Implementar
            pass
        elif choice == '3':
            # TODO: Implementar
            pass
        elif choice == '4':
            # TODO: Implementar
            pass
        elif choice == '5':
            break

def mongodb_menu(db):
    while True:
        print("\n=== MENU MONGODB ===")
        print("1. Crear")
        print("2. Mostrar")
        print("3. Actualizar")
        print("4. Eliminar")
        print("5. Volver al menú principal")
        
        choice = input("Selección: ")
        
        if choice == '1':
            # TODO: Implementar
            pass
        elif choice == '2':
            # TODO: Implementar
            pass
        elif choice == '3':
            # TODO: Implementar
            pass
        elif choice == '4':
            # TODO: Implementar
            pass
        elif choice == '5':
            break

# ================================================ Menú principal ====================================================

def main():
    # Inicializar conexiones
    cassandra_session = None
    mongo_db = None
    
    while True:
        print("\n=== MENU PRINCIPAL ===")
        print("1. Conectar a Cassandra")
        print("2. Conectar a MongoDB")
        print("3. Salir")
        
        choice = input("Selección: ")
        
        if choice == '1':
            cassandra_session = connect_cassandra()
            cassandra_menu(cassandra_session)
        elif choice == '2':
            mongo_db = connect_mongodb()
            mongodb_menu(mongo_db)
        elif choice == '3':
            logger.info("Saliendo...")
            break

if __name__ == '__main__':
    main()
