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


def main():
    mongo_db = conexion_mongo()
    cassandra_session = conexion_cassandra()
    

if __name__ == "__main__":
    main()