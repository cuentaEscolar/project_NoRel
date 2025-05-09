import Conexion.mongo_queries as mq
import Conexion.mongo_model as mm
import Conexion.cassandra_model as cm
import Conexion.dgraph_queries as dq
import Conexion.dgraph_connection as dc

class CoherentLoader():

    def __init__(self):

        mongo_session =  mm.get_session()
        cassandra_session = cm.get_session()
        dgraph_session =  dc.DgraphConnection()

        return mongo_session, cassandra_session, dgraph_session

