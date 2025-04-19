from Conexion import mongo_model 
import pymongo 

class Test_mongo_model():
    def test_session_creation(self):
        session = mongo_model.get_session()
        assert type(session) == pymongo.synchronous.mongo_client.MongoClient
        return session
    
    #def test_schema_creation(self):
        #expected_tables = ( "log_by_a_d", "log_by_a_d_de",
        #"log_by_a_d_u", "log_by_a_d_u_v",
        #"log_by_a_d_de_u", "log_by_a_d_de_u_v")
        #session = cassandra_model.get_session()
        #created_tables = cassandra_model.test_session(session) 
        #assert created_tables is not None
#
        #for table in created_tables:
            #assert table.name in expected_tables

