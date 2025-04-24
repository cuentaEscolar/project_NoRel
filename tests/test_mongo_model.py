from Conexion import mongo_model, mongo_gets, mongo_queries
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

#python -m tests.test_mongo_model
client = mongo_model.get_session()
##print(mongo_model.base_populate(client))
# json_resp_disp = mongo_gets.get_dispositivos()
# json_resp_conf = mongo_gets.get_configuraciones()
# json_resp_casas = mongo_gets.get_casas()
# json_resp_usuarios = mongo_gets.get_usuarios()

# for disp in json_resp_disp:
#     mongo_gets.print_x(disp)

# for conf in json_resp_conf:
#     mongo_gets.print_x(conf)

# for ca in json_resp_casas:
#     mongo_gets.print_x(ca)

# for user in json_resp_usuarios:
#     mongo_gets.print_x(user)

##PRUEBAS DE QUERIES
#1
# mongo_queries.get_usuario_info()
id_casa = mongo_queries.get_id_casa(1)