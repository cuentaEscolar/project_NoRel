from Conexion import mongo_model, mongo_gets, mongo_queries
import pymongo 

#python -m tests.test_mongo_model

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


# client = mongo_model.get_session()
# print(mongo_model.base_populate(client))
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
#primero obtengo username
username = "user_0" #input("Ingresa tu nombre de usuario: ")
#obtner la casa del usuario
#1
# get info del usuario
#mongo_queries.get_usuario_info(username)

#2 
#primero obtengo la casa y su id
num_casa = 1 #int(input("ingrese su número de casa: "))
id_casa = mongo_queries.get_id_casa(num_casa)

#2
#get configuraciones activas on off de dispositivos tipo x 
tipo = "lavadora" #input("Ingresa el tipo de dispositivo que quieres buscar")
#mongo_queries.get_configuracion_horario(id_casa, tipo)

#3
#get dispositivos por tipo y estado
#pregunto otra vez el tipo 
#pregunto por el estado
estado = "desactivo" #input("Ingresa el tipo de dispositivo que quieres buscar")
#mongo_queries.get_dispositivo_por_tipo_estado(id_casa,tipo, estado)

#4
#todas las configuraciones de un dispositivo
#pregundo por el id del dispositivo que lo pueden encontrar en el query anterior
id_dispositivo = "680bc4bdc1c5b2ec3f1a99b2" #input("Ingrese id del dispositivo: ")
#mongo_queries.get_configuraciones_por_dispositivo(id_dispositivo)

#5
#buscar config por nombre
#pido el nombre
nombre_config = "noche" #input("Ingresa el nombre de la configuración: ")
#mongo_queries.get_config_por_nombre(id_casa, nombre_config)

#6
#configuracion completa dada un id
config_id = "680bc4bdc1c5b2ec3f1a99b3" #imput("Inserte id de configuración: ")
#mongo_queries.get_configuracion_completa(config_id)

#7
#Configuraciones por fecha de modificación de un dispositivo.	
fecha_modificacion = '2024-10-18'
#mongo_queries.get_config_por_fecha_modificacion(id_casa, fecha_modificacion)


#8
#Configuraciones por hora on.	
hora_on = '16:19'
#mongo_queries.get_config_por_hora_on(id_casa, hora_on)

#9
# Número de tipo dispositivos en una casa.	
#vuelvo a preguntar el tipo
#mongo_queries.get_cantidad_dispositivos_por_tipo(id_casa, "")


#10
#Dispositivo por fecha de instalación.	
fecha_instalacion = '2023-03-05'
mongo_queries.get_dispositivo_por_fecha_intalacion(id_casa, fecha_instalacion)

#11
# Configuraciones dado estado.	
estado_config = "desactivo" #input("Ingresa estado de configuraciones a buscar: ")
#mongo_queries.get_config_por_estado(id_casa, estado_config)

#12
#Dispositivos por nombre.	
nombre_dispositivo = "Aire 1" #input("Ingresa el nombre del dispositivo: ")
#mongo_queries.get_dispositivo_por_nombre(id_casa, nombre_dispositivo)