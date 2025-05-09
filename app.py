import Conexion.mongo_queries as mq
import Conexion.mongo_model as mm
import Conexion.cassandra_model as cm
import Conexion.dgraph_queries as dq
import Conexion.dgraph_connection as dc

import Conexion.mongo_queries
import json
from Conexion.dgraph_connection import DgraphConnection
from Conexion.dgraph_loader import load_data_to_dgraph
import Conexion.dgraph_queries
import os

def set_username():
    username = input('Ingresa tu username: ')
    return username

def set_num_casa():
    num_casa = input('Ingresa tu número de casa: ')
    return num_casa

def print_menu():
    options = {
        0: "Poblar datos",
        1: "Mostrar información de cuenta",
        2: "Mostrar menú de logs",
        3: "Mostrar menú de dispositivos",
        4: "Mostrar menú de configuraciones",
        5: "Mostrar menú de relaciones de dispositivos",
        6: "Cambiar username",
        7: "Salir",
    }
    for key in options.keys():
        print(key, '--', options[key])


#Mongo menu
#todas las busquedas son por casa
def make_menu(options):
    l = len(options)
    def _h_():
        for key in range(l):
            print('    ', key + 1, '--', options[key])
    return _h_

def print_mongo_menu():
    pass
def print_mongo_menu_dispositivos():
    options = {
        1: "Mostrar cantidad de dispositivos",
        2: "Buscar dispositivos por nombre",
        3: "Buscar dispositivos por tipo y estado",
        4: "Buscar dispositivos por fecha de instalación"
    }
    for key in options.keys():
        print('    ', key, '--', options[key])

def print_mongo_menu_configuraciones():
    options = {
        1: "Buscar configuraciones de un dispositivo",
        2: "Buscar configuraciones por nombre",
        3: "Buscar configuraciones por id",
        4: "Buscar configuraciones por hora de encendido",
        5: "Buscar configuraciones por tipo de dispositivo",
        6: "Buscar configuraciones por fecha de modificación",
        7: "Buscar configuraciones por estado" 
    }
    for key in options.keys():
        print('    ', key, '--', options[key])

def print_dgraph_menu():
    options = {
        1: "Dipositivos de una casa",
        2: "Aires acondicionados de una casa",
        3: "Bombillas de una casa",
        4: "Aspiradoras de una casa",
        5: "Refrigeradores de una casa",
        6: "Cerraduras de una casa",
        7: "Dipositivos encendidos",
        8: "Dispositivos apagados",
        9: "Dispositivos en modo eco",
        10: "Dispositivos en estado de error",
        11: "Dispositivos en standy",
        12: "Dispositivos en habitación",
        13: "Dispositivos sincronizados entre sí",
        14: "Dispositivos en cluster funcional",
        15: "Regresar a menú principal"
    }
    for key in options.keys():
        print('    ', key, '--', options[key])

def print_dgraph_query_result(query_name, result):
    # Todos los queries de dgraph regresan un JSON como respuesta sin imprimir nada por si mismos, esta función le da formato a esas respuestas
    # Usar esta función intermedia es más sencillo que reescribir cada query para que imprima su propio resultado
    """Imprime los resultados de un query de manera formateada"""
    print("\n" + "="*50)
    print(f"Resultados de: {query_name}")
    print("="*50)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print("="*50 + "\n")

def select_opc_menu_relaciones(client, option):
    if option == 1:
        # Dipositivos de una casa
        casa_id = input("Ingresa el id de la casa: ")
        print_dgraph_query_result("Dispositivos en Casa", dq.dispositivos_en_casa(client, casa_id))
    if option == 2:
        # Aires acondicionados de una casa
        casa_id = input("Ingresa el id de la casa: ")
        print_dgraph_query_result("Aires Acondicionados", dq.aires_acondicionados(client, casa_id))
    if option == 3:
        # Bombillas de una casa
        casa_id = input("Ingresa el id de la casa: ")
        print_dgraph_query_result("Bombillas", dq.bombillas(client, casa_id))
    if option == 4:
        # Aspiradoras de una casa
        casa_id = input("Ingresa el id de la casa: ")
        print_dgraph_query_result("Aspiradoras", dq.aspiradoras(client, casa_id))
    if option == 5:
        # Refrigeradores de una casa
        casa_id = input("Ingresa el id de la casa: ")
        print_dgraph_query_result("Refrigeradores", dq.refrigeradores(client, casa_id))
    if option == 6:
        # Cerraduras de una casa
        casa_id = input("Ingresa el id de la casa: ")
        print_dgraph_query_result("Cerraduras", dq.cerraduras(client, casa_id))
    if option == 7:
        # Dipositivos encendidos
        casa_id = input("Ingresa el id de la casa: ")
        print_dgraph_query_result("Dispositivos Encendidos", dq.dispositivos_encendidos(client, casa_id))
    if option == 8:
        # Dispositivos apagados
        casa_id = input("Ingresa el id de la casa: ")
        print_dgraph_query_result("Dispositivos Apagados", dq.dispositivos_apagados(client, casa_id))
    if option == 9:
        # Dispositivos en modo eco
        casa_id = input("Ingresa el id de la casa: ")
        print_dgraph_query_result("Dispositivos en Modo Eco", dq.dispositivos_modo_eco(client, casa_id)) 
    if option == 10:
        # Dispositivos en estado de error
        casa_id = input("Ingresa el id de la casa: ")
        print_dgraph_query_result("Dispositivos con Error", dq.dispositivos_con_error(client, casa_id)) 
    if option == 11:
        # Dispositivos en standy
        casa_id = input("Ingresa el id de la casa: ")
        print_dgraph_query_result("Dispositivos en Standby", dq.dispositivos_en_standby(client, casa_id))
    if option == 12:
        # Dispositivos en habitación
        casa_id = input("Ingresa el id de la casa: ")
        habitacion = input("Ingresa el nombre de la habitación: ")
        print_dgraph_query_result("Dispositivos por Habitación", dq.dispositivos_por_habitacion(client, casa_id, habitacion))
    if option == 13:
        # Dispositivos sincronizados entre sí
        casa_id = input("Ingresa el id de la casa: ")
        print_dgraph_query_result("Dispositivos Sincronizados", dq.dispositivos_sincronizados(client, casa_id))
    if option == 14:
        # Dispositivos en cluster funcional
        casa_id = input("Ingresa el id de la casa: ")
        tipo_funcional = input("Ingresa el tipo de cluster funcional (ej: Climatización, Seguridad, etc): ")
        print_dgraph_query_result("Dispositivos por Cluster Funcional", dq.dispositivos_cluster_funcional(client, casa_id, tipo_funcional))

def print_opcion_dispositivos():
    options = {
        1: "Aire acondicionado",
        2: "Bombilla",
        3: "Refrigerador",
        4: "Aspiradora",
        5: "Cerradura",
        6: "Lavadora"
    }
    for key in options.keys():
        print('    ', key, '--', options[key])

def select_tipo_dispositivo(opcion):
    if opcion == "":
        return None
    else:
        op = int(opcion)
    
    if op == 1:
        tipo = "aire_acondicionado"
    if op == 2:
        tipo = "bombilla"
    if op == 3:
        tipo = "refrigerador"
    if op == 4:
        tipo = "aspiradora"
    if op == 5:
        tipo = "cerradura"
    if op == 6:
        tipo = "lavadora"
    return tipo 

def select_opc_menu_dispositivos(id_casa, option_disp):
    if option_disp == 1:
        #"Mostrar cantidad de dispositivos"
        print_opcion_dispositivos()
        tipo_op = input("Ingresa un tipo de dispositivo (Enter para omitir): ")
        tipo = select_tipo_dispositivo(tipo_op)
        Conexion.mongo_queries.get_cantidad_dispositivos_por_tipo(id_casa, tipo)
    if option_disp == 2:
        #"Buscar dispositivos por nombre"
        nombre_dispositivo = input("Ingresa el nombre del dispositivo: ")
        Conexion.mongo_queries.get_dispositivo_por_nombre(id_casa, nombre_dispositivo)
    if option_disp == 3:
        #"Buscar dispositivos por tipo y estado"
        print_opcion_dispositivos()
        tipo_op = input("Ingresa un tipo de dispositivo (ENTER para OMITIR): ")
        tipo = select_tipo_dispositivo(tipo_op)
        estado = input("Ingresar estado (activo o desactivo). ENTER para OMITIR: ")
        Conexion.mongo_queries.get_dispositivo_por_tipo_estado(id_casa, tipo, estado)
    if option_disp == 4:
        #"Buscar dispositivos por fecha de instalación"
        fecha_instalacion = input("Ingresa fecha (ejem: 2024-06-01): ")
        Conexion.mongo_queries.get_dispositivo_por_fecha_intalacion(id_casa, fecha_instalacion)

def select_opc_menu_configuraciones(id_casa, option_conf):
    if option_conf == 1:
        #"Buscar configuraciones de un dispositivo"
        id_dispositivo = input("Ingresar id de dispositivo: ")
        Conexion.mongo_queries.get_configuraciones_por_dispositivo(id_dispositivo)
    if option_conf == 2:
        #"Buscar configuraciones por nombre",
        nombre_config = input("Ingresa nombre de configuración: ")
        Conexion.mongo_queries.get_config_por_nombre(id_casa, nombre_config)
    if option_conf == 3:
        #"Buscar configuraciones por id",
        config_id = input("Ingresar id de configuración: ")
        Conexion.mongo_queries.get_configuracion_completa(config_id)
    if option_conf == 4:
        #"Buscar configuraciones por hora de encendido",
        hora_on = input("Ingresa hora de encendido (ejem: 19:00): ")
        Conexion.mongo_queries.get_config_por_hora_on(id_casa, hora_on)
    if option_conf == 5:
        #"Buscar configuraciones por tipo de dispositivo",
        print_opcion_dispositivos()
        tipo_op = input("Ingresa un tipo de dispositivo (Enter para omitir): ")
        tipo = select_tipo_dispositivo(tipo_op)
        Conexion.mongo_queries.get_configuracion_por_tipo(id_casa, tipo)
    if option_conf == 6:
        #"Buscar configuraciones por fecha de modificación",
        fecha_modificacion = input("Ingresa fecha (ejem: 2024-06-01): ")
        Conexion.mongo_queries.get_config_por_fecha_modificacion(id_casa, fecha_modificacion)
    if option_conf == 7:
        #"Buscar configuraciones por estado" 
        estado_config = input("Ingresar estado (activo o desactivo). ENTER para OMITIR: ")
        Conexion.mongo_queries.get_config_por_estado(id_casa, estado_config)

def main():
    #ingresar username para poder usar la app
    username = set_username()
    mongo_session =  mm.get_session()
    cassandra_session = cm.get_session()
    dgraph_session = dc.DgraphConnection.initialize_dgraph()

    print(mongo_session)
    print(cassandra_session)
    print(dgraph_session)
    return


    while(True):
        print_menu()
        option = int(input('Ingresa una opción: '))
        if option == 0:
            #poblar bases de datos

            # Dgraph
            base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dataGen')
            connection = DgraphConnection()
            uids = load_data_to_dgraph(base_path, connection) # Cargar los datos
            print("Datos subidos a DGraph")
            ...
        if option == 1:
            Conexion.mongo_queries.get_usuario_info(username)
        if option == 2:
            #mostrar menu de Cassandra
            ...
        if option == 3:
            num_casa = set_num_casa()
            id_casa = Conexion.mongo_queries.get_id_casa(num_casa)
            print_mongo_menu_dispositivos()
            option_disp = int(input('Ingresa una opción: '))
            select_opc_menu_dispositivos(id_casa, option_disp)
        if option == 4:
            num_casa = set_num_casa()
            id_casa = Conexion.mongo_queries.get_id_casa(num_casa)
            print_mongo_menu_configuraciones()
            option_conf = int(input('Ingresa una opción: '))
            select_opc_menu_configuraciones(id_casa, option_conf)
        if option == 5:
            while (True):
                print("Menú de relaciones de dispositivos\n")
                print_dgraph_menu()
                option = int(input('Ingresa una opción: '))
                if option == 15:
                    break
                select_opc_menu_relaciones(dgraph_session, option)
            ...
        if option == 6:
            username = set_username()
        if option == 7:
            exit(0)


if __name__ == '__main__':
    main()
