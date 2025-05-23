import Conexion.mongo_queries as mq
import Conexion.mongo_model as mm
import Conexion.cassandra_model as cm
import Conexion.dgraph_queries as dq
import Conexion.dgraph_connection as dc
import Conexion.printing_utils as pu
import dataGen.generator as gen

import json
from Conexion.dgraph_connection import DgraphConnection
from Conexion.dgraph_loader import load_data_to_dgraph
import Conexion.dgraph_queries
import os
from datetime import datetime
import time
import uuid
import re


def print_dict(x):
    for k in x.keys():
        print(f"{k}: {x[k]}")
    print("="*50)


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

def print_mongo_menu_dispositivos():
    options = {
        1: "Mostrar cantidad de dispositivos",
        2: "Buscar dispositivos por nombre",
        3: "Buscar dispositivos por tipo y estado",
        4: "Buscar dispositivos por fecha de instalación",
        5: "Cambiar casa",
        6: "Regresar a menú principal"
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
        7: "Buscar configuraciones por estado",
        8: "Cambiar casa",
        9: "Regresar a menú principal"
    }
    for key in options.keys():
        print('    ', key, '--', options[key])

def print_dgraph_menu():
    options = {
        0: "Mostrar todas las casas",
        1: "Dispositivos de una casa",
        2: "Aires acondicionados de una casa",
        3: "Bombillas de una casa",
        4: "Aspiradoras de una casa",
        5: "Refrigeradores de una casa",
        6: "Cerraduras de una casa",
        7: "Dipositivos encendidos",
        8: "Dispositivos apagados",
        9: "Dispositivos en modo eco",
        10: "Dispositivos en estado de error",
        11: "Dispositivos en standby",
        12: "Dispositivos en habitación",
        13: "Dispositivos sincronizados entre sí",
        14: "Clusters en una casa",
        15: "Dispositivos en cluster funcional",
        16: "Regresar a menú principal"
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
    if option == 0:
        # Todas las casas
        print_dgraph_query_result("Todas las casas", dq.casas(client))
    if option == 1:
        # Dipositivos de una casa
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Dispositivos en Casa", dq.dispositivos_en_casa(client, casa_id))
    if option == 2:
        # Aires acondicionados de una casa
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Aires Acondicionados", dq.aires_acondicionados(client, casa_id))
    if option == 3:
        # Bombillas de una casa
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Bombillas", dq.bombillas(client, casa_id))
    if option == 4:
        # Aspiradoras de una casa
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Aspiradoras", dq.aspiradoras(client, casa_id))
    if option == 5:
        # Refrigeradores de una casa
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Refrigeradores", dq.refrigeradores(client, casa_id))
    if option == 6:
        # Cerraduras de una casa
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Cerraduras", dq.cerraduras(client, casa_id))
    if option == 7:
        # Dipositivos encendidos
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Dispositivos Encendidos", dq.dispositivos_encendidos(client, casa_id))
    if option == 8:
        # Dispositivos apagados
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Dispositivos Apagados", dq.dispositivos_apagados(client, casa_id))
    if option == 9:
        # Dispositivos en modo eco
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Dispositivos en Modo Eco", dq.dispositivos_modo_eco(client, casa_id)) 
    if option == 10:
        # Dispositivos en estado de error
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Dispositivos con Error", dq.dispositivos_con_error(client, casa_id)) 
    if option == 11:
        # Dispositivos en standy
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Dispositivos en Standby", dq.dispositivos_en_standby(client, casa_id))
    if option == 12:
        # Dispositivos en habitación
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        habitacion = input("Ingresa el nombre de la habitación: ")
        print_dgraph_query_result("Dispositivos por Habitación", dq.dispositivos_por_habitacion(client, casa_id, habitacion))
    if option == 13:
        # Dispositivos sincronizados entre sí
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Dispositivos Sincronizados", dq.dispositivos_sincronizados(client, casa_id))
    if option == 14:
        # Todos los clusters de una casa
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        print_dgraph_query_result("Clusters en casa", dq.clusters(client, casa_id))
    if option == 15:
        # Dispositivos en cluster funcional
        casa_id = 'casa_'+(input("Ingresa el id de la casa: "))
        tipo_funcional = input("Ingresa el tipo de cluster funcional (iluminacion, climatizacion, seguridad o entretenimiento): ")
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
        mq.get_cantidad_dispositivos_por_tipo(id_casa, tipo)
    if option_disp == 2:
        #"Buscar dispositivos por nombre"
        nombre_dispositivo = input("Ingresa el nombre del dispositivo: ")
        mq.get_dispositivo_por_nombre(id_casa, nombre_dispositivo)
    if option_disp == 3:
        #"Buscar dispositivos por tipo y estado"
        print_opcion_dispositivos()
        tipo_op = input("Ingresa un tipo de dispositivo (ENTER para OMITIR): ")
        tipo = select_tipo_dispositivo(tipo_op)
        estado = input("Ingresar estado (activo o desactivo). ENTER para OMITIR: ")
        mq.get_dispositivo_por_tipo_estado(id_casa, tipo, estado)
    if option_disp == 4:
        #"Buscar dispositivos por fecha de instalación"
        fecha_instalacion = input("Ingresa fecha (ejem: 2024-06-01): ")
        mq.get_dispositivo_por_fecha_intalacion(id_casa, fecha_instalacion)

def select_opc_menu_configuraciones(id_casa, option_conf):
    if option_conf == 1:
        #"Buscar configuraciones de un dispositivo"
        id_dispositivo = input("Ingresar id de dispositivo: ")
        mq.get_configuraciones_por_dispositivo(id_dispositivo)
    if option_conf == 2:
        #"Buscar configuraciones por nombre",
        nombre_config = input("Ingresa nombre de configuración: ")
        mq.get_config_por_nombre(id_casa, nombre_config)
    if option_conf == 3:
        #"Buscar configuraciones por id",
        config_id = input("Ingresar id de configuración: ")
        mq.get_configuracion_completa(config_id)
    if option_conf == 4:
        #"Buscar configuraciones por hora de encendido",
        hora_on = input("Ingresa hora de encendido (ejem: 19:00): ")
        mq.get_config_por_hora_on(id_casa, hora_on)
    if option_conf == 5:
        #"Buscar configuraciones por tipo de dispositivo",
        print_opcion_dispositivos()
        tipo_op = input("Ingresa un tipo de dispositivo (Enter para omitir): ")
        tipo = select_tipo_dispositivo(tipo_op)
        mq.get_configuracion_por_tipo(id_casa, tipo)
    if option_conf == 6:
        #"Buscar configuraciones por fecha de modificación",
        fecha_modificacion = input("Ingresa fecha (ejem: 2024-06-01): ")
        mq.get_config_por_fecha_modificacion(id_casa, fecha_modificacion)
    if option_conf == 7:
        #"Buscar configuraciones por estado" 
        estado_config = input("Ingresar estado (activo o desactivo). ENTER para OMITIR: ")
        mq.get_config_por_estado(id_casa, estado_config)

def house_selector(username):
    print("Seleccionar Casa")
    user = mq.get_usuario_info(username)
    num_casa = -1
    houses = user[0]['casas']
    hr = pu.house_reader(houses)
    while num_casa not in hr:
        print(pu.nice_house_format(houses))
        print("")
        num_casa = int(input('Selecciona una casa'))
    return hr[num_casa]
    ...


def manage_date():
    print("Elegir un tipo de fecha")
    d_menu = make_menu(f"""Ultimo mes
Hoy
Par de fechas arbitrarias""".split("\n")
    )()

    ttnow = "toTimeStamp(now())"
    option  = int(input())

    if option == 1:
        print("Datos de los ultimos 30 dias")
        return f"{ttnow} - 30d|{ttnow}".split("|")

    if option == 2:
        print("Datos de hoy")
        return f"{ttnow} {ttnow}".split()

    if option == 3:
        l, r = "", ""
        pattern = r"([1-9]\d\d\d)-([01]\d)-([0-3]\d)"
        while( not re.match(pattern, l)): 
            l = input('Ingrese la primera fecha en el formato YYYY-MM-DD\n')
        while( not re.match(pattern, r)): 
            r = input('Ingrese la segunda fecha en el formato YYYY-MM-DD\n')
        print(f"Fecha en rango {l} --> {r}")
        #l += " 00:00:00"
        #r += " 00:00:00"
        return f"{pu.esc(l)}|{pu.esc(r)}+1d".split("|")

def manage_unit():

    s = set(( "celsius", "kWh", "locacion", "state", "on_time"))     
    s.update(("kWh", "state", "on_time"))
    s.update(('intentos_forcejeo', "hora_apertura"))
    s.update(("kWh", "celsius", "door_open_time"))
    s.update(("kWh", "ruta", "state"))
    keys = list(s)
    keys.sort()
    make_menu(keys)()
    print("")
    o = int(input("Seleccionar un tipo de unidad\n"))
    print(o)
    if 0 < o <= len(keys):
        return keys[o]

    return ""

        
def manage_device():
    return input("Insertar id del dispositivo\n")

def manage_yn(s, yes_val, next_menu):

    x = input(s)
    print("")

    if x in "y|si|Y|Si|sI".split("|"):
        return yes_val, next_menu()

    return "", ""

def manage_value():
    return input("Escribe el valor deseado\n")
    return ""

def adv_queries(session, account):
    """
        "aire_acondicionado": ( "celsius", "kWh", "locacion", "state", "on_time")     ,
        "bombilla": ("kWh", "state", "on_time"),
        "cerradura": ('intentos_forcejeo', "hora_apertura"),
        "refrigerador": ("kWh", "celsius", "door_open_time"),
        "aspiradora" : ("kWh", "ruta", "state")
    """
    d_s, d_e = manage_date()
    
    de_par, de_val = manage_yn("Hacer busqueda por dispositivo: y/N", "_de", manage_device  )
    u_par , u_val = manage_yn("Hacer busqueda por tipo de sensor: y/N", "_u", manage_unit ) 
    v_par , v_val = manage_yn("Hacer busqueda por valor: y/N", "_v", manage_value ) 
    if de_val : 
        de_val = uuid.UUID(de_val)
    """
    3
    4
    y
    6ef51108-577b-5b22-8cb1-2d6e8b391509
    """
    get_method = f"cm.get_log_by_a_d{de_par}{u_par}{v_par}(account, d_s, d_e "
    if de_par: 
        get_method += ", de_val"
    if u_val: 
        get_method += ", u_val"
    if v_val: 
        get_method += ", v_val"
    get_method   += ")"

    return eval(get_method)
    

def menu_cassandra(session, username):

    get_methods = cm.create_gets(session)
    print(get_methods)
    id_casa = house_selector(username)
    print("")
    c_menu = make_menu(
        f"""Mostrar todos los logs de la casa
Buscar logs por fecha
Busqueda avanzada """.split("\n")
    )()
    print("")
    query_type = int(input())

    if query_type == 1:
        pu.logs_unpacker(cm.get_all_logs(id_casa))

    if query_type == 2:
        d_s, d_e = manage_date()
        pu.logs_unpacker(cm.get_log_by_a_d(id_casa, d_s, d_e))

    if query_type == 3:
        pu.logs_unpacker(adv_queries(session, id_casa))
        


def main():
    #ingresar username para poder usar la app
    username = set_username()
    mongo_session =  mm.get_session()
    cassandra_session = cm.get_session()
    dgraph_session = dc.DgraphConnection.initialize_dgraph()

    while(True):
        print_menu()
        option = int(input('Ingresa una opción: '))

        if option == 0:
            gen.main()
            #poblar bases de datos
        if option == 1:
            users = mq.get_usuario_info(username)
            for user in users:
                pu.user_printer(user)

        if option == 2:
            #mostrar menu de Cassandra
            menu_cassandra(cassandra_session, username)
            ...
        if option == 3:
            num_casa = set_num_casa()
            id_casa = mq.get_id_casa(num_casa)
            while True:
                print_mongo_menu_dispositivos()
                option_disp = int(input('Ingresa una opción: '))
                if option_disp == 5:
                    # Cambiar casa
                    num_casa = set_num_casa()
                    id_casa = mq.get_id_casa(num_casa)
                elif option_disp == 6:
                    # Regresar a menú principal
                    break
                else:
                    select_opc_menu_dispositivos(id_casa, option_disp)
        if option == 4:
            num_casa = set_num_casa()
            id_casa = mq.get_id_casa(num_casa)
            while True:
                print_mongo_menu_configuraciones()
                option_conf = int(input('Ingresa una opción: '))
                if option_conf == 8:
                    # Cambiar casa
                    num_casa = set_num_casa()
                    id_casa = mq.get_id_casa(num_casa)
                elif option_conf == 9:
                    # Regresar a menú principal
                    break
                else:
                    select_opc_menu_configuraciones(id_casa, option_conf)
        if option == 5:
            while (True):
                print("Menú de relaciones de dispositivos\n")
                print_dgraph_menu()
                option = int(input('Ingresa una opción: '))
                if option == 16:
                    break
                select_opc_menu_relaciones(dgraph_session, option)
        if option == 6:
            username = set_username()
        if option == 7:
            exit(0)


if __name__ == '__main__':
    main()
