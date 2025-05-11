#python -m dataGen.generator
import csv
# Generamos muchos datos aleatorios, esto es muy necesario para el proyecto
import random

# Usamos esta librería para definir fechas
from datetime import datetime, timedelta
from cassandra.util import uuid_from_time
from Conexion import cassandra_model
from Conexion.printing_cassandra_utils import coerce_to_string
from Conexion.mongo_gets import get_x
from Conexion.mongo_model import get_session

from uuid import uuid5, NAMESPACE_DNS
import os
import json  # Generamos JSON en vez de CSV para Dgraph

from bson import ObjectId

from Conexion.dgraph_connection import DgraphConnection #Conexion a Dgraph
from Conexion.dgraph_loader import load_data_to_dgraph # Carga de datos a Dgraph

# De esta variable depende el número de datos creados
#

DAYS = 3
NUM_USUARIOS = 1

# La fecha inicial es hace 30 días
FECHA_INICIAL = datetime.now() - timedelta(days=3)
# La fecha final es ahora mismo
FECHA_FINAL = datetime.now()

# Genera un timestamp aleatorio entre dos fechas
def generar_timestamp_aleatorio(inicio=FECHA_INICIAL, fin=FECHA_FINAL):
    delta = fin - inicio
    segundos_aleatorios = random.randrange(int(delta.total_seconds()))  # Genera un número aleatorio de segundos
    return inicio + timedelta(seconds=segundos_aleatorios)

# Formatea una fecha para que sea hora:minuto, necesario para la generación de datos
def formato_hora(dt):
    return dt.strftime("%H:%M")

# Genera una hora aleatoria
def generar_hora_aleatoria():
    hora = random.randint(0, 23)
    minuto = random.randint(0, 59)
    return f"{hora:02d}:{minuto:02d}" # 02d para que siempre sean dos dígitos

# Genera una duración aleatoria en horas y minutos
def generar_duracion_aleatoria(max_horas=12):
    horas = random.randint(0, max_horas)
    minutos = random.randint(0, 59)
    return f"{horas:02d}:{minutos:02d}" # 02d para que siempre sean dos dígitos

# Genera una temperatura aleatoria para aires acondicionados
def generar_temperatura_aleatoria(min_temp=16, max_temp=30): # min_temp y max_temp son los valores mínimos y máximos de la temperatura
    return round(random.uniform(min_temp, max_temp), 1) # round para que siempre sea un número con un decimal

# Genera un consumo de energía aleatorio para un dispositivo
def generar_consumo_energia_aleatorio(dispositivo):
    rangos = { # Podría ser un solo rango para todos los dispositivos, pero iba a provocar datos absurdos
        "aire_acondicionado": (0.8, 2.5),
        "bombilla": (0.005, 0.015),
        "cerradura": (0.001, 0.005),
        "aspiradora": (0.3, 0.8),
        "refrigerador": (0.1, 0.5)
    }
    min_consumo, max_consumo = rangos.get(dispositivo, (0.1, 1.0)) # Si el dispositivo no está en el diccionario, se usa el rango por defecto
    return round(random.uniform(min_consumo, max_consumo), 3) # round para que siempre sea un número con tres decimales

# Genera una string con un color aleatorio
def generar_color_aleatorio():
    colores = ["blanco cálido", "blanco frío", "amarillo", "azul", "verde", "rojo", "púrpura", "naranja"]
    return random.choice(colores) # Elegir un color aleatorio de la lista

# Genera una string con un estado aleatorio
def generar_estado_aleatorio():
    estados = ["encendido", "apagado", "standby", "modo eco", "error"]
    return random.choice(estados)

# Genera una string con una ruta aleatoria
def generar_ruta_aleatoria():
    rutas = ["completa", "sala principal", "habitaciones", "cocina", "personalizada"] # Incluimos personalizada para no tener demasiadas rutas
    return random.choice(rutas)

#Generar una string con una locacion de la casa aleatoria
def generar_locacion_aleatoria():
    locaciones = ["sala", "dormitorio principal", "dormitorio secundario", "cocina", "comedor", "estudio"]
    locaciones.extend(["habitación de invitados", "sala principal", "habitación principal", "cocina", "oficina"])
    return random.choice(locaciones)

#FUNCIONES QUE SE USAN PARA CREAR DATOS DE MONGO
def validar_hora_on_off():
    while True:
        hora_on = generar_hora_aleatoria()
        hora_off = generar_hora_aleatoria()
        if (hora_off >= hora_on):
            return hora_on, hora_off

def generar_fecha_mongo(inicio, fin):
    fecha_inicio = datetime.strptime(inicio, "%Y-%m-%d")
    fecha_fin = datetime.strptime(fin, "%Y-%m-%d")

    delta_dias = (fecha_fin - fecha_inicio).days
    dias_random = random.randint(0, delta_dias)

    fecha_random = fecha_inicio + timedelta(days=dias_random)
    return fecha_random.strftime("%Y-%m-%d")

def formatear_fecha(fecha):
    return fecha.strftime("%Y-%m-%d")

#funciones para generar el nombre de configuracion segun su tipo
def generar_nombre_config(tipo):
    configuraciones = {
        "aspiradora": ["Modo Turbo", "Limpieza Profunda", "Silencio Nocturno", "Aspiración Express", "Eco Friendly", "Energía Eco", "Eco", "Modo Vacaciones", "Preparación Fiesta de Noche", "Limpieza Pre-Cena Familiar", "Modo Relámpago para Película", "Limpieza Básica para Amigos", "Todo Reluciente"],
        "lavadora": ["Lavado Delicado", "Ropa Blanca Extra", "Eliminación de Manchas", "Lavado Express", "Ahorro de Agua", "Ropa formal", "Ropa delicada", "Lavado emergencia", "Emergencia", "Lavado domingos"],
        "cerradura": ["Modo Seguridad Máxima", "Acceso Familiar", "Bloqueo Automático", "Modo Invitado", "Sin Llaves", "Vacaciones", "Modo fiesta"],
        "refrigerador": ["Modo Super Congelado", "Descongelado Automático", "Energía Eco", "Eco Friendly", "Modo Vacaciones",  "Conservación Extrema", "Bebidas frias: Fiesta", "Cena de noche"],
        "bombilla": ["Luz Relax", "Modo Lectura", "Color Festivo", "Luz de Energía Baja", "Ecologica", "Eco", "Eco Friendly", "Luz Día", "Luz Noche", "Ahorro", "Luces festivas", "Cena romantica", "Luces romanticas", "Modo cine", "Noche de peliculas", "Karaoke", "Luz relajante"],
        "aire_acondicionado": ["Modo Frío Intenso", "Ventilación Suave", "Modo Ahorro Energético", "Clima Tropical", "Calor Reconfortante", "Fresco", "Calido", "Clima por la mañana"]
    }
    nombre = random.choice(configuraciones.get(tipo, ["Configuración desconocida"]))
    return nombre

#genera los dispositivos de mongo
def generar_dispositivos(casa_id, dispositivos_collection, configuraciones_collection):
    fecha_actual = formatear_fecha(FECHA_FINAL)
    dispositivos_for_casaCollection = {"aire_acondicionado": [],"bombilla": [],"refrigerador": [],"cerradura": [],"aspiradora": [],"lavadora": [],}
    dispositivos = []
    num_dispositivos = { 
        "aire_acondicionado": random.randint(1, 3),
        "bombilla": random.randint(5, 15),
        "cerradura": random.randint(1, 6),
        "aspiradora": random.randint(0, 2),
        "refrigerador": random.randint(1, 2),
        "lavadora": random.randint(1, 2)
    }     
    for tipo, cantidad in num_dispositivos.items():
        for i in range(cantidad):
            dispositivo_id = ObjectId()
            nombre = f"{tipo.replace('_', ' ').capitalize()} {i}"
            fecha_instalacion = generar_fecha_mongo("2020-01-01", fecha_actual)
            configuraciones = (generar_configuraciones(tipo, dispositivo_id, configuraciones_collection, fecha_instalacion))
            dispositivos.append({
                "_id": dispositivo_id,
                "id_casa": casa_id,
                "tipo": tipo,
                "nombre_dispositivo": nombre,
                "estado": random.choice(["activo", "desactivo"]),
                "modelo": f"Modelo-{random.randint(1, 50)}",
                "fecha_instalacion": fecha_instalacion,
                "configuraciones": configuraciones
            })
            dispositivos_for_casaCollection[tipo].append({"id_dispositivo": dispositivo_id})
    dispositivos_collection.insert_many(dispositivos)
    return dispositivos_for_casaCollection
        
#genera las configuraciones de un dispositivo
def generar_configuraciones(tipo, dispositivo_id, configuraciones_collection, fecha_instalacion):
    fecha_actual = formatear_fecha(FECHA_FINAL)
    configuraciones_for_dispCollection = []
    configuraciones=[]
    for _ in range(random.randint(1, 5)): 
        hora_on, hora_off = validar_hora_on_off()
        config_result = { 
            "_id": ObjectId(),
            "id_dispositivo": dispositivo_id,
            "nombre_configuracion": generar_nombre_config(tipo),
            "estado_configuracion": random.choice(["activo", "desactivo"]),
            "hora_on": hora_on,
            "hora_off": hora_off,
            "ubicacion": generar_locacion_aleatoria(),
            "fecha_ultima_modificacion": generar_fecha_mongo(fecha_instalacion, fecha_actual),
            "config_especial": generar_configuracion_especial(tipo, hora_on, hora_off)
        }
        configuraciones.append(config_result)
        configuraciones_for_dispCollection.append({"nombre_config":config_result["nombre_configuracion"], "id_config": config_result["_id"]})

    configuraciones_collection.insert_many(configuraciones)
    return configuraciones_for_dispCollection

#genera la configuracion especial
def generar_configuracion_especial(tipo, hora_on, hora_off):
    def generar_hora_autolimpieza(hora_on, hora_off):
        hora_inicial = int(hora_on.split(':')[0])
        hora_final = int(hora_off.split(':')[0])
        hora = f"{random.randint(hora_inicial, hora_final):02}:{random.choice([0, 15, 30, 45]):02}"
        return hora

    configuraciones = {
        "aspiradora": {
            "hora_autolimpieza": generar_hora_autolimpieza(hora_on, hora_off),
            "ruta": generar_ruta_aleatoria()
        },
        "lavadora": {
            "ciclos_lavado": random.sample(["secado", "exprimir", "enjuague", "centrifugado", "lavado"],random.randint(1, 5)),
            "carga": random.choice(["pesada", "ligera", "normal"]),
            "nivel_agua": random.choice(["alta", "media", "baja", "minimo"]),
            "temperatura_agua": random.choice(["fria", "caliente"])
        },
        "bombilla": {
            "brillo": random.randint(10, 100),
            "color": generar_color_aleatorio()
        },
        "aire_acondicionado": {
            "temperatura": generar_temperatura_aleatoria(min_temp=16, max_temp=30),
            "unidad": random.choice(["K", "C°", "F"])
        },
        "refrigerador": {
            "temperatura": generar_temperatura_aleatoria(0, 10),
            "unidad": random.choice(["K", "C°", "F"])
        }
    }
    return configuraciones.get(tipo, {"error": "No se encontro tipo de dispositivo"})

#generar usurios y casas de mongo
#de hay se crean sus dispositivos y configuraciones
def generador_usuarios_casas(usuarios_collection, casas_collection, dispositivos_collection, configuraciones_collection):
    usuarios = []
    casas = []
    count = 1
    for i in range(NUM_USUARIOS):
        user_casas = []
        user_id = ObjectId()
        usuarios_result = {
            "_id": user_id,
            "username": f"user_{i}",
            "correo": f"user{i}@gmail.com",
            "casas": user_casas
        }
        for _ in range(random.randint(1, 3)):
            casa_id = ObjectId()
            num_casa = count
            count += 1
            user_casas.append({"num_casa":num_casa, "id_casa": casa_id})
            casa_dispositivos = generar_dispositivos(casa_id, dispositivos_collection, configuraciones_collection)
            casa_result = {
                "_id": casa_id,
                "id_usuario": user_id,  
                "num_casa": num_casa,
                "dispositivos": casa_dispositivos
            }
            casas.append(casa_result)
        usuarios.append(usuarios_result)
    casas_collection.insert_many(casas)
    usuarios_collection.insert_many(usuarios)

#crear indices de mongo
def crear_indices_mongo(db):
    db.casas.create_index({"num_casa": 1})
    db.dispositivos.create_index({"nombre_dispositivo": "text"})
    db.dispositivos.create_index({"tipo": 1})
    db.dispositivos.create_index({"estado": 1})
    db.dispositivos.create_index({"fecha_instalacion": 1})
    db.dispositivos.create_index({ "id_casa": 1 })
    db.configuraciones.create_index({"estado_configuracion": 1})
    db.configuraciones.create_index({"nombre_configuracion": "text"})
    db.configuraciones.create_index({"fecha_ultima_modificacion": 1})
    db.configuraciones.create_index({"hora_on": 1})
    db.configuraciones.create_index({ "id_dispositivo": 1 })

#funcion para poblar base de datos
def poblar_mongodb(db, usuarios_collection, casas_collection, dispositivos_collection, configuraciones_collection):
    generador_usuarios_casas(usuarios_collection, casas_collection, dispositivos_collection, configuraciones_collection)
    crear_indices_mongo(db)
    print("Datos de Mongo creados correctamente")

#funcion para generar los datos en mongo y el csv con los campos "id_dispositivo", "tipo_dispositivo", "id_casa", "estado"
def generar_datos_mongodb():
    #1) generar session con get_session
    session = get_session() 
    db =  session["intelligent_houses"]
    #2) poblar base de datos de mongo con poblar_mongo
    usuarios_collection = db["usuarios"]
    casas_collection = db["casas"]
    dispositivos_collection = db["dispositivos"]
    configuraciones_collection = db["configuraciones"]
    return poblar_mongodb(db, usuarios_collection, casas_collection, dispositivos_collection, configuraciones_collection)

def get_longitud_casas():
    pipeline = [{
    "$group": {
      "_id": "null", 
      "count": { "$sum": 1 }
    }
    }]
    agg_p = json.dumps(pipeline)
    data = get_x("/casas/agregacion",agg = agg_p)
    return data

def export_data_mongodb():
    #3) llamar a get_x con sufijo a dispsoitivos. Regresa json de todos los dispositivos en base de datos
    #4) crear un mongo_dispositivos.csv con campos: id_dispositivo, "tipo_dispositivo, "id_casa)
   dispositivos = get_x("/dispositivos", )
   with open("dataGen/mongo_dispositivos.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["id_dispositivo", "tipo_dispositivo", "id_casa", "estado"])
        for dispositivo in dispositivos:
            writer.writerow([
                dispositivo.get("_id"),
                dispositivo.get("tipo"),
                dispositivo.get("id_casa"),
                dispositivo.get("estado")
            ])

# Generación de datos para Dgraph (relaciones entre dispositivos)
def generar_datos_dgraph():
    """
    Genera archivos CSV para DGraph con la siguiente estructura (simplifica la carga de datos a Dgraph):
    - dgraph_casas.csv: Datos básicos de las casas
    - dgraph_dispositivos.csv: Información de todos los dispositivos
    - dgraph_clusters.csv: Información de los clusters
    - dgraph_relaciones.csv: Conexiones entre nodos (casa-dispositivo, cluster-dispositivo, dispositivo-dispositivo)
    """
    
    # Crear archivos CSV con sus encabezados
    with open("dataGen/dgraph_casas.csv", "w", newline="", encoding="utf-8") as f_casas, \
         open("dataGen/dgraph_dispositivos.csv", "w", newline="", encoding="utf-8") as f_disp, \
         open("dataGen/dgraph_clusters.csv", "w", newline="", encoding="utf-8") as f_clusters, \
         open("dataGen/dgraph_relaciones.csv", "w", newline="", encoding="utf-8") as f_rel:
        
        # Definir los escritores CSV
        writer_casas = csv.writer(f_casas)
        writer_disp = csv.writer(f_disp)
        writer_clusters = csv.writer(f_clusters)
        writer_rel = csv.writer(f_rel)
        
        # Escribir encabezados
        writer_casas.writerow(["id_casa", "nombre"])
        writer_disp.writerow(["id_dispositivo", "categoria", "estado", "temperatura", "modo", 
                            "ubicacion", "brillo", "color", "potencia", "ruta"])
        writer_clusters.writerow(["id_cluster", "tipo", "categoria", "nombre"])
        writer_rel.writerow(["origen_id", "tipo_relacion", "destino_id"])
        
        # Leer dispositivos existentes del archivo MongoDB
        dispositivos = []
        casas_ids = set()  # Conjunto para almacenar los IDs de casas únicos
        with open("dataGen/mongo_dispositivos.csv", "r", newline="", encoding="utf-8") as f_mongo_disp:
            reader = csv.DictReader(f_mongo_disp)
            for row in reader:
                dispositivos.append((row["id_dispositivo"], row["tipo_dispositivo"], row["id_casa"]))
                casas_ids.add(row["id_casa"])  # Añadir el ID de la casa
        
        # Escribir las casas usando los IDs de Mongo
        for casa_id in casas_ids:
            casa_id_str = f"casa_{casa_id}"
            writer_casas.writerow([casa_id_str, f"Casa {casa_id}"])
        
        # Crear diccionario para agrupar dispositivos por tipo
        dispositivos_por_tipo = {}
        for id_disp, tipo_disp, casa_id in dispositivos:
            if tipo_disp not in dispositivos_por_tipo:
                dispositivos_por_tipo[tipo_disp] = []
            dispositivos_por_tipo[tipo_disp].append((id_disp, casa_id))
        
        # Generar datos para cada casa
        num_casas = get_longitud_casas()[0]["count"]
        for casa_id in range(1, num_casas + 1):
            # Escribir datos de la casa
            casa_id_str = f"casa_{casa_id}"
            writer_casas.writerow([casa_id_str, f"Casa {casa_id}"])
            
            # Filtrar dispositivos de esta casa
            disp_casa = []
            for id_disp, tipo_disp, casa_id in dispositivos:
                # Escribir dispositivos y su relación con la casa
                # Generar datos específicos según el tipo de dispositivo
                estado = generar_estado_aleatorio()
                temp = generar_temperatura_aleatoria() if tipo_disp in ["aire_acondicionado", "refrigerador"] else ""
                modo = random.choice(["auto", "manual"]) if tipo_disp == "aire_acondicionado" else ""
                ubicacion = generar_locacion_aleatoria() if tipo_disp in ["aire_acondicionado", "bombilla", "cerradura"] else ""
                brillo = f"{random.randint(10, 100)}%" if tipo_disp == "bombilla" else ""
                color = generar_color_aleatorio() if tipo_disp == "bombilla" else ""
                potencia = random.randint(1, 3) if tipo_disp == "aspiradora" else ""
                ruta = generar_ruta_aleatoria() if tipo_disp == "aspiradora" else ""
                
                # Escribir dispositivo
                writer_disp.writerow([id_disp, tipo_disp, estado, temp, modo, ubicacion, 
                                    brillo, color, potencia, ruta])
                
                # Escribir relación casa-dispositivo
                writer_rel.writerow([f"casa_{casa_id}", "tiene_dispositivos", id_disp])
                
                if casa_id == str(casa_id):  # Si el dispositivo pertenece a esta casa
                    disp_casa.append((id_disp, tipo_disp, casa_id))
            
            # Generar clusters por habitación
            habitaciones = ["sala", "cocina", "dormitorio_principal", "baño"]
            for hab in habitaciones:
                if random.random() > 0.3:  # 70% probabilidad de tener cluster
                    cluster_id = f"cluster_{casa_id}_{hab}"
                    writer_clusters.writerow([cluster_id, "cluster", "habitacion", f"{hab.capitalize()}_{casa_id}"])
                    
                    # Asignar dispositivos al cluster según ubicación
                    for id_disp, tipo_disp, _ in disp_casa:
                        if random.random() > 0.3:  # 70% probabilidad de asignación
                            writer_rel.writerow([cluster_id, "contiene_dispositivos", id_disp])
            
            # Generar clusters funcionales
            tipos_funcionales = ["iluminacion", "climatizacion", "seguridad", "entretenimiento"]
            for tipo in tipos_funcionales:
                if random.random() > 0.5:  # 50% probabilidad de tener cluster
                    cluster_id = f"cluster_{casa_id}_{tipo}"
                    writer_clusters.writerow([cluster_id, "cluster", "funcional", f"{tipo.capitalize()}_{casa_id}"])
                    
                    # Asignar dispositivos según función
                    for id_disp, tipo_disp, _ in disp_casa:
                        if ((tipo == "iluminacion" and tipo_disp == "bombilla") or
                            (tipo == "climatizacion" and tipo_disp in ["aire_acondicionado", "refrigerador"]) or
                            (tipo == "seguridad" and tipo_disp == "cerradura")):
                            if random.random() > 0.3:  # 70% probabilidad de asignación
                                writer_rel.writerow([cluster_id, "agrupa_dispositivos", id_disp])

        # Generar relaciones de sincronización entre dispositivos del mismo tipo
        for tipo_disp, lista_dispositivos in dispositivos_por_tipo.items():
            # Para cada dispositivo, elegir aleatoriamente otros dispositivos del mismo tipo para sincronizar
            for id_disp, casa_id in lista_dispositivos:
                # Filtrar dispositivos que no sean el actual y estén en otras casas
                otros_dispositivos = [(d_id, c_id) for d_id, c_id in lista_dispositivos 
                                    if d_id != id_disp and c_id != casa_id]
                
                if otros_dispositivos:  # Si hay otros dispositivos disponibles
                    # Elegir un número aleatorio de dispositivos para sincronizar (1 a 3)
                    num_sincronizaciones = random.randint(1, min(3, len(otros_dispositivos)))
                    dispositivos_sincronizar = random.sample(otros_dispositivos, num_sincronizaciones)
                    
                    # Crear las relaciones de sincronización
                    for disp_sync, _ in dispositivos_sincronizar:
                        writer_rel.writerow([id_disp, "sincroniza_con", disp_sync])

    dgraph_client = DgraphConnection.initialize_dgraph()
    data_gen_path = os.path.dirname(os.path.abspath(__file__))
    uids = load_data_to_dgraph(data_gen_path, None)
    print("Datos para Dgraph generados correctamente en formato CSV.")

def load_csv_devices(file):
    dispositivos = []
    with open(file, "r", newline="", encoding="utf-8") as f_disp:
        reader = csv.DictReader(f_disp) # Leer el archivo como un diccionario
        for row in reader:
            if row["estado"] == "activo":  # Solo generar datos para dispositivos activos
                # Añadir el dispositivo a la lista creada unas lineas arriba
                dispositivos.append( { "device_uuid": row["id_dispositivo"], 
                                      "device_type": row["tipo_dispositivo"], 
                                      "account":row["id_casa"]})
    #print(dispositivos)
    return dispositivos

def gen_random_timestamp(current_date):
    return  (current_date + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59)))
# Generación de datos para Cassandra (datos de sensores en tiempo real)
#
def cassandra_log(timestamp, device):
    functions_per_unit = {
        "celsius" : generar_temperatura_aleatoria,
        "kWh" : generar_consumo_energia_aleatorio ,
        "locacion" : generar_locacion_aleatoria,
        "state" : generar_estado_aleatorio,
        "on_time" : generar_duracion_aleatoria,
        "intentos_forcejeo" : (lambda  :  random.randint(1, 5) if random.random() < 0.01 else 0) ,
        "hora_apertura" :  gen_random_timestamp,
        "door_open_time" :  generar_duracion_aleatoria    ,
        "ruta" : generar_ruta_aleatoria

    }

    units_per_device_type = { "aire_acondicionado": (
            "celsius", 
            "kWh", 
            "locacion", 
            "state", "on_time")     ,
        "bombilla": ("kWh", "state", "on_time"),
        "cerradura": ('intentos_forcejeo', "hora_apertura"),
        "refrigerador": ("kWh", "celsius", "door_open_time"),
                             "aspiradora" : ("kWh", "ruta", "state")
                }

    if device["device_type"] not in units_per_device_type: return  [ ]
    functions_per_unit["hora_apertura"] = lambda : gen_random_timestamp(timestamp)
    functions_per_unit['kWh'] = lambda : generar_consumo_energia_aleatorio(device['device_type'])
    result = []
    print_ret = lambda x: (x, uuid5(NAMESPACE_DNS, str(x)))[1]
    #print_ret = (lambda x: (print(x), UUID(str(x)))[1])
    for unit in units_per_device_type[device["device_type"]]:
        result.append (( device["account"], device['device_type'], uuid_from_time(gen_random_timestamp(timestamp)), 
                        print_ret(device['device_uuid']), unit, coerce_to_string(functions_per_unit[unit]()), '') )
    return result

def emit_cassandra_data_from_csv(the_csv, f):
    dispositivos = load_csv_devices(the_csv)
    fecha_actual = FECHA_INICIAL
    while fecha_actual <= FECHA_FINAL:
        print(len(dispositivos))
        l = len(dispositivos)
        for i, device in enumerate(dispositivos):
            print(f"{i}/{l}")
            for log in cassandra_log(fecha_actual, device):
                f(log)

        fecha_actual += timedelta(days=1)
        print(fecha_actual)

def generar_datos_cassandra():
    cassandra_session = cassandra_model.get_session()
    emit_cassandra_data_from_csv("dataGen/mongo_dispositivos.csv", cassandra_model.insert_data(cassandra_session)  )
 
    print("Datos para Cassandra generados correctamente.")
    
# Función principal
def main():
    print(f"Generando datos para {NUM_USUARIOS} usuarios...")
    
    generar_datos_mongodb()
    export_data_mongodb() #generar csv necesarios para dgraph y cassandra
    generar_datos_dgraph()
    generar_datos_cassandra()
  
    print("\nProceso completado.")
    

if __name__ == "__main__":
    main()