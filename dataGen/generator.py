# Abrir la consola de VSCode en el directorio 'dataGen' y ejecutar el comando:
# python generator.py
# De esta manera, los archivos se guardarán en el directorio donde está este código
# Si se hace click en el botón de ejecutar de VSCode, los archivos se guardarán en el directorio raíz de VSCode
# Y tendrán que ser copiados a la carpeta correcta
#python -m dataGen.generator
import csv
# Generamos muchos datos aleatorios, esto es muy necesario para el proyecto
import random

# Usamos esta librería para definir fechas
from datetime import datetime, timedelta
from cassandra.util import uuid_from_time
from Conexion.printing_cassandra_utils import coerce_to_string
from Conexion.mongo_gets import get_x
from Conexion.mongo_model import base_populate, get_session
from uuid import UUID

import os
import uuid
import json  # Generamos JSON en vez de CSV para Dgraph

# De esta variable depende el número de datos creados
NUM_CASAS = 10

# La fecha inicial es hace 30 días
FECHA_INICIAL = datetime.now() - timedelta(days=30)
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

def generar_datos_mongodb():
   #1) generar session con get_session
   session = get_session()
   #2) poblar base de datos de mongo con base_populate
   base_populate(session)
   #3) llamar a get_x con sufijo a dispsoitivos. Regresa json de todos los dispositivos en base de datos
   dispositivos = get_x("/dispositivos", )
   #4) crear un mongo_dispositivos.csv con campos: id_dispositivo, "tipo_dispositivo, "id_casa)
   with open("mongo_dispositivos.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["id_dispositivo", "tipo_dispositivo", "id_casa"])
        for dispositivo in dispositivos:
            writer.writerow([
                dispositivo.get("_id"),
                dispositivo.get("tipo"),
                dispositivo.get("id_casa")
            ])
    
   

# Generación de datos para Dgraph (relaciones entre dispositivos)
def generar_datos_dgraph():
    """
    Genera archivos CSV para DGraph con la siguiente estructura (simplifica la carga de datos a Dgraph):
    - casas.csv: Datos básicos de las casas
    - dispositivos.csv: Información de todos los dispositivos
    - clusters.csv: Información de los clusters
    - relaciones.csv: Conexiones entre nodos (casa-dispositivo, cluster-dispositivo, dispositivo-dispositivo)
    """
    
    # Crear archivos CSV con sus encabezados
    with open("dgraph_casas.csv", "w", newline="", encoding="utf-8") as f_casas, \
         open("dgraph_dispositivos.csv", "w", newline="", encoding="utf-8") as f_disp, \
         open("dgraph_clusters.csv", "w", newline="", encoding="utf-8") as f_clusters, \
         open("dgraph_relaciones.csv", "w", newline="", encoding="utf-8") as f_rel:
        
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
        with open("mongodb_dispositivos.csv", "r", newline="", encoding="utf-8") as f_mongo_disp:
            reader = csv.DictReader(f_mongo_disp)
            for row in reader:
                dispositivos.append((row["id_dispositivo"], row["tipo_dispositivo"], row["id_casa"]))
        
        # Crear diccionario para agrupar dispositivos por tipo
        dispositivos_por_tipo = {}
        for id_disp, tipo_disp, casa_id in dispositivos:
            if tipo_disp not in dispositivos_por_tipo:
                dispositivos_por_tipo[tipo_disp] = []
            dispositivos_por_tipo[tipo_disp].append((id_disp, casa_id))
        
        # Generar datos para cada casa
        for casa_id in range(1, NUM_CASAS + 1):
            # Escribir datos de la casa
            casa_id_str = f"casa_{casa_id}"
            writer_casas.writerow([casa_id_str, f"Casa {casa_id}"])
            
            # Filtrar dispositivos de esta casa
            disp_casa = [d for d in dispositivos if int(d[2]) == casa_id]
            
            # Escribir dispositivos y su relación con la casa
            for id_disp, tipo_disp, _ in disp_casa:
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
                writer_rel.writerow([casa_id_str, "tiene_dispositivos", id_disp])
            
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
    
    print("Datos para Dgraph generados correctamente en formato CSV.")

def load_csv_devices(file):
    dispositivos = []
    with open(file, "r", newline="", encoding="utf-8") as f_disp:
        reader = csv.DictReader(f_disp) # Leer el archivo como un diccionario
        for row in reader:
            if row["activo"] == "True":  # Solo generar datos para dispositivos activos
                # Añadir el dispositivo a la lista creada unas lineas arriba
                dispositivos.append( { "device_uuid": row["id_dispositivo"], 
                                      "device_type": row["tipo_dispositivo"], 
                                      "account":row["id_casa"]})
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
    for unit in units_per_device_type[device["device_type"]]:
        result.append (( device["account"], device['device_type'], uuid_from_time(gen_random_timestamp(timestamp)), 
                        UUID(device['device_uuid']), unit, coerce_to_string(functions_per_unit[unit]()), '') )
    return result

def emit_cassandra_data_from_csv(the_csv, f):
    dispositivos = load_csv_devices(the_csv)
    fecha_actual = FECHA_INICIAL
    while fecha_actual <= FECHA_FINAL:
        for device in dispositivos:
            for log in cassandra_log(fecha_actual, device):
                f(log)
        fecha_actual += timedelta(days=1)

def generar_datos_cassandra():
    # Lee los dispositivos del archivo para MongoDB para mantener consistencia
    with open("cassandra_logs.csv", "w", newline="", encoding="utf-8") as f:

        writer = csv.writer(f)
        writer.writerow([ "account", "device_type", "log_date", "device", 
            "unit", "value", "comment" ])
        emit_cassandra_data_from_csv("mongodb_dispositivos.csv", writer.writerow)
    print("Datos para Cassandra generados correctamente.")
    

# Función principal
def main():
    print(f"Generando datos para {NUM_CASAS} casas...")
    
    generar_datos_mongodb()
    generar_datos_dgraph()
    generar_datos_cassandra()
    
    print("\nProceso completado. Los archivos CSV se han guardado en el directorio actual.")
    print("Resumen de archivos generados:")
    # Esta parte da un resumen de los archivos generados y su tamaño
    for archivo in os.listdir("."):
        if archivo.endswith(".py"): # Si es un archivo .py (como este código), se lo salta para no mostrarlo en el resumen
            continue
        tamaño = os.path.getsize(f"{archivo}") / 1024  # Tamaño en KB
        print(f"- {archivo}: {tamaño:.2f} KB")

if __name__ == "__main__":
    main()