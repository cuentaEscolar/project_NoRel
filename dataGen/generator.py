# Abrir la consola de VSCode en el directorio 'dataGen' y ejecutar el comando:
# python generator.py
# De esta manera, los archivos se guardarán en el directorio donde está este código
# Si se hace click en el botón de ejecutar de VSCode, los archivos se guardarán en el directorio raíz de VSCode
# Y tendrán que ser copiados a la carpeta correcta

import csv
# Generamos muchos datos aleatorios, esto es muy necesario para el proyecto
import random

# Usamos esta librería para definir fechas
from datetime import datetime, timedelta
from cassandra.util import uuid_from_time
from Conexion.printing_cassandra_utils import coerce_to_string
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

# Crear directorio para los archivos CSV
# os.makedirs("datos_iot", exist_ok=True)

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

# Generación de datos para MongoDB (configuración y metadata)
def generar_datos_mongodb():
    # Archivo para dispositivos
    with open("mongodb_dispositivos.csv", "w", newline="", encoding="utf-8") as f: # Importante el utf-8 o todo se va al carajo
        writer = csv.writer(f)
        # Crear columnas
        writer.writerow(["id_dispositivo", "id_casa", "tipo_dispositivo", "modelo",
                         "fecha_instalacion", "activo"])
        
        # Generar datos de cada casa
        for casa in range(1, NUM_CASAS + 1):
            num_dispositivos = { # Número distinto para cada dispositivo
                "aire_acondicionado": random.randint(1, 3),
                "bombilla": random.randint(5, 15),
                "cerradura": random.randint(1, 4),
                "aspiradora": random.randint(0, 2),
                "refrigerador": random.randint(1, 2)
            }
            
            # Para cada tipo de dispositivo se generan los datos
            for tipo, cantidad in num_dispositivos.items():

                # Cicla entre los dispositivos de un tipo
                for i in range(cantidad):
                    # Asignar id
                    id_dispositivo = str(uuid.uuid4())
                    # Se inventa un modelo con un número aleatorio, porque poner los modelos reales sería demasiado complicado
                    modelo = f"Modelo-{random.randint(100, 999)}"

                    # Generar fecha de instalación al azar
                    fecha_instalacion = generar_timestamp_aleatorio(
                        FECHA_INICIAL - timedelta(days=365*2),  # 2 años antes de la fecha actual
                        FECHA_INICIAL
                    ).strftime("%Y-%m-%d") # Formato de fecha   

                    # Generar estado activo o inactivo donde weights hace que el 95% sean True y el 5% sean False
                    activo = random.choices([True, False], weights=[0.95, 0.05])[0] # [0] para que el resultadosea un booleano
                    
                    # Escribir los datos en el archivo CSV en una fila
                    writer.writerow([
                        id_dispositivo, casa, tipo, modelo, 
                        fecha_instalacion, activo
                    ]) 

    # Archivo para configuraciones de dispositivos
    with open("mongodb_configuraciones.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Crear columnas
        writer.writerow(["id_configuracion", "id_dispositivo", "nombre_configuracion", 
                          "programacion_activa", "hora_encendido", "hora_apagado", 
                          "configuracion_especial", "ultima_modificacion"])
        
        # Leer IDs de los dispositivos ya generados
        dispositivos = []
        with open("mongodb_dispositivos.csv", "r", newline="", encoding="utf-8") as f_disp:
            reader = csv.DictReader(f_disp)
            # Añadir los dispositivos a una lista con append
            for row in reader:
                dispositivos.append((row["id_dispositivo"], row["tipo_dispositivo"], row["id_casa"]))
        
        # Cicla entre los dispositivos
        for id_dispositivo, tipo_dispositivo, id_casa in dispositivos:
            # Número de configuraciones random entre 1 y 3
            num_configuraciones = random.randint(1, 3)

            # Cicla entre las configuraciones generadas arriba
            for i in range(num_configuraciones):
                # Se crea el ID de la configuración
                id_configuracion = str(uuid.uuid4())
                # Se inventa un nombre para la configuración que es config + número
                nombre = f"Config_{i+1}"
                # Elegir si está activo o no
                programacion_activa = random.choice([True, False])
                # Generar hora de encendido al azar
                hora_encendido = generar_hora_aleatoria()
                # Generar hora de apagado al azar
                hora_apagado = generar_hora_aleatoria()
                
                # Generar una configuración especial para el dispositivo
                config_especial = "" # Se inicializa con una string vacía

                # Dependiendo del dispositivo, se genera una configuración especial

                if tipo_dispositivo == "aire_acondicionado":
                    # Genera una temperatura aleatoria con la función que genera entre 16 y 30 grados
                    temperatura = f"{generar_temperatura_aleatoria()}°C"
                    # Determina si el modo es auto o manual con un 50% de probabilidad
                    modo = 'auto' if random.random() > 0.5 else 'manual'
                    # Generarl locacion aleatoria
                    ubicacion = generar_locacion_aleatoria()
                    config_especial = f"temperatura:{temperatura},modo:{modo},ubicacion:{ubicacion}"

                elif tipo_dispositivo == "bombilla":
                    # Genera un brillo aleatorio entre 10 y 100 y crea un color aleatorio
                    config_especial = f"brillo:{random.randint(10, 100)}%,color:{generar_color_aleatorio()}"

                elif tipo_dispositivo == "aspiradora":
                    # Genera una potencia aleatoria entre 1 y 3 y crea una ruta aleatoria
                    config_especial = f"potencia:{random.randint(1, 3)},ruta:{generar_ruta_aleatoria()}"
                
                elif tipo_dispositivo == "refrigerador":
                    # Genera una temperatura aleatoria con la función que genera entre 2 y 8 gr
                    temperatura = f"{generar_temperatura_aleatoria(min_temp=2, max_temp=8)}°C"

                # Genera una fecha de modificación aleatoria
                ultima_modificacion = generar_timestamp_aleatorio().strftime("%Y-%m-%d %H:%M:%S")
                
                # Manda todo al archivo CSV
                writer.writerow([
                    id_configuracion, id_dispositivo, nombre, 
                    programacion_activa, hora_encendido, hora_apagado, 
                    config_especial, ultima_modificacion
                ])
    
    # Print para mostrar que todo salió bien
    print("Datos para MongoDB generados correctamente.")

# Generación de datos para Dgraph (relaciones entre dispositivos)
def generar_datos_dgraph():
    # Contrario a las otras 2 funciones generadoras, esta produce archivos JSON en vez de CSV
    # Ya que esto es más sencillo para insertar datos comparado a usar CSV directamente

    # Sin embargo, esta cosa genera DEMASIADA información, con 100 casas genera un JSON de más de 50k lineas
    # No sé si se me fue la mano con algún ciclo o algo así pero el archivo generado es demasiado grande
    # Tal vez sea bueno revisar esto en el futuro

    # Lista que contendrá todos los nodos y relaciones
    dgraph_data = {"set": []}
    
    # Leer dispositivos existentes del archivo MongoDB
    dispositivos = []
    with open("mongodb_dispositivos.csv", "r", newline="", encoding="utf-8") as f_disp:
        reader = csv.DictReader(f_disp)
        for row in reader:
            dispositivos.append((row["id_dispositivo"], row["tipo_dispositivo"], row["id_casa"]))
    
    # Generar nodos para las casas
    casas = {}
    # Para cada casa extraida del archivo de mongoDB generar los nodos de casa para DGraph
    for casa_id in range(1, NUM_CASAS + 1):
        casa_uid = f"_:casa_{casa_id}"
        casas[casa_id] = casa_uid
        casa_node = {
            "uid": casa_uid,
            "tipo": "casa",
            "id_casa": casa_id,
            "nombre": f"Casa_{casa_id}",
            "tiene_dispositivos": []
        }
        dgraph_data["set"].append(casa_node)
    
    # Generar nodos para los dispositivos y conectarlos a sus casas
    dispositivos_nodes = {}
    # Diccionario para mantener registro de qué dispositivos están en qué ubicación
    dispositivos_por_ubicacion = {}
    
    for id_dispositivo, tipo_dispositivo, id_casa in dispositivos:
        dispositivo_uid = f"_:dispositivo_{id_dispositivo[:8]}"
        dispositivos_nodes[id_dispositivo] = dispositivo_uid
        
        # Asignar una ubicación para todos los tipos de dispositivos que la necesitan
        ubicacion = None
        # Si es alguno de estos 3 dispositivos, generar ubicación al azar
        if tipo_dispositivo in ["aire_acondicionado", "bombilla", "cerradura"]:
            ubicacion = generar_locacion_aleatoria()
            # Registrar el dispositivo en su ubicación
            if (id_casa, ubicacion) not in dispositivos_por_ubicacion:
                dispositivos_por_ubicacion[(id_casa, ubicacion)] = []
            dispositivos_por_ubicacion[(id_casa, ubicacion)].append((id_dispositivo, dispositivo_uid))
        
        # Crear nodo del dispositivo
        dispositivo_node = {
            "uid": dispositivo_uid,
            "tipo": "dispositivo",
            "id_dispositivo": id_dispositivo,
            "categoria": tipo_dispositivo,
            "estado": generar_estado_aleatorio(),
            "pertenece_a": [{"uid": casas[int(id_casa)]}]
        }
        
        # Agregar configuraciones específicas según el tipo de dispositivo
        # Para ACs
        if tipo_dispositivo == "aire_acondicionado":
            dispositivo_node.update({
                "temperatura": f"{generar_temperatura_aleatoria()}°C",
                "modo": random.choice(["auto", "manual"]),
                "ubicacion": ubicacion
            })
        # Para bombillas
        elif tipo_dispositivo == "bombilla":
            dispositivo_node.update({
                "brillo": f"{random.randint(10, 100)}%",
                "color": generar_color_aleatorio(),
                "ubicacion": ubicacion
            })
        # Para aspiradoras
        elif tipo_dispositivo == "aspiradora":
            dispositivo_node.update({
                "potencia": random.randint(1, 3),
                "ruta": generar_ruta_aleatoria()
            })
        # Para refrigeradores
        elif tipo_dispositivo == "refrigerador":
            dispositivo_node.update({
                "temperatura": f"{generar_temperatura_aleatoria(2, 8)}°C"
            })
        # Cerraduras
        elif tipo_dispositivo == "cerradura":
            dispositivo_node.update({
                "ubicacion": ubicacion
            })
        
        # Agregar el dispositivo a la lista de dispositivos de la casa
        casa_node = next(casa for casa in dgraph_data["set"] 
                        if casa["uid"] == casas[int(id_casa)])
        casa_node["tiene_dispositivos"].append({"uid": dispositivo_uid})
        
        dgraph_data["set"].append(dispositivo_node)
    
    # Generar clusters por habitación
    habitaciones = ["sala", "cocina", "dormitorio_principal", "baño"]
    for casa_id in range(1, NUM_CASAS + 1):
        for habitacion in habitaciones:
            if random.random() > 0.3:  # 70% de probabilidad de tener cluster
                cluster_uid = f"_:cluster_{casa_id}_{habitacion}"
                cluster_node = {
                    "uid": cluster_uid,
                    "tipo": "cluster",
                    "categoria": "habitacion",
                    "nombre": f"{habitacion.capitalize()}_{casa_id}",
                    "pertenece_a": [{"uid": casas[casa_id]}],
                    "contiene_dispositivos": []
                }
                
                # Asignar dispositivos al cluster basado en su ubicación
                for id_dispositivo, tipo_dispositivo, id_casa in dispositivos:
                    if int(id_casa) == casa_id:
                        # Obtener el nodo del dispositivo
                        dispositivo_node = next(
                            d for d in dgraph_data["set"] 
                            if d.get("id_dispositivo") == id_dispositivo
                        )
                        
                        # Verificar si el dispositivo tiene ubicación y si corresponde a esta habitación
                        if ("ubicacion" in dispositivo_node and 
                            dispositivo_node["ubicacion"].lower().replace(" ", "_") == habitacion):
                            cluster_node["contiene_dispositivos"].append(
                                {"uid": dispositivos_nodes[id_dispositivo]}
                            )
                
                dgraph_data["set"].append(cluster_node)
    
    # Generar clusters de categoría
    tipos_funcionales = ["iluminacion", "climatizacion", "seguridad", "entretenimiento"]
    for casa_id in range(1, NUM_CASAS + 1):
        for tipo in tipos_funcionales:
            if random.random() > 0.5:  # 50% de probabilidad de tener cluster
                cluster_uid = f"_:cluster_{casa_id}_{tipo}"
                cluster_node = {
                    "uid": cluster_uid,
                    "tipo": "cluster",
                    "categoria": "funcional",
                    "nombre": f"{tipo.capitalize()}_{casa_id}",
                    "pertenece_a": [{"uid": casas[casa_id]}],
                    "agrupa_dispositivos": []
                }
                
                # Asignar dispositivos al cluster según su función
                for id_dispositivo, tipo_dispositivo, id_casa in dispositivos:
                    if int(id_casa) == casa_id:
                        if ((tipo == "iluminacion" and tipo_dispositivo == "bombilla") or
                            (tipo == "climatizacion" and tipo_dispositivo in ["aire_acondicionado", "refrigerador"]) or
                            (tipo == "seguridad" and tipo_dispositivo == "cerradura")):
                            if random.random() > 0.3:  # 70% de probabilidad de asignación
                                cluster_node["agrupa_dispositivos"].append(
                                    {"uid": dispositivos_nodes[id_dispositivo]}
                                )
                
                dgraph_data["set"].append(cluster_node)
    
    # Generar relaciones entre dispositivos
    for id_dispositivo1, tipo_dispositivo1, id_casa1 in dispositivos:
        # Filtrar dispositivos de la misma casa
        dispositivos_misma_casa = [
            (id2, tipo2) for id2, tipo2, casa2 in dispositivos 
            if casa2 == id_casa1 and id2 != id_dispositivo1
        ]
        
        # Generar conexiones aleatorias
        num_conexiones = random.randint(0, min(3, len(dispositivos_misma_casa)))
        for _ in range(num_conexiones):
            if dispositivos_misma_casa:
                id_dispositivo2, tipo_dispositivo2 = random.choice(dispositivos_misma_casa)
                dispositivos_misma_casa.remove((id_dispositivo2, tipo_dispositivo2))
                
                # Si es alguno de estos dispositivos, se agrega esta relación
                if ((tipo_dispositivo1 == "aire_acondicionado" and tipo_dispositivo2 == "bombilla") or 
                    (tipo_dispositivo1 == "bombilla" and tipo_dispositivo2 == "aire_acondicionado")):
                    tipo_relacion = "sincroniza_con"
                    
                    # Agregar la relación al dispositivo origen
                    dispositivo_origen = next(d for d in dgraph_data["set"] 
                                           if d["uid"] == dispositivos_nodes[id_dispositivo1])
                    if tipo_relacion not in dispositivo_origen:
                        dispositivo_origen[tipo_relacion] = []
                    dispositivo_origen[tipo_relacion].append({
                        "uid": dispositivos_nodes[id_dispositivo2],
                        "peso": round(random.uniform(0.1, 1.0), 2)
                    })
    
    # Guardar todo en un solo archivo JSON
    with open("dgraph_data.json", "w", encoding="utf-8") as f:
        json.dump(dgraph_data, f, indent=2, ensure_ascii=False)
    
    print("Datos para Dgraph generados correctamente en formato de mutación.")

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

