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

import os
import uuid
import json  # Generamos JSON en vez de CSV para Dgraph

# De esta variable depende el número de datos creados
NUM_CASAS = 100 

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
    locacion = ["habitación de invitados", "sala principal", "habitación principal", "cocina", "oficina"]
    return random.choice(locacion)

# Genera una string con una locación aleatoria
def generar_locacion_aleatoria():
    locaciones = ["sala", "dormitorio principal", "dormitorio secundario", "cocina", "comedor", "estudio"]
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
    # Si se complica mucho su uso, tengo una versión que genera CSV

    # Archivo para nodos (para los dispositivos)
    nodos_data = []  # Lista para almacenar los nodos
    dispositivos = []  # Para los dispositivos
    
    # Leer dispositivos desde el archivo ya generado para MongoDB
    with open("mongodb_dispositivos.csv", "r", newline="", encoding="utf-8") as f_disp:
        reader = csv.DictReader(f_disp)  # Leer el archivo como un diccionario
        for row in reader:
            # Añadir cada dispositivo a la lista 
            dispositivos.append((row["id_dispositivo"], row["tipo_dispositivo"], row["id_casa"]))
    
    nodos = []  # Inicializar una lista vacía para almacenar los nodos
    for id_dispositivo, tipo_dispositivo, id_casa in dispositivos:  # Para cada atributo de los dispositivos
        id_nodo = str(uuid.uuid4())  # Crear un ID aleatorio
        nombre_nodo = f"{tipo_dispositivo.capitalize()}_{id_casa}_{id_dispositivo[:6]}"  # Crear un nombre para el nodo
        # Crear la estructura del nodo en JSON y prepararlo para el archivo
        nodo = {
            "id_nodo": id_nodo,
            "id_dispositivo": id_dispositivo,
            "tipo_nodo": tipo_dispositivo,
            "nombre_nodo": nombre_nodo,
            "id_casa": id_casa
        }
        nodos_data.append(nodo)  # Añadir el nodo al JSON
        nodos.append((id_nodo, tipo_dispositivo, id_casa))  # Añadir el nodo a la lista para uso posterior
    
    # Guardar nodos en archivo JSON
    with open("dgraph_nodos.json", "w", encoding="utf-8") as f:
        json.dump(nodos_data, f, indent=2) # indent le da la identación correcta
    
    # Listas para clusters
    clusters_data = []
    clusters = []
    
    for casa in range(1, NUM_CASAS + 1):
        # Cluster por casa
        id_cluster_casa = str(uuid.uuid4())
        # Estructura del cluster
        cluster_casa = {
            "id_cluster": id_cluster_casa,
            "nombre_cluster": f"Casa_{casa}",
            "tipo_cluster": "casa",
            "id_casa": casa
        }
        # Añadir el cluster a la lista de datos
        clusters_data.append(cluster_casa)
        # Añadir el cluster a la lista general
        clusters.append((id_cluster_casa, "casa", casa))
        
        # Cluster por habitación (salas)
        habitaciones = ["sala", "cocina", "dormitorio_principal", "baño"]
        # Para cada habitación
        for habitacion in habitaciones:
            # Si la probabilidad es mayor a 0.3, se crea un cluster para la habitación porque...
            if random.random() > 0.3:  # No todas las casas tienen clusters por habitación
                id_cluster_hab = str(uuid.uuid4())
                # Estructura del cluster
                cluster_hab = {
                    "id_cluster": id_cluster_hab,
                    "nombre_cluster": f"{habitacion.capitalize()}_{casa}",
                    "tipo_cluster": habitacion,
                    "id_casa": casa
                }
                # Añadir el cluster a la lista de datos
                clusters_data.append(cluster_hab)
                # Añadir el cluster a la lista general
                clusters.append((id_cluster_hab, habitacion, casa))
        
        # Clusters funcionales
        tipos_funcionales = ["iluminacion", "climatizacion", "seguridad", "entretenimiento"]
        for tipo in tipos_funcionales:
            if random.random() > 0.5:  # No todas las casas tienen todos los clusters funcionales, lo decidimos con un random
                id_cluster_func = str(uuid.uuid4())
                # Estructura del cluster
                cluster_func = {
                    "id_cluster": id_cluster_func,
                    "nombre_cluster": f"{tipo.capitalize()}_{casa}",
                    "tipo_cluster": tipo,
                    "id_casa": casa
                }
                # Lista de datos
                clusters_data.append(cluster_func)
                # Lista
                clusters.append((id_cluster_func, tipo, casa))
    
    # Guardar clusters en archivo JSON
    with open("dgraph_clusters.json", "w", encoding="utf-8") as f:
        json.dump(clusters_data, f, indent=2) # recuperamos clusters_data y lo guardamos en el archivo
    
    # Archivo para relaciones
    relaciones_data = []  # Lista para almacenar las relaciones
    
    # Relaciones entre nodos y clusters (pertenencia)
    for id_nodo, tipo_nodo, id_casa_nodo in nodos:
        # Crear una lista de clusters de casa desde la lista existente 'clusters' si el tipo es casa y el id de la casa es el mismo
        clusters_casa = [c for c in clusters if c[1] == "casa" and c[2] == id_casa_nodo]

        if clusters_casa:
            # Seleccionamos el cluster de casa
            id_cluster_casa = clusters_casa[0][0]
            # Crear un id aleatorio para la relacion
            id_relacion = str(uuid.uuid4())

            # Estructura de la relacion
            relacion = {
                "id_relacion": id_relacion,
                "id_origen": id_nodo,
                "tipo_origen": "dispositivo",
                "id_destino": id_cluster_casa,
                "tipo_destino": "cluster",
                "tipo_relacion": "pertenece_a",
                "peso": 1.0
            }
            # Añadir la relacion a la lista de relaciones
            relaciones_data.append(relacion)
        
        # Conectar con cluster de habitación (basado en el tipo de dispositivo y probabilidad)
        if tipo_nodo == "bombilla":
            habitaciones_posibles = ["sala", "cocina", "dormitorio_principal", "baño"]
        elif tipo_nodo == "aire_acondicionado":
            habitaciones_posibles = ["sala", "dormitorio_principal"]
        elif tipo_nodo == "cerradura":
            habitaciones_posibles = ["entrada", "puerta_trasera"]
        elif tipo_nodo == "aspiradora":
            habitaciones_posibles = ["sala"]
        elif tipo_nodo == "refrigerador":
            habitaciones_posibles = ["cocina"]
        else:
            habitaciones_posibles = ["sala"]
            
        # Para cada habitación posible
        for habitacion in habitaciones_posibles:
            # Crear una lista de clusters de habitación desde la lista existente 'clusters' si el tipo es habitación y el id de la casa es el mismo
            clusters_hab = [c for c in clusters if c[1] == habitacion and c[2] == id_casa_nodo]

            # Si hay clusters de habitación y la probabilidad es mayor a 0.3
            if clusters_hab and random.random() > 0.3:
                # Seleccionamos el cluster de habitación
                id_cluster_hab = clusters_hab[0][0]
                # Crear un id aleatorio para la relacion
                id_relacion = str(uuid.uuid4())
                # Estructura de la relacion
                relacion = {
                    "id_relacion": id_relacion,
                    "id_origen": id_nodo,
                    "tipo_origen": "dispositivo",
                    "id_destino": id_cluster_hab,
                    "tipo_destino": "cluster",
                    "tipo_relacion": "ubicado_en",
                    "peso": round(random.uniform(0.7, 1.0), 2)
                }
                # Añadir la relacion a la lista de relaciones
                relaciones_data.append(relacion)
        
        # Asignar un cluster funcional basado en el tipo de dispositivo
        if tipo_nodo == "bombilla":
            func_posible = "iluminacion"
        elif tipo_nodo == "aire_acondicionado" or tipo_nodo == "refrigerador":
            func_posible = "climatizacion"
        elif tipo_nodo == "cerradura":
            func_posible = "seguridad"
        else:
            func_posible = random.choice(["entretenimiento", "otros"])
            
        # Crear una lista de clusters funcionales desde la lista existente 'clusters' si el tipo es funcional y el id de la casa es el mismo
        clusters_func = [c for c in clusters if c[1] == func_posible and c[2] == id_casa_nodo]
        # Si existen clusters funcionales
        if clusters_func:
            # Seleccionamos el cluster funcional
            id_cluster_func = clusters_func[0][0]
            # Crear un id aleatorio para la relacion
            id_relacion = str(uuid.uuid4())
            relacion = {
                "id_relacion": id_relacion,
                "id_origen": id_nodo,
                "tipo_origen": "dispositivo",
                "id_destino": id_cluster_func,
                "tipo_destino": "cluster",
                "tipo_relacion": "sirve_como",
                "peso": round(random.uniform(0.8, 1.0), 2)
            }
            relaciones_data.append(relacion)
    
    # Relaciones entre dispositivos (interacción)
    for i, (id_nodo1, tipo_nodo1, id_casa1) in enumerate(nodos):
        # Solo relaciones entre dispositivos de la misma casa
        nodos_misma_casa = [(id, tipo, casa) for id, tipo, casa in nodos if casa == id_casa1 and id != id_nodo1]
        
        # Número aleatorio de relaciones (no todos los dispositivos están conectados)
        num_relaciones = random.randint(0, min(3, len(nodos_misma_casa)))
        # Para cada relación
        for _ in range(num_relaciones):
            # Si hay dispositivos de la misma casa
            if nodos_misma_casa:
                # Seleccionamos un dispositivo aleatorio de la misma casa
                id_nodo2, tipo_nodo2, _ = random.choice(nodos_misma_casa)
                # Eliminamos el dispositivo de la lista para evitar duplicados
                nodos_misma_casa.remove((id_nodo2, tipo_nodo2, _))
                
                # Tipo de relación basado en tipos de dispositivos, esta parte es bastante arbitraria, se puede cambiar
                if (tipo_nodo1 == "aire_acondicionado" and tipo_nodo2 == "bombilla") or \
                   (tipo_nodo1 == "bombilla" and tipo_nodo2 == "aire_acondicionado"):
                    tipo_relacion = "sincroniza_con"
                elif (tipo_nodo1 == "cerradura" and tipo_nodo2 == "bombilla") or \
                     (tipo_nodo1 == "bombilla" and tipo_nodo2 == "cerradura"):
                    tipo_relacion = "señaliza"
                else:
                    tipo_relacion = random.choice(["comunica_con", "controla", "depende_de"])
                
                id_relacion = str(uuid.uuid4())
                # Peso aleatorio entre 0.1 y 1.0 a 2 decimales
                peso = round(random.uniform(0.1, 1.0), 2)
                relacion = {
                    "id_relacion": id_relacion,
                    "id_origen": id_nodo1,
                    "tipo_origen": "dispositivo",
                    "id_destino": id_nodo2,
                    "tipo_destino": "dispositivo",
                    "tipo_relacion": tipo_relacion,
                    "peso": peso
                }
                relaciones_data.append(relacion)
    
    # Guardar relaciones en archivo JSON
    with open("dgraph_relaciones.json", "w", encoding="utf-8") as f: # no olvidemos el encoding 
        json.dump(relaciones_data, f, indent=2)
    
    print("Datos para Dgraph generados correctamente.")

# Generación de datos para Cassandra (datos de sensores en tiempo real)
def generar_datos_cassandra():
    # Lee los dispositivos del archivo para MongoDB para mantener consistencia
    dispositivos = []
    with open("mongodb_dispositivos.csv", "r", newline="", encoding="utf-8") as f_disp:
        reader = csv.DictReader(f_disp) # Leer el archivo como un diccionario
        for row in reader:
            if row["activo"] == "True":  # Solo generar datos para dispositivos activos
                # Añadir el dispositivo a la lista creada unas lineas arriba
                dispositivos.append((row["id_dispositivo"], row["tipo_dispositivo"], row["id_casa"]))
    
    # Archivo para sensores de aire acondicionado
    with open("cassandra_aire_acondicionado.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Crear columnas
        writer.writerow([ # Identificadores, tiempo de registro, y los datos que definimos registrar en el word
            "id_registro", "id_dispositivo", "id_casa", "timestamp", 
            "consumo_energia", "temperatura", "hora_encendido", "hora_apagado", 
            "locacion", "estado", "tiempo_encendido"
        ])
        
        # Para cada dispositivo
        for id_dispositivo, tipo_dispositivo, id_casa in dispositivos:
            # Si no es aire acondicionado, se lo salta, para evitar errores
            if tipo_dispositivo != "aire_acondicionado":
                continue
                
            # Genera múltiples registros para cada dispositivo (datos de varios días)
            fecha_actual = FECHA_INICIAL
            # Mientras la fecha actual sea menor o igual a la fecha final
            while fecha_actual <= FECHA_FINAL:
                # ===== IMPORTANTE: No todos los días tienen registros =====
                if random.random() < 0.8:  # 80% de probabilidad de tener un registro en un día
                    id_registro = str(uuid.uuid4())
                    # Genera un timestamp aleatorio para el registro entre la fecha actual y la fecha final
                    timestamp = (fecha_actual + timedelta(hours=random.randint(0, 23), 
                                                        minutes=random.randint(0, 59))).strftime("%Y-%m-%d %H:%M:%S")
                    # Genera un consumo de energía aleatorio para el dispositivo
                    consumo = generar_consumo_energia_aleatorio("aire_acondicionado")
                    # temperatura aleatoria
                    temperatura = generar_temperatura_aleatoria()
                    # random
                    hora_encendido = generar_hora_aleatoria()
                    hora_apagado = generar_hora_aleatoria()
                    locacion = generar_locacion_aleatoria()
                    estado = generar_estado_aleatorio()
                    tiempo_encendido = generar_duracion_aleatoria(8)
                    
                    # Teniendo todos los datos, se escriben en el archivo
                    writer.writerow([
                        id_registro, id_dispositivo, id_casa, timestamp, 
                        consumo, temperatura, hora_encendido, hora_apagado, 
                        locacion, estado, tiempo_encendido
                    ])
                
                fecha_actual += timedelta(days=1)
    
    # Archivo para datos de bombillas
    with open("cassandra_bombillas.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Crear columnas
        writer.writerow([
            "id_registro", "id_dispositivo", "id_casa", "timestamp", 
            "consumo_energia", "configuracion_color", "hora_encendido", "hora_apagado"
        ])
        
        # Para cada dispositivo
        for id_dispositivo, tipo_dispositivo, id_casa in dispositivos:
            # Si no es bombilla, se lo salta
            if tipo_dispositivo != "bombilla":
                continue
                
            # Generar múltiples registros por cada dispositivo
            fecha_actual = FECHA_INICIAL
            while fecha_actual <= FECHA_FINAL:
                # ===== IMPORTANTE: No todos los días tienen registros =====
                if random.random() < 0.9:  # 90% de probabilidad de tener un registro en un día (bombillas se usan más frecuentemente)
                    id_registro = str(uuid.uuid4())
                    # Genera un timestamp aleatorio para el registro entre la fecha actual y la fecha final
                    timestamp = (fecha_actual + timedelta(hours=random.randint(0, 23), 
                                                        minutes=random.randint(0, 59))).strftime("%Y-%m-%d %H:%M:%S")
                    # Datos random
                    consumo = generar_consumo_energia_aleatorio("bombilla")
                    color = generar_color_aleatorio()
                    hora_encendido = generar_hora_aleatoria()
                    hora_apagado = generar_hora_aleatoria()
                    
                    # Escribir al archivo
                    writer.writerow([
                        id_registro, id_dispositivo, id_casa, timestamp, 
                        consumo, color, hora_encendido, hora_apagado
                    ])

                fecha_actual += timedelta(days=1)
    
    # Archivo para datos de cerraduras
    with open("cassandra_cerraduras.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Crear columnas
        writer.writerow([
            "id_registro", "id_dispositivo", "id_casa", "timestamp", 
            "consumo_energia", "hora_apertura", "intentos_forcejeo"
        ])
        
        # Para cada dispositivo
        for id_dispositivo, tipo_dispositivo, id_casa in dispositivos:
            # Si no es cerradura, se lo salta
            if tipo_dispositivo != "cerradura":
                continue
                
            # Generar múltiples registros por cada dispositivo
            fecha_actual = FECHA_INICIAL
            while fecha_actual <= FECHA_FINAL:
                # Varias aperturas por día
                num_aperturas = random.randint(0, 10)
                # Para cada apertura
                for _ in range(num_aperturas):
                    # random
                    id_registro = str(uuid.uuid4())
                    hora = random.randint(0, 23)
                    minuto = random.randint(0, 59)
                    # Genera un timestamp aleatorio para el registro entre la fecha actual y la fecha final y le da formato
                    timestamp = (fecha_actual + timedelta(hours=hora, minutes=minuto)).strftime("%Y-%m-%d %H:%M:%S")
                    # random
                    consumo = generar_consumo_energia_aleatorio("cerradura")
                    # a 2 dígitos
                    hora_apertura = f"{hora:02d}:{minuto:02d}"
                    
                    # Pocos intentos de forcejeo, mayormente 0
                    intentos_forcejeo = 0
                    if random.random() < 0.01:  # 1% de probabilidad de generar un intento de forcejeo
                        intentos_forcejeo = random.randint(1, 5) # random entre 1 y 5 intentos
                    
                    # Escribir al archivo
                    writer.writerow([
                        id_registro, id_dispositivo, id_casa, timestamp, 
                        consumo, hora_apertura, intentos_forcejeo
                    ])
                
                fecha_actual += timedelta(days=1)
    
    # Archivo para aspiradoras
    with open("cassandra_aspiradoras.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Crear columnas
        writer.writerow([
            "id_registro", "id_dispositivo", "id_casa", "timestamp", 
            "consumo_energia", "hora_encendido", "hora_apagado", 
            "hora_auto_limpieza", "ruta", "estado"
        ])
        
        # Para cada dispositivo
        for id_dispositivo, tipo_dispositivo, id_casa in dispositivos:
            # Si no es aspiradora, se lo salta
            if tipo_dispositivo != "aspiradora":
                continue
                
            # Generalmente las aspiradoras no se usan todos los días
            fecha_actual = FECHA_INICIAL
            while fecha_actual <= FECHA_FINAL:
                if random.random() < 0.1:  # 10% de probabilidad (no se usa todos los días)
                    id_registro = str(uuid.uuid4())
                    # Genera timestamp random y le da formato
                    timestamp = (fecha_actual + timedelta(hours=random.randint(8, 18), 
                                                        minutes=random.randint(0, 59))).strftime("%Y-%m-%d %H:%M:%S")
                    # random
                    consumo = generar_consumo_energia_aleatorio("aspiradora")
                    hora_encendido = generar_hora_aleatoria()
                    hora_apagado = generar_hora_aleatoria()
                    
                    # Auto-limpieza ocasional, para los roombas y los irobot, que son aspiradoras IoT
                    hora_auto_limpieza = ""
                    if random.random() < 0.3:  # 30% de probabilidad de auto-limpieza
                        hora_auto_limpieza = generar_hora_aleatoria() 
                    
                    ruta = generar_ruta_aleatoria()
                    estado = generar_estado_aleatorio()
                    
                    # Escribir al archivo
                    writer.writerow([
                        id_registro, id_dispositivo, id_casa, timestamp, 
                        consumo, hora_encendido, hora_apagado, 
                        hora_auto_limpieza, ruta, estado
                    ])
                
                fecha_actual += timedelta(days=1)
    
    # Archivo para datos de sensores (registros de refrigeradores)
    with open("cassandra_refrigeradores.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Crear columnas
        writer.writerow([
            "id_registro", "id_dispositivo", "id_casa", "timestamp", 
            "consumo_energia", "temperatura_interna", "tiempo_puerta_abierta"
        ])
        
        # Para cada dispositivo
        for id_dispositivo, tipo_dispositivo, id_casa in dispositivos:
            # Si no es refrigerador, se lo salta
            if tipo_dispositivo != "refrigerador":
                continue
                
            # Los refrigeradores tienen registros continuos (todos los días)
            fecha_actual = FECHA_INICIAL
            while fecha_actual <= FECHA_FINAL: # Mientras la fecha actual sea menor o igual a la fecha final

                # Esta sección de generar datos para refrigeradores, fue todo un tema ya que tiene muchas variables
                # que se tienen que tener en cuenta y además la manera de medirlos es bastante compleja. Por lo que para
                # esta sección, me tuve que apoyar de varias fuentes externas como StackOverflow y un poco de ChatGPT
                # para poder darle pies y cabeza a todo esto.

                # Varios registros por día (cada pocas horas)
                for hora in range(0, 24, 4):  # Cada 4 horas
                    id_registro = str(uuid.uuid4()) 
                    timestamp = (fecha_actual + timedelta(hours=hora, 
                                                        minutes=random.randint(0, 59))).strftime("%Y-%m-%d %H:%M:%S")
                    
                    # El consumo cambia según la hora del día (más en horas pico)
                    factor_hora = 1.0
                    # Si es hora de comida, el consumo aumenta (asumiendo horas de comida de 11 a 14 y de 18 a 21)
                    if 11 <= hora <= 14 or 18 <= hora <= 21:
                        factor_hora = 1.2  # Mayor consumo en horas de comidas (más aperturas)
                        
                    # Genera un consumo de energía aleatorio para el dispositivo
                    consumo = generar_consumo_energia_aleatorio("refrigerador") * factor_hora
                    
                    # Temperatura interna (normalmente entre 2 y 8 grados)
                    temperatura = round(random.uniform(2.0, 8.0), 1)
                    
                    # Tiempo con puerta abierta (minutos:segundos)
                    minutos = random.randint(0, 5)
                    segundos = random.randint(0, 59)
                    tiempo_puerta_abierta = f"{minutos:02d}:{segundos:02d}"
                    
                    # Escribir al archivo
                    writer.writerow([
                        id_registro, id_dispositivo, id_casa, timestamp, 
                        consumo, temperatura, tiempo_puerta_abierta
                    ])
                
                fecha_actual += timedelta(days=1)
    
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