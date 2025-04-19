import random
import datetime
from bson import ObjectId

NUM_USUARIOS = 10

def generar_hora():
    hora = random.randint(0, 23)
    minuto = random.randint(0,59)
    return f"{hora:02}:{minuto:02}"

def validar_hora_on_off():
    while True:
        hora_on = generar_hora()
        hora_off = generar_hora()
        if (hora_off >= hora_on):
            return hora_on, hora_off

def generar_fecha(inicio, fin):
    fecha_inicio = datetime.datetime.strptime(inicio, "%Y-%m-%d")
    fecha_fin = datetime.datetime.strptime(fin, "%Y-%m-%d")

    delta_dias = (fecha_fin - fecha_inicio).days
    dias_random = random.randint(0, delta_dias)

    fecha_random = fecha_inicio + datetime.timedelta(days=dias_random)
    return fecha_random.strftime("%Y-%m-%d")

def formatear_fecha(fecha):
    return fecha.strftime("%Y-%m-%d")

def generar_nombre_config(tipo):
    configuraciones = {
        "aspiradora": ["Modo Turbo", "Limpieza Profunda", "Silencio Nocturno", "Aspiración Express", "Eco Friendly", "Energía Eco", "Eco", "Modo Vacaciones", "Preparación Fiesta de Noche", "Limpieza Pre-Cena Familiar", "Modo Relámpago para Película", "Limpieza Básica para Amigos", "Todo Reluciente"],
        "lavadora": ["Lavado Delicado", "Ropa Blanca Extra", "Eliminación de Manchas", "Lavado Express", "Ahorro de Agua", "Ropa formal", "Ropa delicada", "Lavado emergencia", "Emergencia", "Lavado domingos"],
        "cerraduras": ["Modo Seguridad Máxima", "Acceso Familiar", "Bloqueo Automático", "Modo Invitado", "Sin Llaves", "Vacaciones", "Modo fiesta"],
        "refrigerador": ["Modo Super Congelado", "Descongelado Automático", "Energía Eco", "Eco Friendly", "Modo Vacaciones",  "Conservación Extrema", "Bebidas frias: Fiesta", "Cena de noche"],
        "bombilla": ["Luz Relax", "Modo Lectura", "Color Festivo", "Luz de Energía Baja", "Ecologica", "Eco", "Eco Friendly", "Luz Día", "Luz Noche", "Ahorro", "Luces festivas", "Cena romantica", "Luces romanticas", "Modo cine", "Noche de peliculas", "Karaoke", "Luz relajante"],
        "aire_acondicionado": ["Modo Frío Intenso", "Ventilación Suave", "Modo Ahorro Energético", "Clima Tropical", "Calor Reconfortante", "Fresco", "Calido", "Clima por la mañana"]
    }
    nombre = random.choice(configuraciones.get(tipo, ["Configuración desconocida"]))
    return {"nombre_config": nombre}

def generar_color_aleatorio():
    colores = ["blanco cálido", "blanco frío", "amarillo", "azul", "verde", "rojo", "morado", "naranja"]
    return random.choice(colores) 

def generar_locacion_aleatoria(tipo):
    locacion = ["habitación de invitados", "sala principal", "habitación principal", "cocina", "oficina", "jardin", "baño","cuarto de lavado", "comedor", "sala secundaria", "cochera"]
    if tipo == "refrigerador":
        return "cocina"
    elif tipo == "lavadora":
        return "cuarto de lavado"
    return random.choice(locacion)
    
def generar_dispositivos(casa_id, dispositivos_collection, configuraciones_collection):
    fecha_actual = formatear_fecha(datetime.datetime.now())
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
            nombre = f"{tipo.capitalize()}-{i}"
            fecha_instalacion = generar_fecha("2020-01-01", fecha_actual)
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
        
def generar_configuracion_especial(tipo, hora_on, hora_off):
    def generar_hora_autolimpieza(hora_on, hora_off):
        hora_inicial = int(hora_on.split(':')[0])
        hora_final = int(hora_off.split(':')[0])
        hora = f"{random.randint(hora_inicial, hora_final):02}:{random.choice([0, 15, 30, 45]):02}"
        return hora

    configuraciones = {
        "aspiradora": {
            "hora_autolimpieza": generar_hora_autolimpieza(hora_on, hora_off),
            "ruta": random.choice(["Limpieza completa", "Limpieza ligera", "Limpieza de dormitorios", "Ruta sala","Ruta cocina", "Ruta completa", "Limpieza express", "Limpieza de noche"]),
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
            "temperatura": random.randint(15, 30),
            "unidad": random.choice(["K", "C°", "F"])
        },
        "refrigerador": {
            "temperatura": random.randint(0, 10),
            "unidad": random.choice(["K", "C°", "F"])
        }
    }
    return configuraciones.get(tipo, {"error": "No se encontro tipo de dispositivo"})

def generar_configuraciones(tipo, dispositivo_id, configuraciones_collection, fecha_instalacion):
    fecha_actual = formatear_fecha(datetime.datetime.now())
    configuraciones_for_dispCollection = []
    configuraciones=[]
    for i in range(random.randint(1, 5)): 
        hora_on, hora_off = validar_hora_on_off()
        config_result = { 
            "_id": ObjectId(),
            "id_dispositivo": dispositivo_id,
            "nombre_configuracion": generar_nombre_config(tipo),
            "estado_configuracion": random.choice(["activo", "desactivo"]),
            "hora_on": hora_on,
            "hora_off": hora_off,
            "ubicacion": generar_locacion_aleatoria(tipo),
            "fecha_ultima_modificacion": generar_fecha(fecha_instalacion, fecha_actual),
            "config_especial": generar_configuracion_especial(tipo, hora_on, hora_off)
        }
        configuraciones.append(config_result)
        configuraciones_for_dispCollection.append({"nombre_config":config_result["nombre_configuracion"], "id_config": config_result["_id"]})

    configuraciones_collection.insert_many(configuraciones)
    return configuraciones_for_dispCollection

def crearIndices(db):
    return

def generador(usuarios_collection, casas_collection, dispositivos_collection, configuraciones_collection):
    usuarios = []
    casas = []
    count = 1
    for i in range(NUM_USUARIOS):
        user_casas = []
        user_id = ObjectId()
        usuarios_result = {
            "_id": user_id,
            "username": f"user_{i}",
            "correo": f"user{i}@example.com",
            "casas": user_casas
        }
        for j in range(random.randint(1, 3)):
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

def poblar_mongodb(db):
    usuarios_collection = db["usuarios"]
    casas_collection = db["casas"]
    dispositivos_collection = db["dispositivos"]
    configuraciones_collection = db["configuraciones"]
    generador(usuarios_collection, casas_collection, dispositivos_collection, configuraciones_collection)