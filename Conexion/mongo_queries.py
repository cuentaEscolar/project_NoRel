import Conexion.mongo_gets
from bson.objectid import ObjectId
import json


def print_x(x):
    for k in x.keys():
        print(f"{k}: {x[k]}")
    print("="*50)
#funcion para imprimir los resultados
def print_data(data): 
    if not data:
        print("No se recibieron datos.")
    else:
        for item in data:
            print_x(item)


#funcion para obtener el id de la casa 
# evitar hacer las agregaciones desde la coleccion de casas siempre
def get_id_casa(casa):
    data = Conexion.mongo_gets.get_casas(num_casa = casa)
    for casa in data:
        return casa["_id"]

#1.Casas del usuario 	
# El usuario puede ver los datos de su cuenta y sus propiedades. 
# Se da un username.	
# Se muestra el username, correo, y num_casa de las casas de su propiedad.
def get_usuario_info(usuario):
    data = Conexion.mongo_gets.get_usuarios(username = usuario)
    for user in data:
        Conexion.mongo_gets.print_x(user)

#2.Configuración on/off.	
# El usuario puede ver la configuración de prendido y apagado de los dispositivos (si esta activa esa configuración) de su casa 
# dado un tipo.	
# Se muestra nombre_dispositivo, el nombre_configuración (si esta activa), la hora_on y hora_off. 
# En el caso de aspiradoras también se muestra la hora_autolimpieza.
def get_configuracion_horario(id_casa, tipo):
    agg_pipeline = [
        {
           "$match": {
            "id_casa": id_casa,
            "tipo": tipo
        }
        },{
            "$lookup": {
                "from": "configuraciones",
                "localField": "_id",
                "foreignField": "id_dispositivo",
                "as": "configuraciones_info"
            }
        },
        { "$unwind": "$configuraciones_info" }, 
        {
            "$match": { "configuraciones_info.estado_configuracion": "activo" }
        },
        {
            "$project": {
                "_id": 0,
                "nombre_dispositivo": 1,                     
                "nombre_configuracion": "$configuraciones_info.nombre_configuracion",
                "hora_on": "$configuraciones_info.hora_on",
                "hora_off": "$configuraciones_info.hora_off",
                "hora_autolimpieza": "$configuraciones_info.config_especial.hora_autolimpieza"
            }
        }
    ]
    agg = json.dumps(agg_pipeline)
    data = Conexion.mongo_gets.get_dispositivos_con_agregacion(agg)
    print_data(data)
    
#3.Dispositivos por casas.	
# El usuario puede ver la lista de dispositivos que tiene en su casa y si están activos o no. 
# Da un tipo .	
# Se muestra el id_dispositivo, tipo, nombre_dispositivo.
def get_dispositivo_por_tipo_estado(id_casa, tipo, estado):
    agg_pipeline = [
    {
           "$match": {
            "id_casa": id_casa,
            "tipo": tipo,
            "estado": estado
        }
    },
    {
        "$project": {
            "_id": 1,
            "nombre_dispositivo": 1,                     
            "tipo": 1,
            "estado": 1
        }
    }
    ]
    agg = json.dumps(agg_pipeline, default=str)
    data = Conexion.mongo_gets.get_dispositivos_con_agregacion(agg)
    print_data(data)
    

#4.Configuraciones por dispositivo.	
#El usuario puede ver todas las configuraciones de un dispositivo en específico 
# dado un id.	
#Se muestra el nombre_dispositivo, el estado_configuración, nombre_configuración.
def get_configuraciones_por_dispositivo(id_dispositivo):
    agg_pipeline = [
        {
            "$match": {
                "_id": id_dispositivo
            }
        },
        {
            "$lookup": {
                "from": "configuraciones",
                "localField": "_id",
                "foreignField": "id_dispositivo",
                "as": "configuraciones_info"
            }
        },
        { "$unwind": "$configuraciones_info" },
        {
            "$project": {
                "_id": 0,
                "nombre_dispositivo": 1,
                "estado_configuracion": "$configuraciones_info.estado_configuracion",
                "nombre_configuracion": "$configuraciones_info.nombre_configuracion"
            }
        }
    ]
    agg = json.dumps(agg_pipeline)
    data = Conexion.mongo_gets.get_dispositivos_con_agregacion(agg)
    print_data(data)


#5.Configuraciones por nombre.	
# El usuario puede ver las configuraciones con nombre que contengan un texto especifico. 	
# Se muestra el id_configuracion, nombre_configuración, 
# estado_configuracion, fecha_ultima_modificacion, y el nombre_dispositivo al que pertenece esa configuración. }
def get_config_por_nombre(id_casa, nombre_config):
    agg_pipeline = [
        {
            "$match": {
                "$text": { "$search": nombre_config }
            }
        },
        {
            "$lookup": {
                "from": "dispositivos",
                "localField": "id_dispositivo",
                "foreignField": "_id",
                "as": "dispositivo_info"
            }
        },
        { "$unwind": "$dispositivo_info" },
        {
            "$match": {
                "dispositivo_info.id_casa": id_casa
            }
        },
        {
            "$project": {
                "_id": 1,
                "nombre_dispositivo": "$dispositivo_info.nombre_dispositivo",
                "nombre_configuracion": 1,
                "estado_configuracion": 1,
                "fecha_ultima_modificacion": 1
            }
        }
    ]
    agg = json.dumps(agg_pipeline)
    data = Conexion.mongo_gets.get_configuraciones_con_agregacion(agg)
    print_data(data)


#6.Configuración completa dado un id.	
# El usuario puede ver la configuración completa de un dispositivo dado su id_configuracion.	
# Se muestra el nombre_dispositivo, hora_on, hora_off, fecha_modificacion, configuración_especial, estado configuración, ubicación.
def get_configuracion_completa(config_id):
    data = Conexion.mongo_gets.get_configuraciones(_id = config_id)
    print_data(data)

#7.Configuraciones por fecha de modificación de un dispositivo.	
# El usuario puede ver historial de configuraciones dada una fecha de modificación.	
# Se muestra el id_configuracion, nombre_configuracion, fecha_ultima_modificacion, id_dispositivo.
def get_config_por_fecha_modificacion():
    ...

#8.Configuraciones por hora de encendido.	
# El usuario puede ver las configuraciones dada una hora_on. 	
# Se muestra nombre_config, hora_on, hora_off, nombre_dispositivo.
def get_config_por_hora_on():
    ...

#9.Número de tipo dispositivos en una casa.	
# El usuario puede ver cuántos tipos de dispositivo tienen en su casa. 
# Se da un tipo.	
# Se muestra num_casa, tipo_dispositivos, y la cantidad.
def get_cantidad_dispositivos_por_tipo():
    ...

#10.Dispositivo por fecha de instalación.	
# El usuario puede ver los dispositivos en orden ascendente dada una fecha_instalacion. 	
# Se muestra nombre_dispositivo, id_dipsositivo, fecha_instalacion.
def get_dispositivo_por_fecha_intalacion():
    ...

#11. Configuraciones dado estado.	
# El usuario puede ver sus configuraciones activas o desactivas. 
# Se da el estado.	
# Se muestra nombre_configuracion, nombre_dispositivo, estado.
def get_config_por_estado(id_casa, estado_config):
    agg_pipeline = [
        {
            "$match": { "id_casa": id_casa }
        },
        {
            "$lookup": {
                "from": "configuraciones",
                "localField": "_id",
                "foreignField": "id_dispositivo",
                "as": "configuraciones_info"
            }
        },
        { "$unwind": "$configuraciones_info" },
        {
            "$match": { "configuraciones_info.estado_configuracion": estado_config }
        },
        {
            "$project": {
                "_id": 0,
                "nombre_dispositivo": 1,
                "nombre_configuracion": "$configuraciones_info.nombre_configuracion",
                "estado_configuracion": "$configuraciones_info.estado_configuracion"
            }
        }
    ]
    agg = json.dumps(agg_pipeline)
    data = Conexion.mongo_gets.get_dispositivos_con_agregacion(agg)
    print_data(data)


#12. Dispositivos por nombre.	
# El usuario puede ver las configuraciones con nombre que contengan un texto especifico. 	
# Se muestra el id_dispositivo, nombre_dispositivo, modelo, fecha_instalacion.
def get_dispositivo_por_nombre(id_casa, nombre_dispositivo):
    agg_pipeline = [
        {
            "$match": { "id_casa": id_casa,
                        "$text": { "$search": nombre_dispositivo}
                    }
        },
        {
            "$project": {
                "_id": 1,
                "nombre_dispositivo": 1,  
                "modelo": 1,  
                "fecha_instalacion": 1                 
            }
        }
    ]
    agg = json.dumps(agg_pipeline)
    data = Conexion.mongo_gets.get_dispositivos_con_agregacion(agg)
    print_data(data)
    