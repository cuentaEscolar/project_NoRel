import Conexion.mongo_gets
import json

def print_dict(x):
    for k in x.keys():
        print(f"{k}: {x[k]}")
    print("="*50)

def print_data(data): 
    if not data:
        print("No se recibieron datos.")
    else:
        for item in data:
            print_dict(item)

def ejecutar_agregacion(pipeline, suff):
    agg_p = json.dumps(pipeline)
    data = Conexion.mongo_gets.get_x(suff,agg = agg_p)
    print_data(data)

#funcion para obtener el id de la casa 
def get_id_casa(casa):
    data = Conexion.mongo_gets.get_x("/casas",num_casa = casa)
    for casa in data:
        return casa["_id"]

#funcion para hacer el match segun que datos se den para la busqueda
def crear_match(id_casa, tipo, estado):
    match = {"id_casa": id_casa}
    if tipo:
        match["tipo"] = tipo
    if estado:
        match["estado"] = estado
    return {"$match": match}


#1.Info del usuario 	
def get_usuario_info(usuario):
    data = Conexion.mongo_gets.get_x("/usuarios",username = usuario)
    return data
    for user in data:
        print_dict(user)

#2.Configuraciones de un tipo de dispositivo #TIPO es opcional
def get_configuracion_por_tipo(id_casa, tipo):
    agg_pipeline = [
        crear_match(id_casa, tipo, None),
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
                "id_configuracion": "$configuraciones_info._id",              
                "nombre_configuracion": "$configuraciones_info.nombre_configuracion",
                "estado_configuracion": "$configuraciones_info.estado_configuracion",
                "hora_on": "$configuraciones_info.hora_on",
                "hora_off": "$configuraciones_info.hora_off",
                "hora_autolimpieza": "$configuraciones_info.config_especial.hora_autolimpieza"
            }
        }
    ]
    ejecutar_agregacion(agg_pipeline, "/dispositivos/agregacion")
    
#3.Dispositivos por casas y tipo.	#TIPO Y ESTADO es OPCIONAL
def get_dispositivo_por_tipo_estado(id_casa, tipo, estado):
    agg_pipeline = [
    crear_match(id_casa,tipo,estado),
    {
        "$project": {
            "_id": 1,
            "nombre_dispositivo": 1,   
            "modelo": 1,                  
            "tipo": 1,
            "estado": 1
        }
    }
    ]
    ejecutar_agregacion(agg_pipeline, "/dispositivos/agregacion")
    
#4.Configuraciones por dispositivo id.	
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
                "id_configuracion": "$configuraciones_info._id",
                "estado_configuracion": "$configuraciones_info.estado_configuracion",
                "nombre_configuracion": "$configuraciones_info.nombre_configuracion"
            }
        }
    ]
    ejecutar_agregacion(agg_pipeline, "/dispositivos/agregacion")

#5.Configuraciones por nombre.	
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
    ejecutar_agregacion(agg_pipeline, "/configuraciones/agregacion")

#6.Configuración completa dado un id.	
def get_configuracion_completa(config_id):
    data = Conexion.mongo_gets.get_x("/configuraciones",_id = config_id)
    print_data(data)

#7.Configuraciones por fecha de modificación.	
def get_config_por_fecha_modificacion(id_casa, fecha_modificacion):
    agg_pipeline = [
        {
            "$match": {
                "id_casa": id_casa
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
            "$match": {
                "configuraciones_info.fecha_ultima_modificacion": {
                    "$gte": fecha_modificacion
                }
            }
        },
        {
            "$sort": {
                "configuraciones_info.fecha_ultima_modificacion": 1 
            }
        },
        {
            "$project": {
                "_id": 1,
                "nombre_dispositivo": "$nombre_dispositivo",
                "nombre_configuracion": "$configuraciones_info.nombre_configuracion",
                "id_configuracion": "$configuraciones_info._id",
                "fecha_ultima_modificacion": "$configuraciones_info.fecha_ultima_modificacion"
            }
        }
    ]
    ejecutar_agregacion(agg_pipeline, "/dispositivos/agregacion")

#8.Configuraciones por hora de encendido.	
def get_config_por_hora_on(id_casa, hora_on):
    agg_pipeline = [
        {
            "$match": {
                "id_casa": id_casa
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
            "$match": {
                "configuraciones_info.hora_on": {
                    "$gte": hora_on 
                }
            }
        },
        {
            "$sort": {
                "configuraciones_info.hora_on": 1 
            }
        },
        {
            "$project": {
                "_id": 1,
                "nombre_dispositivo": "$nombre_dispositivo",
                "nombre_configuracion": "$configuraciones_info.nombre_configuracion",
                "hora_on": "$configuraciones_info.hora_on",
                "hora_off": "$configuraciones_info.hora_off"
                
            }
        }
    ]
    ejecutar_agregacion(agg_pipeline, "/dispositivos/agregacion")

#9.Número de tipo dispositivos en una casa.
def get_cantidad_dispositivos_por_tipo(id_casa, tipo):
    match_etapa = { "$match": { "_id": id_casa } }

    if tipo:
        project_etapa = {
            "$project": {
                "_id": 0,
                "num_casa": 1,
                "tipo_dispositivo": tipo,
                "cantidad": { "$size": { "$ifNull": [ f"$dispositivos.{tipo}", [] ] } }
            }
        }
        agg_pipeline = [match_etapa, project_etapa]
    else:
        project_etapa = {
            "$project": {
                "_id": 0,
                "num_casa": 1,
                "bombilla": { "$size": { "$ifNull": ["$dispositivos.bombilla", []] } },
                "lavadora": { "$size": { "$ifNull": ["$dispositivos.lavadora", []] } },
                "refrigerador": { "$size": { "$ifNull": ["$dispositivos.refrigerador", []] } },
                "aspiradora": { "$size": { "$ifNull": ["$dispositivos.aspiradora", []] } },
                "cerradura": { "$size": { "$ifNull": ["$dispositivos.cerradura", []] } },
                "aire_acondicionado": { "$size": { "$ifNull": ["$dispositivos.aire_acondicionado", []] } }
            }
        }
        agg_pipeline = [match_etapa, project_etapa]
    ejecutar_agregacion(agg_pipeline, "/casas/agregacion")

#10.Dispositivo por fecha de instalación.	
def get_dispositivo_por_fecha_intalacion(id_casa, fecha_instalacion):
    agg_pipeline = [
        {
            "$match": {
                "id_casa": id_casa
            }
        },
        {
            "$match": {
                "fecha_instalacion": {
                    "$gte": fecha_instalacion
                }
            }
        },
        {
            "$sort": {
                "fecha_instalacion": 1 
            }
        },
        {
            "$project": {
                "_id": 1,
                "nombre_dispositivo": "$nombre_dispositivo",
                "fecha_instalacion": 1
            }
        }
    ]
    ejecutar_agregacion(agg_pipeline, "/dispositivos/agregacion")

#11. Configuraciones dado estado.	
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
    ejecutar_agregacion(agg_pipeline, "/dispositivos/agregacion")

#12. Dispositivos por nombre.	
def get_dispositivo_por_nombre(id_casa, nombre_dispositivo):
    agg_pipeline = [
        {
        "$match": {
                "$and": [
                    { "id_casa": id_casa},
                    { "$text": { "$search": nombre_dispositivo } }
                ]
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
    ejecutar_agregacion(agg_pipeline, "/dispositivos/agregacion")
    
