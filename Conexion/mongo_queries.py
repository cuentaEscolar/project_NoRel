from Conexion.mongo_gets import get_usuarios, get_casas, get_configuraciones, get_dispositivos, print_x
from bson.objectid import ObjectId


#funcion para obtener el id de la casa para evitar hacer las agregaciones desde la coleccion de casas siempre
def get_id_casa(casa):
    data = get_casas(num_casa = casa)
    for casa in data:
        return casa["_id"]


#1.Casas del usuario 	
# El usuario puede ver los datos de su cuenta y sus propiedades. 
# Se da un username.	
# Se muestra el username, correo, y num_casa de las casas de su propiedad.
def get_usuario_info():
    usuario = "user_0" #input("Ingresar username: ")
    data = get_usuarios(username = usuario)
    for user in data:
        print_x(user)


#2.Configuración on/off.	
# El usuario puede ver la configuración de prendido y apagado de los dispositivos (si esta activa esa configuración) de su casa 
# dado un tipo.	
# Se muestra nombre_dispositivo, el nombre_configuración (si esta activa), la hora_on y hora_off. 
# En el caso de aspiradoras también se muestra la hora_autolimpieza.
def get_configuracion_horario(id_casa, tipo):
    agregacion = [
        {
            "$match": {
            "$and": [
                { "id_casa": id_casa }, 
                { "tipo": tipo } 
            ]}
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
            "$match": { "configuraciones_info.estado_configuracion": "activo" }
        },
        {
            "$project": {
                "_id": 0,
                "info_dispositivos.nombre_dispositivo": 1,                     
                "configuraciones_info.nombre_configuracion": 1,                
                "configuraciones_info.hora_on": 1,                             
                "configuraciones_info.hora_off": 1,                            
                "configuraciones_info.config_especial.hora_autolimpieza": 1   
            }
        }
    ]
    ...


#3.Dispositivos por casas.	
# El usuario puede ver la lista de dispositivos que tiene en su casa y si están activos o no. 
# Da un tipo .	
# Se muestra el id_dispositivo, tipo, nombre_dispositivo.
def get_dispositivo_por_tipo_estado(casa, tipo, estado):
    agregacion = [
    {
        "$match": { "num_casa": casa }
    },
    {
        "$project": {
            "_id": 0,
            "dispositivos": {
                "$objectToArray": "$dispositivos" 
            }
        }
    },
    { "$unwind": "$dispositivos" },
    {
        "$match": { "dispositivos.k": tipo}
    },
    { "$unwind": "$dispositivos.v" },
    {
        "$lookup": {
            "from": "dispositivos",
            "localField": "dispositivos.v",
            "foreignField": "_id",
            "as": "info_dispositivos"
        }
    },
    { "$unwind": "$info_dispositivos" }, 
    {
        "$match": {"info_dispositivos.estado": estado}
    },
    {
            "$project": {
                "_id": 0,
                "info_dispositivos._id":1,
                "info_dispositivos.nombre_dispositivo": 1,                     
                "info_dispositivos.tipo": 1,
                "info_dispositivos.estado": 1
            }
        }
    ]
    ...

#4.Configuraciones por dispositivo.	
#El usuario puede ver todas las configuraciones de un dispositivo en específico 
# dado un id.	
#Se muestra el nombre_dispositivo, el estado_configuración, nombre_configuración.
def get_configuraciones_por_dispositivo():
    id_dispositivo = "68069ecb6c59183a8bd61ac9" #input("Ingrese id del dispositivo: ")
    agregacion = [
        {
            "$match": {
                "id_dispositivo": ObjectId(id_dispositivo)
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
        {
            "$unwind": "$dispositivo_info"
        },
        {
            "$project": {
                "_id": 0,
                "nombre_dispositivo": "$dispositivo_info.nombre_dispositivo",
                "estado_configuracion": 1,
                "nombre_configuracion": 1
            }
        }
    ]
    ...

#5.Configuraciones por nombre.	
# El usuario puede ver las configuraciones con nombre que contengan un texto especifico. 	
# Se muestra el id_configuracion, nombre_configuración, 
# estado_configuracion, fecha_ultima_modificacion, y el nombre_dispositivo al que pertenece esa configuración. }
def get_config_por_nombre(casa, nombre_config):
    agregacion = [
        {
            "$match": { "num_casa": casa }
        },
        {
            "$project": {
                "_id": 0,
                "dispositivos": {
                    "$objectToArray": "$dispositivos" 
                }
            }
        },
        { "$unwind": "$dispositivos" }, 
        { "$unwind": "$dispositivos.v" }, 
        {
            "$lookup": {
                "from": "dispositivos",
                "localField": "dispositivos.v",
                "foreignField": "_id",
                "as": "info_dispositivos"
            }
        },
        { "$unwind": "$info_dispositivos" }, 
        {
            "$lookup": {
                "from": "configuraciones",
                "localField": "info_dispositivos._id",
                "foreignField": "id_dispositivo",
                "as": "configuraciones_info"
            }
        },
        { "$unwind": "$configuraciones_info" },
        {
            "$match": { "$text": { "$search": nombre_config} }
        },
        {
            "$project": {
                "_id": 0,
                "info_dispositivos.nombre_dispositivo": 1,                   
                "configuraciones_info.nombre_configuracion": 1,  
                "configuraciones_info._id": 1,             
                "configuraciones_info.estado_configuracion": 1,            
                "configuraciones_info.fecha_ultima_modificacion": 1                        
            }
        }
    ]
    ...

#6.Configuración completa dado un id.	
# El usuario puede ver la configuración completa de un dispositivo dado su id_configuracion.	
# Se muestra el nombre_dispositivo, hora_on, hora_off, fecha_modificacion, configuración_especial, estado configuración, ubicación.
def get_configuracion_completa():
    config_id = "68069ecb6c59183a8bd61aca" #imput("Inserte id de configuración: ")
    data = get_usuarios(_id = config_id)
    for config in data:
        print_x(config)

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
def get_config_por_estado(casa, estado_config):
    agregacion = [
    {
        "$match": { "num_casa": casa }
    },
    {
        "$project": {
            "_id": 0,
            "dispositivos": {
                "$objectToArray": "$dispositivos" 
            }
        }
    },
    { "$unwind": "$dispositivos" },
    { "$unwind": "$dispositivos.v" },
    {
        "$lookup": {
            "from": "dispositivos",
            "localField": "dispositivos.v",
            "foreignField": "_id",
            "as": "info_dispositivos"
        }
    },
    { "$unwind": "$info_dispositivos" }, 
    {
        "$lookup": {
            "from": "configuraciones",
            "localField": "info_dispositivos._id",
            "foreignField": "id_dispositivo",
            "as": "configuraciones_info"
        }
    },
    {
        "$match": { "estado_configuracion": estado_config }
    },
    {
        "$project": {
            "_id": 0,
            "info_dispositivos.nombre_dispositivo": 1,                     
            "configuraciones_info.nombre_configuracion": 1,
            "configuraciones_info.estado_configuracion": 1
        }
    }
    ]
    ...


#12. Dispositivos por nombre.	
# El usuario puede ver las configuraciones con nombre que contengan un texto especifico. 	
# Se muestra el id_dispositivo, nombre_dispositivo, modelo, fecha_instalacion.
def get_dispositivo_por_nombre(casa, nombre_dispositivo):
    agregacion = [
        {
            "$match": { "num_casa": casa }
        },
        {
            "$project": {
                "_id": 0,
                "dispositivos": {
                    "$objectToArray": "$dispositivos" 
                }
            }
        },
        { "$unwind": "$dispositivos" }, 
        { "$unwind": "$dispositivos.v" }, 
        {
            "$lookup": {
                "from": "dispositivos",
                "localField": "dispositivos.v",
                "foreignField": "_id",
                "as": "info_dispositivos"
            }
        },
        { "$unwind": "$info_dispositivos" }, 
        {
            "$match": { "$text": { "$search": nombre_dispositivo} }
        },
        {
            "$project": {
                "_id": 0,
                "info_dispositivos._id": 1, 
                "info_dispositivos.nombre_dispositivo": 1,  
                "info_dispositivos.modelo": 1,  
                "info_dispositivos.fecha_instalacion": 1                 
            }
        }
    ]
    ...