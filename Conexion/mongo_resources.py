import falcon
from bson.objectid import ObjectId


class DispositivosResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp):
        try:
            #Recuperar los parametros dados por el usuario
            id = req.get_param('_id')
            id_casa = req.get_param('id_casa')
            tipo = req.get_param('tipo')
            nombre_dispositivo = req.get_param('nombre_dispositivo')
            estado = req.get_param('estado')
            fecha_instalacion = req.get_param('fecha_instalacion')

            #crear queries con los parametros
            query = {}
            if id is not None:
                query['_id'] = ObjectId(id)
            if id_casa is not None:
                query['id_casa'] = ObjectId(id_casa)
            if tipo is not None:
                query['tipo'] = tipo
            if nombre_dispositivo is not None:
                query['$text'] = {'$search': nombre_dispositivo}
            if estado is not None:
                query['estado'] = estado
            if fecha_instalacion is not None:
                query['fecha_instalacion'] = {"$gte": fecha_instalacion}
            
            dispositivos = self.db.dispositivos.find(query)
            dispositivos_list = []
            for dispositivo in dispositivos:
                dispositivo['_id'] = str(dispositivo['_id'])
                dispositivo['id_casa'] = str(dispositivo['id_casa'])
                if 'configuraciones' in dispositivo and isinstance(dispositivo['configuraciones'], list):
                    for config in dispositivo['configuraciones']:
                        if 'id_config' in config:
                            config['id_config'] = str(config['id_config'])
                dispositivos_list.append(dispositivo)
            resp.media = dispositivos_list
            resp.status = falcon.HTTP_200
        except Exception as e:
            resp.media = {"error": str(e)}
            print(f"Error en on_get: {e}")
        

class ConfiguracionesResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp):
        try:
            #Recuperar los parametros dados por el usuario
            id = req.get_param('_id')
            id_dispositivo = req.get_param('id_dispositivo')
            nombre_configuracion = req.get_param('nombre_configuracion')
            estado_configuracion = req.get_param('estado_configuracion')
            hora_on = req.get_param('hora_on')
            hora_off = req.get_param('hora_off')
            fecha_ultima_modificacion = req.get_param('fecha_ultima_modificacion')

            #crear queries con los parametros
            query = {}
            if id is not None:
                query['_id'] = ObjectId(id)
            if id_dispositivo is not None:
                query['id_dispositivo'] = ObjectId(id_dispositivo)
            if nombre_configuracion is not None:
                query['$text'] = {'$search': nombre_configuracion}
            if estado_configuracion is not None:
                query['estado_configuracion'] = estado_configuracion
            if hora_on is not None:
                query['hora_on'] = {"$gte": hora_on}
            if fecha_ultima_modificacion is not None:
                query['fecha_ultima_modificacion'] = {"$gte": fecha_ultima_modificacion}
            
            configuraciones = self.db.configuraciones.find(query)
            configuraciones_list = []
            for configuracion in configuraciones:
                configuracion['_id'] = str(configuracion['_id'])
                configuracion['id_dispositivo'] = str(configuracion['id_dispositivo'])
                configuraciones_list.append(configuracion)
            resp.media = configuraciones_list
            resp.status = falcon.HTTP_200
        except Exception as e:
            resp.media = {"error": str(e)}
            print(f"Error en on_get: {e}")