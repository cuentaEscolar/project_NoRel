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
        

