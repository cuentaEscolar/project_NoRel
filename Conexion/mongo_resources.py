import falcon
from bson.objectid import ObjectId
import json


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

class CasasResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp):
        try:
            #Recuperar los parametros dados por el usuario
            id = req.get_param('_id')
            id_usuario = req.get_param('id_usuario')
            num_casa = req.get_param_as_int('num_casa')

            #crear queries con los parametros
            query = {}
            if id is not None:
                query['_id'] = ObjectId(id)
            if id_usuario is not None:
                query['id_usuario'] = ObjectId(id_usuario)
            if num_casa is not None:
                query['num_casa'] = num_casa
            
            casas = self.db.casas.find(query)
            casas_list = []
            for casa in casas:
                casa['_id'] = str(casa['_id'])
                casa['id_usuario'] = str(casa['id_usuario'])
                if 'dispositivos' in casa and isinstance(casa['dispositivos'], dict):
                    for tipo, dispositivos in casa['dispositivos'].items():
                        for dispositivo in dispositivos:
                            if 'id_dispositivo' in dispositivo:
                                dispositivo['id_dispositivo'] = str(dispositivo['id_dispositivo'])
                casas_list.append(casa)
            resp.media = casas_list
            resp.status = falcon.HTTP_200
        except Exception as e:
            resp.media = {"error": str(e)}
            print(f"Error en on_get: {e}")

class UsuariosResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp):
        try:
            #Recuperar los parametros dados por el usuario
            id = req.get_param('_id')
            username = req.get_param('username')

            #crear queries con los parametros
            query = {}
            if id is not None:
                query['_id'] = ObjectId(id)
            if username is not None:
                query['username'] = username
            
            usuarios = self.db.usuarios.find(query)
            usuarios_list = []
            for usuario in usuarios:
                usuario['_id'] = str(usuario['_id'])
                if 'casas' in usuario and isinstance(usuario['casas'], list):
                    for casa in usuario['casas']:
                        if 'id_casa' in casa:
                            casa['id_casa'] = str(casa['id_casa'])
                usuarios_list.append(usuario)
            resp.media = usuarios_list
            resp.status = falcon.HTTP_200
        except Exception as e:
            resp.media = {"error": str(e)}
            print(f"Error en on_get: {e}")


class DispositivosAgregacionResource:
    def __init__(self, db):
        self.db = db
    async def on_get(self, req, resp):
        try:
            agg = req.get_param('agg')
            if not agg:
                resp.status = falcon.HTTP_400
                resp.media = {"error": "Falta el parámetro con la agregación"}
                return
            try:
                agg_pipeline = json.loads(agg)
                convertir_id_a_ObjectId(agg_pipeline)
            except json.JSONDecodeError as e:
                resp.status = falcon.HTTP_400
                resp.media = {"error": f"JSON inválido: {str(e)}"}
                return

            # Ejecutar la agregación
            result = self.db.dispositivos.aggregate(agg_pipeline)
            resultado_list = []

            for doc in result:
                # Convertir ObjectId a string
                for key in doc:
                    if isinstance(doc[key], ObjectId):
                        doc[key] = str(doc[key])
                resultado_list.append(doc)

            resp.media = resultado_list
            resp.status = falcon.HTTP_200
        except Exception as e:
            resp.status = falcon.HTTP_500
            resp.media = {"error": str(e)}
            print(f"Error en on_get_agregacion: {e}")


class CasasAgregacionResource:
    def __init__(self, db):
        self.db = db
    async def on_get(self, req, resp):
        try:
            agg = req.get_param('agg')
            if not agg:
                resp.status = falcon.HTTP_400
                resp.media = {"error": "Falta el parámetro con la agregación"}
                return
            
            try:
                agg_pipeline = json.loads(agg)
                convertir_id_a_ObjectId(agg_pipeline)
            except json.JSONDecodeError as e:
                resp.status = falcon.HTTP_400
                resp.media = {"error": f"JSON inválido: {str(e)}"}
                return

            # Ejecutar la agregación
            result = self.db.casas.aggregate(agg_pipeline)
            resultado_list = []

            for doc in result:
                # Convertir ObjectId a string
                for key in doc:
                    if isinstance(doc[key], ObjectId):
                        doc[key] = str(doc[key])
                resultado_list.append(doc)

            resp.media = resultado_list
            resp.status = falcon.HTTP_200
        except Exception as e:
            resp.status = falcon.HTTP_500
            resp.media = {"error": str(e)}
            print(f"Error en on_get_agregacion: {e}")


class ConfiguracionesAgregacionResource:
    def __init__(self, db):
        self.db = db
    async def on_get(self, req, resp):
        try:
            agg = req.get_param('agg')
            if not agg:
                resp.status = falcon.HTTP_400
                resp.media = {"error": "Falta el parámetro con la agregación"}
                return
            try:
                agg_pipeline = json.loads(agg)
                convertir_id_a_ObjectId(agg_pipeline)
            except json.JSONDecodeError as e:
                resp.status = falcon.HTTP_400
                resp.media = {"error": f"JSON inválido: {str(e)}"}
                return

            # Ejecutar la agregación
            result = self.db.configuraciones.aggregate(agg_pipeline)
            resultado_list = []

            for doc in result:
                # Convertir ObjectId a string
                for key in doc:
                    if isinstance(doc[key], ObjectId):
                        doc[key] = str(doc[key])
                resultado_list.append(doc)

            resp.media = resultado_list
            resp.status = falcon.HTTP_200
        except Exception as e:
            resp.status = falcon.HTTP_500
            resp.media = {"error": str(e)}
            print(f"Error en on_get_agregacion: {e}")

#funcion para convertir el id string del pipeline a un tipo ObjectId
def convertir_id_a_ObjectId(pipeline):
    for etapa in pipeline:
        if "$match" in etapa:
            try:
                if "_id" in etapa["$match"]:
                    etapa["$match"]["_id"] = ObjectId(etapa["$match"]["_id"])
                    
                if "id_casa" in etapa["$match"]:
                    etapa["$match"]["id_casa"] = ObjectId(etapa["$match"]["id_casa"])
                
                if "dispositivo_info.id_casa" in etapa["$match"]:
                    etapa["$match"]["dispositivo_info.id_casa"] = ObjectId(etapa["$match"]["dispositivo_info.id_casa"])
                    
                if "$and" in etapa["$match"]:
                    for cond in etapa["$match"]["$and"]:
                        if "id_casa" in cond:
                            cond["id_casa"] = ObjectId(cond["id_casa"])
                        if "_id" in cond:
                            cond["_id"] = ObjectId(cond["_id"])
                    
            except Exception as e:
                print("No se pudo convertir id a ObjectId", e)