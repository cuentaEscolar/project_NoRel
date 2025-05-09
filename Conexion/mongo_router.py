#!/usr/bin/env python3
import falcon.asgi
import logging

from Conexion.mongo_model import get_session

from Conexion.mongo_resources import DispositivosResource, ConfiguracionesResource, CasasResource, UsuariosResource, DispositivosAgregacionResource, CasasAgregacionResource, ConfiguracionesAgregacionResource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoggingMiddleware:
    async def process_request(self, req, resp):
        logger.info(f"Request: {req.method} {req.uri}")

    async def process_response(self, req, resp, resource, req_succeeded):
        logger.info(f"Response: {resp.status} for {req.method} {req.uri}")

# Initialize MongoDB client and database
client = get_session()
db = client["intelligent_houses"]

# Create the Falcon application
app = falcon.asgi.App(middleware=[LoggingMiddleware()])

# Instantiate the resources
dispositivo_resource = DispositivosResource(db)
configuraciones_resource = ConfiguracionesResource(db)
casas_resource = CasasResource(db)
usuarios_resource = UsuariosResource(db)

#buscar dispositivos con agregaciones
dispositivo_agregaciones_resource = DispositivosAgregacionResource(db)
casas_agregaciones_resource = CasasAgregacionResource(db)
configuraciones_agregaciones_resource = ConfiguracionesAgregacionResource(db)
# Add routes to serve the resources
app.add_route('/dispositivos', dispositivo_resource)
app.add_route('/configuraciones', configuraciones_resource)
app.add_route('/casas', casas_resource)
app.add_route('/usuarios', usuarios_resource)

#ruta para buscar dipsositivos con agregaciones
app.add_route('/dispositivos/agregacion', dispositivo_agregaciones_resource)
app.add_route('/casas/agregacion', casas_agregaciones_resource)
app.add_route('/configuraciones/agregacion', configuraciones_agregaciones_resource)