#!/usr/bin/env python3
import falcon.asgi
import logging

from Conexion.mongo_model import get_database, get_session

from Conexion.mongo_resources import DispositivosResource, ConfiguracionesResource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoggingMiddleware:
    async def process_request(self, req, resp):
        logger.info(f"Request: {req.method} {req.uri}")

    async def process_response(self, req, resp, resource, req_succeeded):
        logger.info(f"Response: {resp.status} for {req.method} {req.uri}")

# Initialize MongoDB client and database
client = get_session()
db = get_database(client)

# Create the Falcon application
app = falcon.asgi.App(middleware=[LoggingMiddleware()])

# Instantiate the resources
dispositivo_resource = DispositivosResource(db)
configuraciones_resource = ConfiguracionesResource(db)

# Add routes to serve the resources
app.add_route('/dispositivos', dispositivo_resource)
app.add_route('/configuraciones', configuraciones_resource)