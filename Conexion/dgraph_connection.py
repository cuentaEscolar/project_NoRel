import pydgraph
import logging
from Conexion.dgraph_model import set_schema

class DgraphConnection:
    def __init__(self, host='localhost', port='9080'):
        self.client_stub = None
        self.client = None
        self.host = host
        self.port = port

    def connect(self):
        try:
            self.client_stub = pydgraph.DgraphClientStub(f'{self.host}:{self.port}')
            self.client = pydgraph.DgraphClient(self.client_stub)
            return self.client
        except Exception as e:
            raise

    def close(self):
        if self.client_stub:
            self.client_stub.close()

    # static para poder llamarla sin llamar la clase primero
    # esta función está aquí y no por fuera para mantener más limpieza en el código que llama todas las bases
    @staticmethod
    def initialize_dgraph():
        try:
            logging.info("Initializing Dgraph connection")
            connection = DgraphConnection()
            client = connection.connect()
            
            # Intentar establecer el schema
            set_schema(client)
            logging.info("Dgraph initialization successful")
            return client
        except Exception as e:
            logging.error(f"Failed to initialize Dgraph: {e}")
            raise