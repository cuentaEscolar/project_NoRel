import pydgraph
import logging

logger = logging.getLogger(__name__)

class DgraphConnection:
    def __init__(self, host='localhost', port='9080'):
        self.client_stub = None
        self.client = None
        self.host = host
        self.port = port

    def connect(self):
        try:
            self.client_stub = pydgraph.DgraphClientStub('localhost:9080')
            self.client = pydgraph.DgraphClient(self.client_stub)
            return self.client
        except Exception as e:
            logger.error(f"Failed to connect to Dgraph: {e}")
            raise

    def close(self):
        if self.client_stub:
            self.client_stub.close()