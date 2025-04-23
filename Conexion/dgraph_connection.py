import pydgraph

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