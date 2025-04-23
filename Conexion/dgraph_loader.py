import json
from dgraph_connection import DgraphConnection

def load_data_to_dgraph(json_file, connection=None):
    """Main function to load data into Dgraph"""
    if connection is None:
        connection = DgraphConnection()
    
    client = None
    try:
        client = connection.connect()
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        txn = client.txn()
        try:
            response = txn.mutate(set_obj=data)
            txn.commit()
            return response.uids
        finally:
            txn.discard()
            
    except Exception as e:
        raise
    finally:
        if connection:
            connection.close()