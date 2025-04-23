import json
import logging
from dgraph_connection import DgraphConnection

logger = logging.getLogger(__name__)

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
            logger.info(f"Successfully loaded data. UIDs: {response.uids}")
            return response.uids
        finally:
            txn.discard()
            
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise
    finally:
        if connection:
            connection.close()