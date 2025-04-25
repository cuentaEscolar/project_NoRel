from dgraph_connection import DgraphConnection
import csv
import os
import logging
import pydgraph

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_casas(client, file_path):
    """
    Carga los datos de las casas desde el CSV
    Retorna: diccionario con los UIDs asignados {id_casa: uid}
    """
    txn = client.txn()
    resp = None
    try:
        casas = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                casas.append({
                    'uid': '_:' + row['id_casa'],
                    'dgraph.type': 'Casa',
                    'id_casa': int(row['id_casa'].split('_')[1]),  # Extraer número de "casa_X"
                    'nombre': row['nombre']
                })
        logger.info(f"Cargando {len(casas)} casas")
        resp = txn.mutate(set_obj=casas)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

def load_dispositivos(client, file_path):
    """
    Carga los datos de los dispositivos desde el CSV
    Retorna: diccionario con los UIDs asignados {id_dispositivo: uid}
    """
    txn = client.txn()
    resp = None
    try:
        dispositivos = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Crear diccionario base con campos obligatorios
                dispositivo = {
                    'uid': '_:' + row['id_dispositivo'],
                    'dgraph.type': 'Device',
                    'id_dispositivo': row['id_dispositivo'],
                    'categoria': row['categoria'],
                    'estado': row['estado']
                }
                
                # Agregar campos opcionales solo si tienen valor
                optional_fields = ['temperatura', 'modo', 'ubicacion', 'brillo', 
                                 'color', 'potencia', 'ruta']
                for field in optional_fields:
                    if row[field]:
                        dispositivo[field] = row[field]
                
                dispositivos.append(dispositivo)
                
        logger.info(f"Cargando {len(dispositivos)} dispositivos")
        resp = txn.mutate(set_obj=dispositivos)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

def load_clusters(client, file_path):
    """
    Carga los datos de los clusters desde el CSV
    Retorna: diccionario con los UIDs asignados {id_cluster: uid}
    """
    txn = client.txn()
    resp = None
    try:
        clusters = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                clusters.append({
                    'uid': '_:' + row['id_cluster'],
                    'dgraph.type': 'Cluster',
                    'tipo': row['tipo'],
                    'categoria': row['categoria'],
                    'nombre': row['nombre']
                })
        logger.info(f"Cargando {len(clusters)} clusters")
        resp = txn.mutate(set_obj=clusters)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

def create_relations(client, file_path, uids_map):
    """
    Crea las relaciones entre nodos usando el archivo de relaciones
    y el mapeo de UIDs obtenido al crear los nodos
    """
    txn = client.txn()
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                origen_id = row['origen_id']
                destino_id = row['destino_id']
                tipo_relacion = row['tipo_relacion']
                
                if origen_id in uids_map and destino_id in uids_map:
                    mutation = {
                        'uid': uids_map[origen_id],
                        tipo_relacion: {
                            'uid': uids_map[destino_id]
                        }
                    }
                    logger.info(f"Creando relación: {origen_id} -{tipo_relacion}-> {destino_id}")
                    txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()

def load_data_to_dgraph(base_path, connection=None):
    """
    Función principal para cargar datos en DGraph
    Args:
        base_path: Ruta base donde se encuentran los archivos CSV
        connection: Conexión opcional a DGraph
    """
    if connection is None:
        connection = DgraphConnection()
    
    client = None
    try:
        client = connection.connect()
        
        # Definir rutas de archivos
        casas_path = os.path.join(base_path, 'dgraph_casas.csv')
        dispositivos_path = os.path.join(base_path, 'dgraph_dispositivos.csv')
        clusters_path = os.path.join(base_path, 'dgraph_clusters.csv')
        relaciones_path = os.path.join(base_path, 'dgraph_relaciones.csv')
        
        # Cargar nodos y guardar UIDs asignados
        casas_uids = load_casas(client, casas_path)
        dispositivos_uids = load_dispositivos(client, dispositivos_path)
        clusters_uids = load_clusters(client, clusters_path)
        
        # Combinar todos los UIDs en un solo diccionario
        all_uids = {**casas_uids, **dispositivos_uids, **clusters_uids}
        
        # Crear relaciones entre nodos
        create_relations(client, relaciones_path, all_uids)
        
        logger.info("Carga de datos completada exitosamente")
        return all_uids
            
    except Exception as e:
        logger.error(f"Error cargando datos: {e}")
        raise
    finally:
        if connection:
            connection.close()