import json
import pydgraph

# 1.- Los usuarios deben poder filtrar los dispositivos que pertenecen a una casa en específico.
def dispositivos_en_casa(client, casa_id):
    """
    Los usuarios deben poder filtrar los dispositivos que pertenecen a una casa en específico.
    """
    query = """query dispositivos_casa($casa_id: string) {
        casa(func: eq(id_casa, $casa_id)) {
            nombre
            dispositivos: tiene_dispositivos {
                id_dispositivo
                categoria
                estado
                ubicacion
                temperatura
                modo
            }
        }
    }"""
    
    variables = {'$casa_id': casa_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 2.- Los usuarios deben poder filtrar y solo ver los aires acondicionados de una casa.
def aires_acondicionados(client, casa_id):
    """
    Los usuarios deben poder filtrar y solo ver los aires acondicionados de una casa.
    """
    query = """query aires_casa($casa_id: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos @filter(eq(categoria, "aire_acondicionado")) {
                id_dispositivo
                estado
                temperatura
                modo
                ubicacion
            }
        }
    }"""
    
    variables = {'$casa_id': casa_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 3.- Los usuarios deben poder filtrar y solo ver las bombillas de una casa.
def bombillas(client, casa_id):
    """
    Los usuarios deben poder filtrar y solo ver las bombillas de una casa.
    """
    query = """query bombillas_casa($casa_id: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos @filter(eq(categoria, "bombilla")) {
                id_dispositivo
                estado
                brillo
                color
                ubicacion
            }
        }
    }"""
    
    variables = {'$casa_id': casa_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 4.- Los usuarios deben poder filtrar y solo ver las aspiradoras de una casa.
def aspiradoras(client, casa_id):
    """
    Los usuarios deben poder filtrar y solo ver las aspiradoras de una casa.
    """
    query = """query aspiradoras_casa($casa_id: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos @filter(eq(categoria, "aspiradora")) {
                id_dispositivo
                estado
                potencia
                ruta
            }
        }
    }"""
    
    variables = {'$casa_id': casa_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 5.- Los usuarios deben poder filtrar y solo ver los refrigeradores de una casa.
def refrigeradores(client, casa_id):
    """
    Los usuarios deben poder filtrar y solo ver los refrigeradores de una casa.
    """
    query = """query refrigeradores_casa($casa_id: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos @filter(eq(categoria, "refrigerador")) {
                id_dispositivo
                estado
                temperatura
                ubicacion
            }
        }
    }"""
    
    variables = {'$casa_id': casa_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 6.- Los usuarios deben poder ver las cerraduras de una casa en específico.
def cerraduras(client, casa_id):
    """
    Los usuarios deben poder ver las cerraduras de una casa en específico.
    """
    query = """query cerraduras_casa($casa_id: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos @filter(eq(categoria, "cerradura")) {
                id_dispositivo
                estado
                ubicacion
            }
        }
    }"""
    
    variables = {'$casa_id': casa_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 7.- Los usuarios deben poder visualizar todos los dispositivos que están encendidos.
def dispositivos_encendidos(client, casa_id):
    """
    Los usuarios deben poder visualizar todos los dispositivos que están encendidos.
    """
    query = """query encendidos_casa($casa_id: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos @filter(eq(estado, "encendido")) {
                id_dispositivo
                categoria
                estado
                ubicacion
                temperatura
                modo
            }
        }
    }"""
    
    variables = {'$casa_id': casa_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 8.- Los usuarios deben poder visualizar todos los dispositivos que están apagados.
def dispositivos_apagados(client, casa_id):
    """
    Los usuarios deben poder visualizar todos los dispositivos que están apagados.
    """
    query = """query apagados_casa($casa_id: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos @filter(eq(estado, "apagado")) {
                id_dispositivo
                categoria
                estado
                ubicacion
                temperatura
                modo
            }
        }
    }"""
    
    variables = {'$casa_id': casa_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 9.- Los usuarios deben poder visualizar todos los dispositivos que están en modo eco.
def dispositivos_modo_eco(client, casa_id):
    """
    Los usuarios deben poder visualizar todos los dispositivos que están en modo eco.
    """
    query = """query eco_casa($casa_id: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos @filter(eq(estado, "modo eco")) {
                id_dispositivo
                categoria
                estado
                ubicacion
                temperatura
                modo
            }
        }
    }"""
    
    variables = {'$casa_id': casa_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 10.- Los usuarios deben poder visualizar todos los dispositivos que están en estado de error en una casa específica.
def dispositivos_con_error(client, casa_id):
    """
    Los usuarios deben poder visualizar todos los dispositivos que están en estado de error en una casa específica.
    """
    query = """query dispositivos_error($casa_id: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos @filter(eq(estado, "error")) {
                id_dispositivo
                categoria
                estado
                ubicacion
                temperatura
                modo
            }
        }
    }"""
    
    variables = {'$casa_id': casa_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 11.- Los usuarios deben poder visualizar todos los dispositivos que están en standby en una casa específica.
def dispositivos_en_standby(client, casa_id):
    """
    Los usuarios deben poder visualizar todos los dispositivos que están en standby en una casa específica.
    """
    query = """query dispositivos_standby($casa_id: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos @filter(eq(estado, "standby")) {
                id_dispositivo
                categoria
                estado
                ubicacion
                temperatura
                modo
            }
        }
    }"""
    
    variables = {'$casa_id': casa_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 12.- Los usuarios deben poder filtrar los dispositivos que hay en una habitación de una casa
def dispositivos_por_habitacion(client, casa_id, habitacion):
    """
    Los usuarios deben poder filtrar los dispositivos que hay en cada habitación de una casa.
    Retorna los dispositivos ubicados en una habitación específica.
    """
    query = """query dispositivos_habitacion($casa_id: string, $habitacion: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos @filter(alloftext(ubicacion, $habitacion)) {
                id_dispositivo
                categoria
                estado
                ubicacion
                temperatura
                modo
            }
        }
    }"""
    
    variables = {
        '$casa_id': casa_id,
        '$habitacion': habitacion
    }
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 13.- Los usuarios deben poder filtrar los dispositivos que están sincronizados entre sí (mismo cluster funcional).
def dispositivos_sincronizados(client, casa_id):
    """
    Los usuarios deben poder filtrar los dispositivos que están sincronizados entre sí.
    Retorna los dispositivos sincronizados y sus conexiones
    """
    query = """query dispositivos_sincronizados($casa_id: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos @filter(has(sincroniza_con)) {
                id_dispositivo
                categoria
                estado
                ubicacion
                # Obtenemos los dispositivos con los que está sincronizado
                dispositivos_sincronizados: sincroniza_con {
                    id_dispositivo
                    categoria
                    estado
                    ubicacion
                }
            }
        }
    }"""
    
    variables = {'$casa_id': casa_id}
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)

# 14.- Los usuarios deben poder filtrar los dispositivos que están en el mismo cluster funcional.
def dispositivos_cluster_funcional(client, casa_id, tipo_funcional):
    """
    Los usuarios deben poder filtrar los dispositivos que están en el mismo cluster funcional (iluminación, climatización, etc).
    Regresa los clusters del tipo especificado y sus dispositivos
    """
    query = """query dispositivos_cluster($casa_id: string, $tipo: string) {
        casa(func: eq(id_casa, $casa_id)) {
            tiene_dispositivos {
                id_dispositivo
                categoria
                estado
                ubicacion
                ~contiene_dispositivos @filter(eq(tipo, $tipo)) {
                    tipo
                    categoria
                    nombre
                    # Obtenemos todos los dispositivos del mismo cluster
                    contiene_dispositivos {
                        id_dispositivo
                        categoria
                        estado
                        ubicacion
                    }
                }
            }
        }
    }"""
    
    variables = {
        '$casa_id': casa_id,
        '$tipo': tipo_funcional
    }
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)