import json
import pydgraph

# 1.- Los usuarios deben poder filtrar los dispositivos que pertenecen a una casa en específico.
def dispositivos_por_casa(client, casa_id):
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
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos @filter(eq(categoria, "aire_acondicionado")) {
                aires as uid
            }
        }
        
        aires(func: uid(aires)) {
            id_dispositivo
            estado
            temperatura
            modo
            ubicacion
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
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos @filter(eq(categoria, "bombilla")) {
                bombillas as uid
            }
        }
        
        bombillas(func: uid(bombillas)) {
            id_dispositivo
            estado
            brillo
            color
            ubicacion
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
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos @filter(eq(categoria, "aspiradora")) {
                aspiradoras as uid
            }
        }
        
        aspiradoras(func: uid(aspiradoras)) {
            id_dispositivo
            estado
            potencia
            ruta
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
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos @filter(eq(categoria, "refrigerador")) {
                refrigeradores as uid
            }
        }
        
        refrigeradores(func: uid(refrigeradores)) {
            id_dispositivo
            estado
            temperatura
            ubicacion
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
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos @filter(eq(categoria, "cerradura")) {
                cerraduras as uid
            }
        }
        
        cerraduras(func: uid(cerraduras)) {
            id_dispositivo
            estado
            ubicacion
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
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos @filter(eq(estado, "encendido")) {
                encendidos as uid
            }
        }
        
        dispositivos(func: uid(encendidos)) {
            id_dispositivo
            categoria
            estado
            ubicacion
            temperatura
            modo
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
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos @filter(eq(estado, "apagado")) {
                apagados as uid
            }
        }
        
        dispositivos(func: uid(apagados)) {
            id_dispositivo
            categoria
            estado
            ubicacion
            temperatura
            modo
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
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos @filter(eq(estado, "modo eco")) {
                eco as uid
            }
        }
        
        dispositivos(func: uid(eco)) {
            id_dispositivo
            categoria
            estado
            ubicacion
            temperatura
            modo
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
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos @filter(eq(estado, "error")) {
                dispositivos_error as uid
            }
        }
        
        dispositivos(func: uid(dispositivos_error)) {
            id_dispositivo
            categoria
            estado
            ubicacion
            temperatura
            modo
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
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos @filter(eq(estado, "standby")) {
                dispositivos_standby as uid
            }
        }
        
        dispositivos(func: uid(dispositivos_standby)) {
            id_dispositivo
            categoria
            estado
            ubicacion
            temperatura
            modo
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
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos @filter(alloftext(ubicacion, $habitacion)) {
                dispositivos_hab as uid
            }
        }
        
        dispositivos(func: uid(dispositivos_hab)) {
            id_dispositivo
            categoria
            estado
            ubicacion
            temperatura
            modo
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
    Retorna los dispositivos que están en el mismo cluster funcional.
    """
    query = """query dispositivos_sincronizados($casa_id: string) {
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos {
                dispositivos_casa as uid
            }
        }
        
        clusters(func: type(Cluster)) @filter(eq(tipo, "cluster") AND eq(categoria, "funcional")) {
            nombre
            dispositivos_sync: agrupa_dispositivos @filter(uid(dispositivos_casa)) {
                id_dispositivo
                categoria
                estado
                ubicacion
                modo
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
    """
    query = """query dispositivos_cluster($casa_id: string, $tipo: string) {
        var(func: eq(id_casa, $casa_id)) {
            ~tiene_dispositivos {
                dispositivos_casa as uid
            }
        }
        
        cluster(func: type(Cluster)) @filter(eq(categoria, "funcional") AND eq(nombre, $tipo)) {
            nombre
            dispositivos: agrupa_dispositivos @filter(uid(dispositivos_casa)) {
                id_dispositivo
                categoria
                estado
                ubicacion
                modo
            }
        }
    }"""
    
    variables = {
        '$casa_id': casa_id,
        '$tipo': tipo_funcional
    }
    res = client.txn(read_only=True).query(query, variables=variables)
    return json.loads(res.json)