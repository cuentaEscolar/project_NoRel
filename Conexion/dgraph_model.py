import pydgraph

def set_schema(client):
    try:
        schema = """
        type Device {
            id_dispositivo
            categoria
            estado
            temperatura
            modo
            ubicacion
            brillo
            color
            potencia
            ruta
            sincroniza_con: [Device]
        }

        type Casa {
            id_casa
            nombre
            tiene_dispositivos: [Device]
        }

        type Cluster {
            tipo
            categoria
            nombre
            contiene_dispositivos: [Device]
            agrupa_dispositivos: [Device]
            pertenece_a: [Casa]
        }

        id_dispositivo: string @index(exact) .
        categoria: string @index(hash) .
        estado: string @index(hash) .
        temperatura: string .
        modo: string @index(exact) .
        ubicacion: string @index(fulltext) .
        brillo: string .
        color: string @index(exact) .
        potencia: int .
        ruta: string .
        nombre: string @index(term) .
        tipo: string @index(hash) .
        id_casa: string @index(exact) .
        sincroniza_con: [uid] @reverse .
        tiene_dispositivos: [uid] @reverse .
        contiene_dispositivos: [uid] @reverse .
        agrupa_dispositivos: [uid] @reverse .
        pertenece_a: [uid] @reverse .
        """
        
        operation = pydgraph.Operation(schema=schema)
        
        result = client.alter(operation)
        print("Schema creado exitosamente")
        return result
    except Exception as e:
        raise