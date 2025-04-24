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
        }

        id_dispositivo: string @index(exact) .
        categoria: string @index(exact) .
        estado: string @index(exact) .
        temperatura: string .
        modo: string @index(exact) .
        ubicacion: string @index(exact) .
        brillo: string .
        color: string @index(exact) .
        potencia: int .
        ruta: string .
        nombre: string @index(exact) .
        tipo: string @index(exact) .
        id_casa: int .
        tiene_dispositivos: [uid] .
        contiene_dispositivos: [uid] .
        agrupa_dispositivos: [uid] .
        """
        
        operation = pydgraph.Operation(schema=schema)
        
        result = client.alter(operation)
        return result
    except Exception as e:
        raise