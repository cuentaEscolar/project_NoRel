import pydgraph

def set_schema(client):
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
    """
    return client.alter(pydgraph.Operation(schema=schema))