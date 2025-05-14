# project_NoRel

# Crear ambiente virtual
python -m venv IOT

# Activar ambiente virtual
.\IOT\Scripts\Activate.ps1

# Intalar requerimientos
pip install -r requirements.txt


# Pasos para ejecutar el proyecto 

### Contenedores Docker
- ratel
  docker run --name ratel_proyecto -d -p 8000:8000 dgraph/ratel:latest
- dgraph
  docker run --name dgraph_proyecto -d -p 8080:8080 -p 9080:9080  dgraph/standalone
- cassandra
  docker run --name cassandra_proyecto -p 9042:9042 -d cassandra
- mongo
  docker run --name mongodb_proyecto -d -p 27017:27017 mongo

### Para levantar el servidor de mongo
```bash
python -m uvicorn Conexion.mongo_router:app --reload --port 8001
```

### Para generar los datos
```bash
python -m dataGen.generator
```

### Para ejecutar el script principal
```bash
python -m app
```