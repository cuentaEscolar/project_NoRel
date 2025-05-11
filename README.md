# project_NoRel

# Crear ambiente virtual
python -m venv IOT
cd IOT
# Activar ambiente virtual
scripts/Activate 

# Intalar requerimientos
pip install -r requirements.txt
cd ..

# Pasos para ejecutar el proyecto 
### Contenedores Docker
ratel
dgraph
cassandra
mongo

### Para levantar el servidor de mongo
python -m uvicorn Conexion.mongo_router:app --reload --port 8001

### Para generar los datos
python -m dataGen.generator

### Para ejecutar el script principal
python -m app
