# project_NoRel

# Pasos para ejecutar el proyecto 

### Contenedores Docker
- ratel
- dgraph
- cassandra
- mongo

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