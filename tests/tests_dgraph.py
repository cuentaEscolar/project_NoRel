# IMPORTANTE: Ejecuten
# python -m dataGen.loader
# Antes de ejecutar este script o todos los queries van a regresar vacíos

from Conexion.dgraph_connection import DgraphConnection
# Importa cada función del archivo de queries
from Conexion.dgraph_queries import (
    dispositivos_en_casa,
    aires_acondicionados,
    bombillas,
    aspiradoras,
    refrigeradores,
    cerraduras,
    dispositivos_encendidos,
    dispositivos_apagados,
    dispositivos_modo_eco,
    dispositivos_con_error,
    dispositivos_en_standby,
    dispositivos_por_habitacion,
    dispositivos_sincronizados,
    dispositivos_cluster_funcional
)
import json

def print_query_result(query_name, result):
    """Imprime los resultados de un query de manera formateada"""
    print("\n" + "="*50)
    print(f"Resultados de: {query_name}")
    print("="*50)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print("="*50 + "\n")

def test_all_queries():
    """Ejecuta todos los queries y muestra sus resultados"""
    try:
        # Inicializar conexión
        print("Iniciando conexión con Dgraph...")
        client = DgraphConnection.initialize_dgraph()
        
        # variables para las pruebas, tengan en cuenta que en algunas ocasiones hay que modificar estos valores
        # porque es posible que en la casa, no haya un dispositivo de ese tipo en la casa o habitación indicada (ejemplo)
        casa_id = "3" 
        habitacion_prueba = "sala"
        tipo_funcional = "iluminacion"

        """
        Ejecutar y mostrar resultados de cada query
        """

        print_query_result("Dispositivos en Casa", dispositivos_en_casa(client, casa_id))
        
        print_query_result("Aires Acondicionados", aires_acondicionados(client, casa_id))
        
        print_query_result("Bombillas", bombillas(client, casa_id))
        
        print_query_result("Aspiradoras", aspiradoras(client, casa_id))
        
        print_query_result("Refrigeradores", refrigeradores(client, casa_id))
        
        print_query_result("Cerraduras", cerraduras(client, casa_id))
        
        print_query_result("Dispositivos Encendidos", dispositivos_encendidos(client, casa_id))
        
        print_query_result("Dispositivos Apagados", dispositivos_apagados(client, casa_id))
        
        print_query_result("Dispositivos en Modo Eco", dispositivos_modo_eco(client, casa_id))
        
        print_query_result("Dispositivos con Error", dispositivos_con_error(client, casa_id))
        
        print_query_result("Dispositivos en Standby", dispositivos_en_standby(client, casa_id))
        
        print_query_result("Dispositivos por Habitación", dispositivos_por_habitacion(client, casa_id, habitacion_prueba))
        
        print_query_result("Dispositivos Sincronizados", dispositivos_sincronizados(client, casa_id))
        
        print_query_result("Dispositivos por Cluster Funcional", dispositivos_cluster_funcional(client, casa_id, tipo_funcional))

    except Exception as e:
        print(f"Error durante la ejecución: {str(e)}")

if __name__ == "__main__":
    test_all_queries()