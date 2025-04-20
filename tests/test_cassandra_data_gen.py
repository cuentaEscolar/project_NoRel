from dataGen import generator

def test_random_data_generators():
    assert 30 >= generator.generar_temperatura_aleatoria() >= 16

    assert generator.generar_estado_aleatorio() in  ["encendido", "apagado", "standby", "modo eco", "error"]
    assert generator.generar_ruta_aleatoria() in  ["completa", "sala principal", "habitaciones", "cocina", "personalizada"] 
