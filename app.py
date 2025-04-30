import Conexion.mongo_queries


def set_username():
    username = input('Ingresa tu sername: ')
    return username

def set_num_casa():
    num_casa = input('Ingresa tu número de casa: ')
    return num_casa

def print_menu():
    options = {
        0: "Poblar datos",
        1: "Mostrar información de cuenta",
        2: "Mostrar menú para logs",
        3: "Mostrar menú para dispositivos y configuraciones",
        4: "Mostrar menú para relaciones de dispositivos",
        5: "Cambiar username",
        6: "Salir",
    }
    for key in options.keys():
        print(key, '--', options[key])


#Mongo menu
#todas las busquedas son por casa
def print_mongo_menu():
    options = {
        1: "Mostrar cantidad de dispositivos",
        2: "Buscar dispositivos por nombre",
        3: "Buscar dispositivos por tipo y estado",
        4: "Buscar dispositivos por fecha de instalación",
        5: "Buscar configuraciones de un dispositivo",
        6: "Buscar configuraciones por nombre",
        7: "Buscar configuraciones por id",
        8: "Buscar configuraciones por hora de encendido",
        9: "Buscar configuraciones por tipo de dispositivo",
        10: "Buscar configuraciones por fecha de modificación",
        11: "Buscar configuraciones por estado",
        12: "Cambiar casa"
    }
    for key in options.keys():
        print('    ', key, '--', options[key])


def main():
    username = set_username()

    while(True):
        print_menu()
        option = int(input('Ingresa una opción: '))
        if option == 0:
            #insertar datos
            ...
        if option == 1:
            Conexion.mongo_queries.get_usuario_info(username)
        if option == 2:
            #mostrar menu de Cassandra
            ...
        if option == 3:
            num_casa = set_username()
            print_mongo_menu()
            option = int(input('Ingresa una opción: '))
            if(option <= 11 or option >= 1):
                if option == 1:
                    ...
                if option == 2:
                    ...
                if option == 3:
                    ...
                if option == 4:
                    ...
                if option == 5:
                    ...
        if option == 4:
            #mostrar menu de dgraph
            ...
        if option == 5:
            username = set_username()
        if option == 6:
            exit(0)


if __name__ == '__main__':
    main()
