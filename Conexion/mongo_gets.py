import requests
import os

#ENDPOINT
INTELLIGENT_HOUSE_API_URL = os.getenv("INTELLIGENT_HOUSE_API_URL", "http://localhost:8001")

##getter que return un dispositivo y otro que devuelva una configuracion que devuelva una casa y otro un usuario dados parametros 

def get_dispositivos(**parameters):
    suffix = "/dispositivos"
    endpoint = INTELLIGENT_HOUSE_API_URL + suffix
    params = {key: value for key, value in parameters.items() if value is not None}

    response = requests.get(endpoint, params=params)
    if response.ok:
        json_resp = response.json()
        for disp in json_resp:
            print_x(disp)
    else:
        print(f"Error: {response}")
    
def print_x(x):
    for k in x.keys():
        print(f"{k}: {x[k]}")
    print("="*50)

