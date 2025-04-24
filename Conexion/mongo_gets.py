import requests
import os

#ENDPOINT
INTELLIGENT_HOUSE_API_URL = os.getenv("INTELLIGENT_HOUSE_API_URL", "http://localhost:8001")

##getter para dipsositivos, configuraciones, casas y usuarios dado un parametro (no agregación) 

def get_dispositivos(**parameters):
    suffix = "/dispositivos"
    endpoint = INTELLIGENT_HOUSE_API_URL + suffix
    params = {key: value for key, value in parameters.items() if value is not None}

    response = requests.get(endpoint, params=params)
    if response.ok:
        json_resp = response.json()
        return json_resp
    else:
        print(f"Error: {response}")
        return

def get_configuraciones(**parameters):
    suffix = "/configuraciones"
    endpoint = INTELLIGENT_HOUSE_API_URL + suffix
    params = {key: value for key, value in parameters.items() if value is not None}

    response = requests.get(endpoint, params=params)
    if response.ok:
        json_resp = response.json()
        return json_resp
    else:
        print(f"Error: {response}")
        return
    
def get_casas(**parameters):
    suffix = "/casas"
    endpoint = INTELLIGENT_HOUSE_API_URL + suffix
    params = {key: value for key, value in parameters.items() if value is not None}

    response = requests.get(endpoint, params=params)
    if response.ok:
        json_resp = response.json()
        return json_resp
    else:
        print(f"Error: {response}")
        return

def get_usuarios(**parameters):
    suffix = "/usuarios"
    endpoint = INTELLIGENT_HOUSE_API_URL + suffix
    params = {key: value for key, value in parameters.items() if value is not None}

    response = requests.get(endpoint, params=params)
    if response.ok:
        json_resp = response.json()
        return json_resp
    else:
        print(f"Error: {response}")
        return
    
def print_x(x):
    for k in x.keys():
        print(f"{k}: {x[k]}")
    print("="*50)



##getter para dipsositivos, configuraciones, casas y usuarios dado una agregación
