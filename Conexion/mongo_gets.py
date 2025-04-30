import requests
import os

#ENDPOINT
INTELLIGENT_HOUSE_API_URL = os.getenv("INTELLIGENT_HOUSE_API_URL", "http://localhost:8001")

##getter para dipsositivos, configuraciones, casas y usuarios dado parametros o agregación 
#suff para get con parametros: "/dispositivos", "/configuraciones", "/casas", "/usuarios"
#suff para agregacion: "/dispositivos/agregacion", "/casas/agregacion", "/configuraciones/agregacion"
def get_x(suff,**parameters):
    suffix = suff
    endpoint = INTELLIGENT_HOUSE_API_URL + suffix
    params = {key: value for key, value in parameters.items() if value is not None}

    response = requests.get(endpoint, params=params)
    if response.ok:
        json_resp = response.json()
        return json_resp
    else:
        print(f"Error: {response}")
        return

