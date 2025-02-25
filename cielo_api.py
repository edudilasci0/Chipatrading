# cielo_api.py

import requests
import json

class CieloAPI:
    BASE_URL = "https://feed-api.cielo.finance/api/v1"

    def __init__(self, api_key=None):
        self.api_key = api_key

    def get_feed(self, params={}):
        url = f"{self.BASE_URL}/feed"
        try:
            response = requests.get(url, params=params)
            # Registra el status code y el contenido
            print("Status Code:", response.status_code)
            print("Response Content:", response.text)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print("Error en get_feed:", response.status_code, response.text)
                return None
        except Exception as e:
            print("Excepción en get_feed:", e)
            return None
