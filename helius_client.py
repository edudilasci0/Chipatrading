import requests
import json
import time
import logging

class HeliusClient:
    """
    Cliente para interactuar con la API de Helius.
    Obtiene datos de mercado y transacciones para tokens en Solana.
    """

    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.helius.xyz/v1/"
        self.cache = {}
        self.cache_expiry = 60  # TTL en segundos para caché
        self.logger = logging.getLogger("helius_client")

    def _get_cache(self, endpoint, params, ttl=60):
        key = f"{endpoint}:{json.dumps(params) if params else ''}"
        entry = self.cache.get(key)
        if entry and time.time() - entry["timestamp"] < ttl:
            return entry["data"]
        return None

    def _set_cache(self, endpoint, params, data):
        key = f"{endpoint}:{json.dumps(params) if params else ''}"
        self.cache[key] = {"data": data, "timestamp": time.time()}

    def _request(self, endpoint, params=None):
        url = f"{self.base_url}{endpoint}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            response = requests.get(url, params=params, headers=headers, timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Error en solicitud a Helius: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error en solicitud a Helius: {e}")
            return None

    def get_token_data(self, token):
        """
        Obtiene datos de mercado de un token (precio, market cap, volumen, etc.).
        Maneja tokens no encontrados o tokens nativos devolviendo valores predeterminados.
        """
        from config import Config
        if token == "native" or token in Config.IGNORE_TOKENS:
            return {
                "price": 0,
                "market_cap": 0,
                "volume": 0,
                "volume_change_24h": 0,
                "volume_growth": {"growth_5m": 0, "growth_1h": 0}
            }
        endpoint = f"tokens/{token}/market-data"
        params = {}
        cached = self._get_cache(endpoint, params, ttl=self.cache_expiry)
        if cached:
            return cached
        data = self._request(endpoint, params)
        if data is None:
            self.logger.warning(f"Token {token} no encontrado en datos de mercado.")
            empty_data = {
                "price": 0,
                "market_cap": 0,
                "volume": 0,
                "volume_change_24h": 0,
                "volume_growth": {"growth_5m": 0, "growth_1h": 0}
            }
            self._set_cache(endpoint, params, empty_data)
            return empty_data
        else:
            self._set_cache(endpoint, params, data)
        return data

    def get_token_transactions(self, token, interval="5m"):
        """
        Obtiene transacciones del token para un intervalo dado (ej: '1m', '5m').
        """
        endpoint = f"tokens/{token}/transactions"
        params = {"interval": interval}
        ttl = 30 if interval == "1m" else 60
        cached = self._get_cache(endpoint, params, ttl=ttl)
        if cached:
            return cached
        data = self._request(endpoint, params)
        if data is None:
            self.logger.warning(f"Token {token} no encontrado en transacciones.")
            return {}
        self._set_cache(endpoint, params, data)
        return data
