import requests
import json
import time
import logging

class HeliusClient:
    """
    Cliente para interactuar con la API de Helius.
    Obtiene transacciones, datos de mercado y detecta actividad de ballenas.
    """

    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.helius.xyz/v1/"
        self.cache = {}
        self.cache_expiry = 60  # 1 minuto

    def _get_cache(self, endpoint, params, ttl=60):
        key = f"{endpoint}:{json.dumps(params)}"
        entry = self.cache.get(key)
        if entry and time.time() - entry["timestamp"] < ttl:
            return entry["data"]
        return None

    def _set_cache(self, endpoint, params, data):
        key = f"{endpoint}:{json.dumps(params)}"
        self.cache[key] = {"data": data, "timestamp": time.time()}

    def _request(self, endpoint, params=None):
        url = f"{self.base_url}{endpoint}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error en solicitud a Helius: {e}")
            return None

    def get_token_transactions(self, token, interval="5m"):
        """
        Consulta transacciones del token para un intervalo dado.
        interval: "1m", "5m", etc.
        """
        endpoint = f"tokens/{token}/transactions"
        params = {"interval": interval}
        ttl = 30 if interval == "1m" else 60
        cached = self._get_cache(endpoint, params, ttl=ttl)
        if cached:
            return cached
        data = self._request(endpoint, params)
        if data is None:
            logging.warning(f"Token {token} no encontrado o no soportado en transacciones.")
        else:
            self._set_cache(endpoint, params, data)
        return data

    def get_token_data(self, token):
        """
        Obtiene datos de mercado de un token (precio, market cap, etc.).
        """
        endpoint = f"tokens/{token}/market-data"
        params = {}
        cached = self._get_cache(endpoint, params, ttl=60)
        if cached:
            return cached
        data = self._request(endpoint, params)
        if data is None:
            logging.warning(f"Token {token} no encontrado en datos de mercado.")
        else:
            self._set_cache(endpoint, params, data)
        return data

    def get_whale_activity(self, token, min_tx_value=50000):
        """
        Filtra las transacciones del token para detectar actividad de ballenas.
        """
        transactions = self.get_token_transactions(token)
        if not transactions:
            return []
        return [tx for tx in transactions if tx.get("amount_usd", 0) >= min_tx_value]
