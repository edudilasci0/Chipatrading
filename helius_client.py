import requests
import time
import logging
import json

class HeliusClient:
    """
    Cliente para interactuar con la API de Helius.
    Obtiene transacciones recientes, volumen, market cap y otros datos relevantes para la evaluación de señales.
    """

    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.helius.xyz/v1/"
        self.cache = {}
        self.cache_expiry = 60  # 1 minuto de caché

    def _request(self, endpoint, params=None):
        """
        Realiza una solicitud HTTP a la API de Helius y maneja errores.
        """
        url = f"{self.base_url}{endpoint}"
        headers = {"Authorization": f"Bearer {self.api_key}"}

        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error en solicitud a Helius: {e}")
            return None

    def get_token_transactions(self, token, interval="5m"):
        """
        Obtiene las transacciones recientes de un token en un intervalo de tiempo específico.
        """
        if token in self.cache:
            cache_time, data = self.cache[token]
            if time.time() - cache_time < self.cache_expiry:
                return data

        data = self._request(f"tokens/{token}/transactions", params={"interval": interval})

        if data:
            self.cache[token] = (time.time(), data)

        return data

    def get_token_data(self, token):
        """
        Obtiene datos de mercado de un token como precio, market cap y volumen.
        """
        if token in self.cache:
            cache_time, data = self.cache[token]
            if time.time() - cache_time < self.cache_expiry:
                return data

        data = self._request(f"tokens/{token}/market-data")

        if data:
            self.cache[token] = (time.time(), data)

        return data

    def get_whale_activity(self, token, min_tx_value=50000):
        """
        Obtiene transacciones significativas de ballenas en un token.
        """
        transactions = self.get_token_transactions(token)
        if not transactions:
            return []

        whale_txs = [tx for tx in transactions if tx.get("amount_usd", 0) >= min_tx_value]

        return whale_txs

    def analyze_token_activity(self, token):
        """
        Analiza la actividad de un token basado en volumen, transacciones y presencia de ballenas.
        """
        token_data = self.get_token_data(token)
        transactions = self.get_token_transactions(token)

        if not token_data or not transactions:
            return None

        market_cap = token_data.get("market_cap", 0)
        volume_5m = token_data.get("volume_5m", 0)
        tx_count = len(transactions)

        whale_txs = self.get_whale_activity(token)

        return {
            "market_cap": market_cap,
            "volume_5m": volume_5m,
            "tx_count": tx_count,
            "whale_transactions": len(whale_txs),
        }
