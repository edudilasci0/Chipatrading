import requests
import json
import time
import logging

class GMGNClient:
    """
    Cliente para interactuar con la API pública de GMGN.ai para obtener
    datos de tokens en Solana sin necesidad de API key.
    """

    def __init__(self):
        self.base_url = "https://api.gmgn.ai/public/v1/"
        self.cache = {}
        self.cache_expiry = 60  # 1 minuto de caché
        self.logger = logging.getLogger("gmgn_client")

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
        headers = {"Accept": "application/json"}
        try:
            response = requests.get(url, params=params, headers=headers, timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.logger.warning(f"Token o recurso no encontrado en GMGN: {endpoint}")
            else:
                self.logger.error(f"Error HTTP en solicitud a GMGN: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error en solicitud a GMGN: {e}")
            return None

    def get_token_info(self, token):
        """
        Obtiene información detallada sobre un token, incluso si es muy reciente.
        """
        endpoint = f"tokens/{token}"
        cached = self._get_cache(endpoint, None, ttl=30)
        if cached:
            return cached
        data = self._request(endpoint)
        if data:
            self._set_cache(endpoint, None, data)
        return data

    def get_market_data(self, token):
        """
        Obtiene datos de mercado completos para un token.
        Se adapta a la estructura de respuesta de GMGN.
        """
        token_info = self.get_token_info(token)
        if not token_info:
            return None
        return {
            "price": token_info.get("price", 0),
            "market_cap": token_info.get("market_cap", 0),
            "volume": token_info.get("volume_24h", 0),
            "liquidity": token_info.get("liquidity", 0),
            "volume_growth": {
                "growth_5m": (token_info.get("volume_change_5m", 0) / 100) if token_info.get("volume_change_5m") else 0,
                "growth_1h": (token_info.get("volume_change_1h", 0) / 100) if token_info.get("volume_change_1h") else 0
            },
            "holders": token_info.get("holders", 0),
            "creation_time": token_info.get("creation_time")
        }

    def is_memecoin(self, token):
        """
        Detecta si un token es un memecoin basado en patrones y metadatos.
        """
        token_info = self.get_token_info(token)
        if not token_info:
            return False
        name = token_info.get("name", "").lower()
        symbol = token_info.get("symbol", "").lower()
        keywords = ["pepe", "doge", "shib", "cat", "inu", "meme", "wojak", "pump"]
        if any(kw in name for kw in keywords) or any(kw in symbol for kw in keywords):
            return True
        if token_info.get("type") == "meme" or token_info.get("is_meme", False):
            return True
        return False

    def clear_cache(self):
        """Limpia la caché de solicitudes."""
        self.cache = {}
        self.logger.info("Cache de GMGN limpiada")
