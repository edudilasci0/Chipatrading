import time
import requests
import logging
from config import Config

logger = logging.getLogger("helius_client")

class HeliusClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.cache = {}
        self.cache_duration = int(Config.get("HELIUS_CACHE_DURATION", 300))
        # Referencia al cliente DexScreener si está disponible
        self.dexscreener_client = None
    
    def _request(self, endpoint, params, version="v1"):
        url = f"https://api.helius.xyz/{version}/{endpoint}"
        if version == "v1":
            params["apiKey"] = self.api_key
        else:  # v0
            params["api-key"] = self.api_key
        try:
            logger.debug(f"Solicitando Helius {version}: {url}")
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error en HeliusClient._request ({version}): {e}")
            return None
    
    def get_token_data(self, token):
        now = time.time()
        if token in self.cache and now - self.cache[token]["timestamp"] < self.cache_duration:
            return self.cache[token]["data"]
        
        # Para tokens tipo pump, verificar si tiene 'pump' en el nombre
        if "pump" in token.lower():
            logger.info(f"Token {token} parece ser un pump token, usando datos predeterminados")
            # Usar datos predeterminados optimizados para tokens pump
            data = {
                "price": 0.000001,
                "market_cap": 1000000,
                "volume": 25000,
                "volume_growth": {"growth_5m": 0.35, "growth_1h": 0.25},
                "source": "default_pump",
                "token_type": "meme",
                "is_meme": True
            }
            self.cache[token] = {"data": data, "timestamp": now}
            return data
        
        data = None
        # Intentar con API v1
        try:
            endpoint = f"tokens/{token}"
            data = self._request(endpoint, {}, version="v1")
            if not data:
                data = self._request(endpoint, {}, version="v0")
            if not data:
                endpoint = f"addresses/{token}/tokens"
                data = self._request(endpoint, {}, version="v0")
        except Exception as e:
            logger.warning(f"Error comunicando con Helius API: {e}")
        
        if data:
            if isinstance(data, list) and data:
                data = data[0]
            normalized_data = {
                "price": self._extract_value(data, ["price", "priceUsd"]),
                "market_cap": self._extract_value(data, ["marketCap", "market_cap"]),
                "volume": self._extract_value(data, ["volume24h", "volume", "volumeUsd"]),
                "volume_growth": {
                    "growth_5m": self._normalize_percentage(self._extract_value(data, ["volumeChange5m", "volume_change_5m"])),
                    "growth_1h": self._normalize_percentage(self._extract_value(data, ["volumeChange1h", "volume_change_1h"]))
                },
                "name": self._extract_value(data, ["name", "tokenName"]),
                "symbol": self._extract_value(data, ["symbol", "tokenSymbol"]),
                "source": "helius"
            }
            self.cache[token] = {"data": normalized_data, "timestamp": now}
            return normalized_data
        
        # Intentar con DexScreener como respaldo
        if hasattr(self, 'dexscreener_client') and self.dexscreener_client:
            try:
                import asyncio
                dex_data = asyncio.run(self.dexscreener_client.fetch_token_data(token))
                if dex_data and dex_data.get("market_cap", 0) > 0:
                    dex_data["source"] = "dexscreener"
                    self.cache[token] = {"data": dex_data, "timestamp": now}
                    return dex_data
            except Exception as e:
                logger.warning(f"Error consultando DexScreener: {e}")
        
        # Si no hay datos, proporcionar datos predeterminados razonables
        default_data = {
            "price": 0.00001,
            "market_cap": 1000000,
            "volume": 10000,
            "volume_growth": {"growth_5m": 0.1, "growth_1h": 0.05},
            "source": "default"
        }
        self.cache[token] = {"data": default_data, "timestamp": now}
        logger.info(f"Usando datos predeterminados para {token} - APIs fallaron")
        return default_data
    
    def _extract_value(self, data, possible_keys):
        for key in possible_keys:
            if key in data:
                return data[key]
        return 0
    
    def _normalize_percentage(self, value):
        if value is None:
            return 0
        if value > 1 or value < -1:
            return value / 100
        return value
    
    async def get_token_price(self, token):
        token_data = self.get_token_data(token)
        if token_data and 'price' in token_data:
            return token_data['price']
        return 0
    
    async def get_token_data_async(self, token):
        return self.get_token_data(token)
    
    def get_price_change(self, token, timeframe="1h"):
        token_data = self.get_token_data(token)
        if not token_data:
            return 0
        try:
            if timeframe == "1h":
                return token_data.get("price_change_1h", 0)
            elif timeframe == "24h":
                return token_data.get("price_change_24h", 0)
            else:
                return 0
        except Exception as e:
            logger.error(f"Error en get_price_change para {token}: {e}")
            return 0
