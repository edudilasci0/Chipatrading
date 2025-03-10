import asyncio
import time
import logging
import requests

class HeliusClient:
    """
    Cliente para la API de Helius.
    Realiza solicitudes HTTP para obtener datos on-chain.
    """
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.helius.xyz/v1"
    
    def get_token_transactions(self, token, interval="5m"):
        """
        Realiza una solicitud para obtener transacciones de un token en un intervalo dado.
        """
        try:
            url = f"{self.base_url}/tokens/{token}/transactions"
            params = {"interval": interval}
            headers = {"Authorization": f"Bearer {self.api_key}"}
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error en HeliusClient.get_token_transactions para {token}: {e}")
            return {}

    def get_token_data(self, token):
        """
        Obtiene datos adicionales del token (como market cap, precio, etc).
        """
        try:
            url = f"{self.base_url}/tokens/{token}/data"
            headers = {"Authorization": f"Bearer {self.api_key}"}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error en HeliusClient.get_token_data para {token}: {e}")
            return {}

class HeliusTokenDataService:
    """
    Servicio completo para datos de tokens basado en Helius,
    reemplaza completamente a DexScreener.
    """
    def __init__(self, helius_client):
        self.helius_client = helius_client
        self.cache = {}
        self.volume_history = {}  # Para tracking de crecimiento de volumen
        self.last_cleanup = time.time()
    
    async def get_token_price(self, token):
        try:
            data = await asyncio.to_thread(self._get_token_data, token, "5m")
            return data.get('price', 0)
        except Exception as e:
            logging.error(f"Error obteniendo precio para {token}: {e}")
            return 0
    
    async def get_market_cap(self, token):
        try:
            data = await asyncio.to_thread(self._get_token_data, token, "5m")
            return data.get('market_cap', 0)
        except Exception as e:
            logging.error(f"Error obteniendo market cap para {token}: {e}")
            return 0

    async def update_volume_history(self, token):
        data = await asyncio.to_thread(self._get_token_data, token, "5m")
        ts = int(time.time())
        vol = data.get('volume', 0)
        if token not in self.volume_history:
            self.volume_history[token] = []
        self.volume_history[token].append((ts, vol))
        if len(self.volume_history[token]) > 10:
            self.volume_history[token] = self.volume_history[token][-10:]
        if time.time() - self.last_cleanup > 3600:
            self._cleanup_cache()
        return vol, data.get('market_cap', 0), data.get('price', 0)

    def get_volume_growth(self, token):
        now = int(time.time())
        data = self.volume_history.get(token, [])
        if len(data) < 2:
            return {"growth_1m": 0, "growth_5m": 0, "growth_1h": 0}
        vol_now = data[-1][1]
        if vol_now <= 0:
            return {"growth_1m": 0, "growth_5m": 0, "growth_1h": 0}
        vol_1m_ago = self._get_volume_at_time(now - 60, data)
        vol_5m_ago = self._get_volume_at_time(now - 300, data)
        vol_1h_ago = self._get_volume_at_time(now - 3600, data)
        def growth(current, past):
            if past <= 0:
                return 0
            return (current - past) / past
        growth_1m = min(3.0, growth(vol_now, vol_1m_ago))
        growth_5m = min(5.0, growth(vol_now, vol_5m_ago))
        growth_1h = min(10.0, growth(vol_now, vol_1h_ago))
        return {
            "growth_1m": growth_1m,
            "growth_5m": growth_5m,
            "growth_1h": growth_1h
        }

    def _get_token_data(self, token, interval="5m"):
        cache_key = f"{token}_{interval}"
        if cache_key in self.cache:
            cache_time, data = self.cache[cache_key]
            if time.time() - cache_time < 60:
                return data
        try:
            tx_data = self.helius_client.get_token_transactions(token, interval)
            market_data = self.helius_client.get_token_data(token)
            combined_data = {
                'volume': tx_data.get('volume', 0) if tx_data else 0,
                'market_cap': market_data.get('market_cap', 0) if market_data else 0,
                'price': market_data.get('price', 0) if market_data else 0,
                'volume_growth': tx_data.get('volume_growth', {}) if tx_data else {}
            }
            self.cache[cache_key] = (time.time(), combined_data)
            return combined_data
        except Exception as e:
            logging.error(f"Error obteniendo datos para {token}: {e}")
            return {'volume': 0, 'market_cap': 0, 'price': 0, 'volume_growth': {}}

    def _get_volume_at_time(self, target_ts, data_list):
        if not data_list:
            return 0
        if len(data_list) == 1:
            return data_list[0][1]
        closest_idx = 0
        closest_diff = abs(data_list[0][0] - target_ts)
        for i, (ts, vol) in enumerate(data_list):
            diff = abs(ts - target_ts)
            if diff < closest_diff:
                closest_diff = diff
                closest_idx = i
        return data_list[closest_idx][1]

    def _cleanup_cache(self):
        now = time.time()
        for key in list(self.cache.keys()):
            if now - self.cache[key][0] > 600:
                del self.cache[key]
        cutoff = now - 3600
        for token in list(self.volume_history.keys()):
            self.volume_history[token] = [(ts, vol) for ts, vol in self.volume_history[token] if ts > cutoff]
            if not self.volume_history[token]:
                del self.volume_history[token]
        self.last_cleanup = now
