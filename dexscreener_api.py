# dexscreener_api.py
import time
import requests

class DexScreenerClient:
    """
    Cliente para DexScreener, maneja cache de volumen y marketcap.
    """

    def __init__(self):
        self.volume_history = {}  # { token: [(ts, volume), ...] }

    def fetch_token_data(self, token):
        """
        Llama a DexScreener para obtener datos como volumen/h1, market cap, etc.
        Placeholder, deberás adaptar a la ruta real y parsear la respuesta.
        """
        url = f"https://api.dexscreener.com/latest/dex/search?q={token}"
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            # Extraer volumen y marketcap aproximado
            # Ej. data["pairs"][0]["volume"]["h1"], data["pairs"][0]["marketCap"], etc.
            pairs = data.get("pairs", [])
            if pairs:
                h1_vol = pairs[0]["volume"].get("h1", 0)
                mcap = pairs[0].get("marketCap", 0)
                return float(h1_vol), float(mcap)
        return 0.0, 0.0

    def update_volume_history(self, token):
        """
        Llama a fetch_token_data y actualiza el history con (ts, volume).
        """
        vol, mcap = self.fetch_token_data(token)
        ts = int(time.time())
        if token not in self.volume_history:
            self.volume_history[token] = []
        self.volume_history[token].append((ts, vol))

        # Podrías almacenar marketcap en un dict aparte si quieres
        # ...

    def get_volume_growth(self, token):
        """
        Retorna un dict con growth_1m, growth_5m, growth_1h,
        basados en self.volume_history.
        """
        now = int(time.time())
        data = self.volume_history.get(token, [])
        if len(data) < 2:
            return {"growth_1m": 0, "growth_5m": 0, "growth_1h": 0}

        vol_now = data[-1][1]
        vol_1m_ago = self._get_volume_at_time(token, now - 60, data)
        vol_5m_ago = self._get_volume_at_time(token, now - 300, data)
        vol_1h_ago = self._get_volume_at_time(token, now - 3600, data)

        def growth(current, past):
            if past <= 0: 
                return 0
            return (current - past) / past

        return {
            "growth_1m": growth(vol_now, vol_1m_ago),
            "growth_5m": growth(vol_now, vol_5m_ago),
            "growth_1h": growth(vol_now, vol_1h_ago)
        }

    def get_market_cap(self, token):
        """
        Llama a fetch_token_data(token) y retorna el marketcap actual.
        """
        vol, mcap = self.fetch_token_data(token)
        return mcap

    def _get_volume_at_time(self, token, target_ts, data_list):
        """
        Buscamos el valor de volumen más cercano a target_ts en la lista data_list.
        """
        if not data_list:
            return 0
        closest_vol = data_list[0][1]
        closest_diff = abs(data_list[0][0] - target_ts)
        for (ts, vol) in data_list:
            diff = abs(ts - target_ts)
            if diff < closest_diff:
                closest_diff = diff
                closest_vol = vol
        return closest_vol
