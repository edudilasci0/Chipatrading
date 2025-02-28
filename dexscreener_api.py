import time
import requests
import json

class DexScreenerClient:
    """
    Cliente para DexScreener, maneja cache de volumen, precio y marketcap.
    """

    def __init__(self):
        self.volume_history = {}  # { token: [(ts, volume), ...] }
        self.market_cap_cache = {}  # { token: (timestamp, market_cap) }
        self.price_cache = {}  # { token: (timestamp, price) }
        self.request_count = 0
        self.last_reset = time.time()
        self.cache_expiry = 60  # Segundos para expiración de caché

    def fetch_token_data(self, token):
        """
        Llama a DexScreener para obtener datos como volumen/h1, market cap, etc.
        
        Returns:
            tuple: (volumen_1h, market_cap, precio)
        """
        # Verificar caché primero
        if token in self.market_cap_cache and token in self.price_cache:
            mc_timestamp, mcap = self.market_cap_cache[token]
            price_timestamp, price = self.price_cache[token]
            
            # Si los datos son recientes (menos de 1 minuto), usarlos
            if (time.time() - mc_timestamp < self.cache_expiry and 
                time.time() - price_timestamp < self.cache_expiry):
                
                # Buscar volumen en el historial si existe
                vol_1h = 0
                if token in self.volume_history and self.volume_history[token]:
                    vol_1h = self.volume_history[token][-1][1]
                
                return vol_1h, mcap, price
        
        # Rate limiting
        current_time = time.time()
        if current_time - self.last_reset > 60:
            self.request_count = 0
            self.last_reset = current_time
            
        if self.request_count >= 10:
            print("⚠️ Límite de rate para DexScreener alcanzado, esperando...")
            time.sleep(2)  # Pequeña espera
            self.request_count = 0
            
        self.request_count += 1
            
        # Intentar obtener los datos
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token}"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                if "pairs" in data and data["pairs"]:
                    # Tomar el primer par (generalmente el principal)
                    active_pair = data["pairs"][0]
                    
                    # Extraer datos
                    h1_vol = float(active_pair["volume"].get("h1", 0))
                    mcap = float(active_pair.get("marketCap", 0))
                    price = float(active_pair.get("priceUsd", 0))
                    
                    # Actualizar caché
                    now = time.time()
                    self.market_cap_cache[token] = (now, mcap)
                    self.price_cache[token] = (now, price)
                    
                    return h1_vol, mcap, price
            
            print(f"⚠️ No se pudieron obtener datos para {token}, status: {response.status_code}")
            return 0.0, 0.0, 0.0
            
        except Exception as e:
            print(f"⚠️ Error al obtener datos de DexScreener: {e}")
            return 0.0, 0.0, 0.0

    def update_volume_history(self, token):
        """
        Llama a fetch_token_data y actualiza el history con (ts, volume).
        """
        vol, mcap, price = self.fetch_token_data(token)
        ts = int(time.time())
        
        # Actualizar historial de volumen
        if token not in self.volume_history:
            self.volume_history[token] = []
        self.volume_history[token].append((ts, vol))
        
        # Limitar el tamaño del historial (últimas 10 entradas)
        if len(self.volume_history[token]) > 10:
            self.volume_history[token] = self.volume_history[token][-10:]
        
        return vol, mcap, price

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
        vol_1m_ago = self._get_volume_at_time(now - 60, data)
        vol_5m_ago = self._get_volume_at_time(now - 300, data)
        vol_1h_ago = self._get_volume_at_time(now - 3600, data)

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
        Retorna el marketcap almacenado en cache o lo busca si no existe.
        """
        if token in self.market_cap_cache:
            ts, mcap = self.market_cap_cache[token]
            # Si la cache tiene menos de 1 minuto, usar el valor guardado
            if time.time() - ts < self.cache_expiry:
                return mcap
                
        # Si no está en cache o está desactualizado, buscar de nuevo
        _, mcap, _ = self.fetch_token_data(token)
        return mcap

    def get_token_price(self, token):
        """
        Retorna el precio del token desde la caché o lo busca si no existe.
        """
        if token in self.price_cache:
            ts, price = self.price_cache[token]
            # Si la cache tiene menos de 1 minuto, usar el valor guardado
            if time.time() - ts < self.cache_expiry:
                return price
                
        # Si no está en cache o está desactualizado, buscar de nuevo
        _, _, price = self.fetch_token_data(token)
        return price

    def _get_volume_at_time(self, target_ts, data_list):
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

    def clear_cache(self, token=None):
        """
        Limpia la caché para un token específico o toda la caché.
        
        Args:
            token: Token específico o None para limpiar toda la caché
        """
        if token:
            if token in self.market_cap_cache:
                del self.market_cap_cache[token]
            if token in self.price_cache:
                del self.price_cache[token]
            if token in self.volume_history:
                del self.volume_history[token]
            print(f"🧹 Caché limpiada para token {token}")
        else:
            self.market_cap_cache = {}
            self.price_cache = {}
            self.volume_history = {}
            print("🧹 Caché de DexScreener limpiada completamente")
