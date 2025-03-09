import time
import requests
import json
import logging
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Configurar logging
logger = logging.getLogger("dexscreener")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class DexScreenerClient:
    """
    Cliente optimizado para DexScreener, maneja cache de volumen, precio y marketcap.
    Implementa mecanismos avanzados de caché y rate limiting.
    """

    def __init__(self):
        self.volume_history = {}  # { token: [(ts, volume), ...] }
        self.market_cap_cache = {}  # { token: (timestamp, market_cap) }
        self.price_cache = {}  # { token: (timestamp, price) }
        self.request_count = 0
        self.last_reset = time.time()
        self.cache_expiry = 60  # Segundos para expiración de caché básica
        
        # NUEVO: Caché de larga duración para tokens con baja volatilidad
        self.long_cache = {}  # { token: {data, last_update, volatility} }
        
        # NUEVO: Rate limiting mejorado
        self.rate_limit = 10  # Máximo de peticiones por minuto
        self.request_timestamps = []  # Lista de timestamps de peticiones
        
        # NUEVO: Registro de errores
        self.error_tokens = {}  # { token: (last_error_time, error_count) }
        
        # NUEVO: Pool de hilos para peticiones paralelas
        self.executor = ThreadPoolExecutor(max_workers=3)

    async def fetch_token_data(self, token):
        """
        Llama a DexScreener para obtener datos como volumen/h1, market cap, etc.
        Versión mejorada para manejar tokens especiales y errores.
        
        Returns:
            tuple: (volumen_1h, market_cap, precio)
        """
        # NUEVO: Manejar tokens especiales o conocidos
        from config import Config
        
        # Si es un token a ignorar, registrar advertencia y retornar ceros
        if token in Config.IGNORE_TOKENS:
            logger.debug(f"Token ignorado por configuración: {token}")
            return 0.0, 0.0, 0.0
        
        # Si es un token conocido, usar valores predefinidos
        if token in Config.KNOWN_TOKENS:
            known_data = Config.KNOWN_TOKENS[token]
            logger.debug(f"Usando datos predefinidos para token conocido: {token} ({known_data['name']})")
            return known_data['vol_1h'], known_data['market_cap'], known_data['price']
        
        # Verificar caché primero con más detalle en logs
        if token in self.market_cap_cache and token in self.price_cache:
            mc_timestamp, mcap = self.market_cap_cache[token]
            price_timestamp, price = self.price_cache[token]
            
            # Si los datos son recientes, usarlos
            cache_age = time.time() - mc_timestamp
            if cache_age < self.cache_expiry:
                logger.debug(f"Usando caché para {token} (edad: {cache_age:.1f}s)")
                
                # Buscar volumen en el historial si existe
                vol_1h = 0
                if token in self.volume_history and self.volume_history[token]:
                    vol_1h = self.volume_history[token][-1][1]
                
                return vol_1h, mcap, price
            else:
                logger.debug(f"Caché expirada para {token} (edad: {cache_age:.1f}s)")
        
        # MEJORADO: Verificar tokens con errores recientes
        if token in self.error_tokens:
            last_error, error_count = self.error_tokens[token]
            error_age = time.time() - last_error
            
            # Si ha habido múltiples errores recientes, usar caché expirada o retornar ceros
            if error_count > 2 and error_age < 300:  # Reducido de 3 a 2 errores y 300 segundos
                logger.warning(f"Omitiendo token con errores recurrentes: {token}")
                
                # Retornar valores de caché aunque estén expirados
                if token in self.market_cap_cache and token in self.price_cache:
                    _, mcap = self.market_cap_cache[token]
                    _, price = self.price_cache[token]
                    vol_1h = 0
                    if token in self.volume_history and self.volume_history[token]:
                        vol_1h = self.volume_history[token][-1][1]
                    return vol_1h, mcap, price
                
                # Si no hay caché, retornar ceros pero permitir procesar el token
                return 0.0, 0.0, 0.0
        
        # MEJORADO: Aplicar rate limiting más inteligente
        try:
            await self._apply_rate_limiting()
            
            # Intentar obtener los datos con reintento
            max_retries = 2
            for retry in range(max_retries + 1):
                try:
                    url = f"https://api.dexscreener.com/latest/dex/tokens/{token}"
                    
                    # MEJORADO: Ejecutar la petición en un hilo para no bloquear
                    loop = asyncio.get_running_loop()
                    response = await loop.run_in_executor(
                        self.executor, 
                        lambda: requests.get(url, timeout=5)
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # NUEVO: Verificar explícitamente si pairs es None o vacío
                        if data.get("pairs") is None or len(data.get("pairs", [])) == 0:
                            logger.warning(f"Token {token} no tiene pares en DexScreener: {data}")
                            self._register_token_error(token)
                            return 0.0, 0.0, 0.0
                            
                        if "pairs" in data and data["pairs"]:
                            # Tomar el par con mayor volumen (generalmente el principal)
                            pairs = sorted(data["pairs"], 
                                          key=lambda x: float(x["volume"].get("h24", 0)), 
                                          reverse=True)
                            active_pair = pairs[0]
                            
                            # Extraer datos
                            h1_vol = float(active_pair["volume"].get("h1", 0))
                            mcap = float(active_pair.get("marketCap", 0))
                            price = float(active_pair.get("priceUsd", 0))
                            
                            # Actualizar caché
                            now = time.time()
                            self.market_cap_cache[token] = (now, mcap)
                            self.price_cache[token] = (now, price)
                            
                            # MEJORADO: Calcular volatilidad para posible caché larga
                            if token in self.price_cache:
                                old_time, old_price = self.price_cache[token]
                                if old_price > 0:
                                    time_diff = now - old_time
                                    if time_diff > 0:
                                        price_change_pct = abs(price - old_price) / old_price
                                        volatility = price_change_pct / time_diff * 3600  # Normalizado a cambio/hora
                                        
                                        # Si la volatilidad es baja, considerar para long cache
                                        if volatility < 0.02:  # Menos de 2% por hora
                                            self.long_cache[token] = {
                                                'data': (h1_vol, mcap, price),
                                                'last_update': now,
                                                'volatility': volatility
                                            }
                                            logger.debug(f"Token {token} añadido a caché larga (volatilidad: {volatility:.4f})")
                            
                            # Si había errores previos, resetear
                            if token in self.error_tokens:
                                del self.error_tokens[token]
                            
                            return h1_vol, mcap, price
                        else:
                            logger.debug(f"No se encontraron pares para el token {token}")
                    
                    # MEJORADO: Manejar errores específicos
                    if response.status_code == 429:  # Rate limit
                        wait_time = 5 + retry * 2
                        logger.warning(f"⚠️ Rate limit de DexScreener alcanzado, esperando {wait_time} segundos...")
                        await asyncio.sleep(wait_time)
                        continue  # Reintentar
                    elif response.status_code == 404:  # Token no encontrado
                        logger.debug(f"Token no encontrado en DexScreener: {token}")
                    else:
                        logger.warning(f"⚠️ Error en DexScreener: {response.status_code} - {response.text[:100]}")
                    
                    # Solo registrar error si fallaron todos los reintentos
                    if retry == max_retries:
                        self._register_token_error(token)
                        # Intentar usar valores de caché aunque estén expirados
                        if token in self.market_cap_cache and token in self.price_cache:
                            _, mcap = self.market_cap_cache[token]
                            _, price = self.price_cache[token]
                            vol_1h = 0
                            if token in self.volume_history and self.volume_history[token]:
                                vol_1h = self.volume_history[token][-1][1]
                            return vol_1h, mcap, price
                    
                    break  # Salir del loop si no es un error 429
                    
                except Exception as e:
                    logger.warning(f"Excepción al obtener datos de DexScreener (intento {retry+1}/{max_retries+1}): {str(e)}")
                    if retry < max_retries:
                        await asyncio.sleep(1)
                    else:
                        self._register_token_error(token)
                        # Intentar usar valores de caché aunque estén expirados
                        if token in self.market_cap_cache and token in self.price_cache:
                            _, mcap = self.market_cap_cache[token]
                            _, price = self.price_cache[token]
                            vol_1h = 0
                            if token in self.volume_history and self.volume_history[token]:
                                vol_1h = self.volume_history[token][-1][1]
                            return vol_1h, mcap, price
            
        except Exception as e:
            logger.error(f"⚠️ Error crítico en fetch_token_data: {str(e)}")
            self._register_token_error(token)
        
        return 0.0, 0.0, 0.0

    async def _apply_rate_limiting(self):
        """
        MEJORADO: Implementa rate limiting adaptativo.
        """
        now = time.time()
        
        # Limpiar timestamps antiguos (más de 60 segundos)
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        
        # MEJORADO: Verificar si estamos cerca del límite y aplicar espera proporcional
        if len(self.request_timestamps) >= self.rate_limit - 1:
            # Calcular tiempo a esperar de forma adaptativa
            requests_per_min = len(self.request_timestamps)
            wait_factor = min(requests_per_min / (self.rate_limit * 0.8), 1.5)  # Factor de espera proporcional
            
            wait_time = 1.0 * wait_factor  # Espera base ajustada
            
            if len(self.request_timestamps) >= self.rate_limit:
                # Si ya alcanzamos el límite, esperar a que expire la petición más antigua
                oldest = min(self.request_timestamps)
                expire_wait = 60 - (now - oldest) + 0.2  # +0.2 para asegurar que pase el minuto
                wait_time = max(wait_time, expire_wait)
            
            if wait_time > 0.1:  # Solo esperar si es un tiempo significativo
                logger.debug(f"Rate limiting aplicado, esperando {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
        
        # Registrar nueva petición
        self.request_timestamps.append(time.time())

    def _register_token_error(self, token):
        """
        NUEVO: Registra errores por token para evitar peticiones repetidas a tokens problemáticos.
        """
        now = time.time()
        if token in self.error_tokens:
            _, count = self.error_tokens[token]
            self.error_tokens[token] = (now, count + 1)
        else:
            self.error_tokens[token] = (now, 1)

    async def update_volume_history(self, token):
        """
        Llama a fetch_token_data y actualiza el history con (ts, volume).
        Versión asíncrona.
        """
        vol, mcap, price = await self.fetch_token_data(token)
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
        Versión optimizada con manejo de casos especiales.
        """
        now = int(time.time())
        data = self.volume_history.get(token, [])
        if len(data) < 2:
            return {"growth_1m": 0, "growth_5m": 0, "growth_1h": 0}

        vol_now = data[-1][1]
        
        # Si el volumen actual es 0, no podemos calcular crecimiento
        if vol_now <= 0:
            return {"growth_1m": 0, "growth_5m": 0, "growth_1h": 0}
            
        vol_1m_ago = self._get_volume_at_time(now - 60, data)
        vol_5m_ago = self._get_volume_at_time(now - 300, data)
        vol_1h_ago = self._get_volume_at_time(now - 3600, data)

        def growth(current, past):
            if past <= 0: 
                return 0
            return (current - past) / past

        # NUEVO: Limitar crecimiento extremo para evitar outliers
        growth_1m = min(3.0, growth(vol_now, vol_1m_ago))  # Máximo 300% en 1m
        growth_5m = min(5.0, growth(vol_now, vol_5m_ago))  # Máximo 500% en 5m
        growth_1h = min(10.0, growth(vol_now, vol_1h_ago))  # Máximo 1000% en 1h
        
        return {
            "growth_1m": growth_1m,
            "growth_5m": growth_5m,
            "growth_1h": growth_1h
        }

    async def get_market_cap(self, token):
        """
        Retorna el marketcap almacenado en cache o lo busca si no existe.
        Versión asíncrona.
        """
        if token in self.market_cap_cache:
            ts, mcap = self.market_cap_cache[token]
            # Si la cache tiene menos de 1 minuto, usar el valor guardado
            if time.time() - ts < self.cache_expiry:
                return mcap
        
        # NUEVO: Verificar caché de larga duración
        if token in self.long_cache:
            cache_data = self.long_cache[token]
            cache_age = time.time() - cache_data['last_update']
            # Si el token tiene baja volatilidad y la cache no es muy antigua
            if cache_data['volatility'] < 0.01 and cache_age < 3600:
                _, mcap, _ = cache_data['data']
                return mcap
                
        # Si no está en cache o está desactualizado, buscar de nuevo
        _, mcap, _ = await self.fetch_token_data(token)
        return mcap

    async def get_token_price(self, token):
        """
        Retorna el precio del token desde la caché o lo busca si no existe.
        Versión asíncrona.
        """
        if token in self.price_cache:
            ts, price = self.price_cache[token]
            # Si la cache tiene menos de 1 minuto, usar el valor guardado
            if time.time() - ts < self.cache_expiry:
                return price
        
        # NUEVO: Verificar caché de larga duración
        if token in self.long_cache:
            cache_data = self.long_cache[token]
            cache_age = time.time() - cache_data['last_update']
            # Si el token tiene baja volatilidad y la cache no es muy antigua
            if cache_data['volatility'] < 0.01 and cache_age < 3600:
                _, _, price = cache_data['data']
                return price
                
        # Si no está en cache o está desactualizado, buscar de nuevo
        _, _, price = await self.fetch_token_data(token)
        return price

    def _get_volume_at_time(self, target_ts, data_list):
        """
        Buscamos el valor de volumen más cercano a target_ts en la lista data_list.
        Versión optimizada para manejar datos dispersos.
        """
        if not data_list:
            return 0
            
        # Si solo hay un punto, usarlo independientemente del tiempo
        if len(data_list) == 1:
            return data_list[0][1]
            
        # NUEVO: Buscar el punto más cercano por tiempo
        closest_idx = 0
        closest_diff = abs(data_list[0][0] - target_ts)
        
        for i, (ts, vol) in enumerate(data_list):
            diff = abs(ts - target_ts)
            if diff < closest_diff:
                closest_diff = diff
                closest_idx = i
        
        # NUEVO: Si el punto más cercano está muy lejos, interpolar
        if closest_diff > 600:  # Más de 10 minutos
            # Intentar encontrar puntos antes y después para interpolar
            before_points = [(ts, vol) for ts, vol in data_list if ts <= target_ts]
            after_points = [(ts, vol) for ts, vol in data_list if ts > target_ts]
            
            if before_points and after_points:
                # Tomar los puntos más cercanos antes y después
                before = max(before_points, key=lambda x: x[0])
                after = min(after_points, key=lambda x: x[0])
                
                # Interpolar linealmente
                before_ts, before_vol = before
                after_ts, after_vol = after
                
                # Evitar división por cero
                if after_ts == before_ts:
                    return (before_vol + after_vol) / 2
                
                # Calcular proporción de tiempo
                proportion = (target_ts - before_ts) / (after_ts - before_ts)
                
                # Interpolar
                interpolated_vol = before_vol + proportion * (after_vol - before_vol)
                return interpolated_vol
        
        # Si no se pudo interpolar, usar el punto más cercano
        return data_list[closest_idx][1]

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
            if token in self.long_cache:
                del self.long_cache[token]
            if token in self.error_tokens:
                del self.error_tokens[token]
            logger.info(f"🧹 Caché limpiada para token {token}")
        else:
            self.market_cap_cache = {}
            self.price_cache = {}
            self.volume_history = {}
            self.long_cache = {}
            self.error_tokens = {}
            logger.info("🧹 Caché de DexScreener limpiada completamente")
    
    def get_cache_stats(self):
        """
        NUEVO: Retorna estadísticas sobre el estado actual de la caché.
        """
        return {
            "price_cache_size": len(self.price_cache),
            "market_cap_cache_size": len(self.market_cap_cache),
            "volume_history_size": len(self.volume_history),
            "long_cache_size": len(self.long_cache),
            "error_tokens": len(self.error_tokens),
            "cache_hit_potential": len(self.price_cache) / max(1, (len(self.price_cache) + len(self.error_tokens)))
        }
    
    async def prefetch_token_data(self, tokens):
        """
        NUEVO: Prefetch de datos para una lista de tokens en paralelo.
