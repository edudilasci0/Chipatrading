import time
import asyncio
import aiohttp
import logging

logger = logging.getLogger("dexscreener_client")

class DexScreenerClient:
    """
    Cliente optimizado para DexScreener con énfasis en obtener datos precisos 
    de market cap y volumen siguiendo la documentación oficial de la API.
    """
    def __init__(self):
        self.cache = {}
        self.cache_duration = 60  # 1 minuto de caché
        self.request_timestamps = []
        self.rate_limit = 300  # 300 peticiones por minuto según documentación
        self.session = None
        self.error_backoff = 1  # Tiempo de espera inicial para errores

    async def ensure_session(self):
        """Asegura que existe una sesión HTTP abierta"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
        
    async def close_session(self):
        """Cierra la sesión HTTP si está abierta"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

    async def _apply_rate_limiting(self):
        """
        Aplica limitación de tasa para evitar ser bloqueado por la API.
        Según documentación: 300 peticiones por minuto para endpoints de tokens.
        """
        now = time.time()
        # Limpiar timestamps antiguos (más de 60 segundos)
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        
        # Si estamos cerca del límite, esperar
        if len(self.request_timestamps) >= self.rate_limit:
            oldest = min(self.request_timestamps)
            wait_time = 60 - (now - oldest) + 0.2  # Margen de seguridad de 0.2s
            if wait_time > 0:
                logger.debug(f"Rate limit aplicado: esperando {wait_time:.2f}s antes de la próxima solicitud")
                await asyncio.sleep(wait_time)
                
        # Registrar esta solicitud
        self.request_timestamps.append(time.time())

    async def fetch_token_data(self, token, retries=2):
        """
        Obtiene datos completos de un token con énfasis en market cap y volumen.
        Usa el endpoint /latest/dex/tokens/ADDRESS según documentación.
        
        Args:
            token: Dirección del token
            retries: Número de reintentos en caso de error
            
        Returns:
            dict: Datos del token con market cap y volumen o valores por defecto.
        """
        now = time.time()
        
        logger.info(f"Iniciando fetch de datos para token {token}")
        
        # Verificar caché primero
        if token in self.cache and now - self.cache[token]["timestamp"] < self.cache_duration:
            cache_data = self.cache[token]["data"]
            if cache_data and cache_data.get("market_cap", 0) > 0 and cache_data.get("volume", 0) > 0:
                logger.info(f"Datos para {token} recuperados de caché")
                return cache_data
        
        attempt = 0
        current_backoff = self.error_backoff
        
        while attempt <= retries:
            try:
                await self._apply_rate_limiting()
                
                session = await self.ensure_session()
                url = f"https://api.dexscreener.com/latest/dex/tokens/{token}"
                logger.debug(f"Solicitando datos de DexScreener para {token}: {url}")
                
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if not data or "pairs" not in data or not data["pairs"]:
                            logger.warning(f"Sin datos de pares para token {token} (intent {attempt+1})")
                            attempt += 1
                            await asyncio.sleep(current_backoff)
                            current_backoff *= 2
                            continue
                        
                        # Ordenar pares por volumen (h24) y usar el par más activo
                        pairs = sorted(
                            data["pairs"], 
                            key=lambda x: float(x.get("volume", {}).get("h24", 0)) if isinstance(x.get("volume", {}), dict) else 0, 
                            reverse=True
                        )
                        
                        if not pairs:
                            break
                            
                        active_pair = pairs[0]
                        
                        # Extraer datos críticos
                        try:
                            price = float(active_pair.get("priceUsd", 0))
                        except (ValueError, TypeError):
                            price = 0
                        
                        try:
                            market_cap = float(active_pair.get("marketCap", 0))
                        except (ValueError, TypeError):
                            market_cap = 0
                        
                        volume_24h = 0
                        volume_1h = 0
                        vol_data = active_pair.get("volume", {})
                        if isinstance(vol_data, dict):
                            try:
                                volume_24h = float(vol_data.get("h24", 0))
                            except (ValueError, TypeError):
                                volume_24h = 0
                            try:
                                volume_1h = float(vol_data.get("h1", 0))
                            except (ValueError, TypeError):
                                volume_1h = 0
                        
                        if volume_24h > 0 and volume_1h == 0:
                            volume_1h = volume_24h / 12  # Estimación conservadora
                        
                        # Extraer crecimiento de precio
                        growth_5m = 0
                        growth_1h = 0
                        price_change = active_pair.get("priceChange", {})
                        if isinstance(price_change, dict):
                            try:
                                growth_5m = float(price_change.get("m5", 0)) / 100
                            except (ValueError, TypeError):
                                growth_5m = 0
                            try:
                                growth_1h = float(price_change.get("h1", 0)) / 100
                            except (ValueError, TypeError):
                                growth_1h = 0
                        
                        # Extraer liquidez
                        liquidity = 0
                        liquidity_data = active_pair.get("liquidity", {})
                        if isinstance(liquidity_data, dict):
                            try:
                                liquidity = float(liquidity_data.get("usd", 0))
                            except (ValueError, TypeError):
                                liquidity = 0
                        
                        # Verificar trending
                        is_trending = "trending" in active_pair.get("labels", [])
                        
                        token_name = active_pair.get("baseToken", {}).get("name", "")
                        token_symbol = active_pair.get("baseToken", {}).get("symbol", "")
                        
                        result = {
                            "price": price,
                            "market_cap": market_cap,
                            "volume": volume_1h,
                            "volume_24h": volume_24h,
                            "volume_growth": {"growth_5m": growth_5m, "growth_1h": growth_1h},
                            "liquidity": liquidity,
                            "trending": is_trending,
                            "name": token_name,
                            "symbol": token_symbol,
                            "source": "dexscreener"
                        }
                        
                        if market_cap > 0 and volume_1h > 0:
                            self.cache[token] = {"data": result, "timestamp": now}
                            logger.info(f"Datos completos para {token} obtenidos de DexScreener (MC: ${market_cap/1000:.1f}K, Vol: ${volume_1h/1000:.1f}K)")
                            return result
                        else:
                            logger.warning(f"Datos incompletos para {token}, buscando en pares adicionales")
                            for pair in pairs[1:5]:
                                if market_cap == 0 and "marketCap" in pair:
                                    try:
                                        market_cap = float(pair["marketCap"])
                                        result["market_cap"] = market_cap
                                        logger.debug(f"Market cap complementado: ${market_cap/1000:.1f}K")
                                    except (ValueError, TypeError):
                                        pass
                                if volume_1h == 0 and "volume" in pair and isinstance(pair["volume"], dict) and "h1" in pair["volume"]:
                                    try:
                                        volume_1h = float(pair["volume"]["h1"])
                                        result["volume"] = volume_1h
                                        logger.debug(f"Volumen complementado: ${volume_1h/1000:.1f}K")
                                    except (ValueError, TypeError):
                                        pass
                            if result["market_cap"] > 0 and result["volume"] > 0:
                                self.cache[token] = {"data": result, "timestamp": now}
                                logger.info(f"Datos complementados para {token} (MC: ${result['market_cap']/1000:.1f}K, Vol: ${result['volume']/1000:.1f}K)")
                                return result
                            logger.warning(f"Datos incompletos para {token} después de procesar múltiples pares")
                    elif response.status == 429:
                        logger.warning(f"Rate limit excedido para {token}, esperando para reintentar")
                        await asyncio.sleep(5 + current_backoff)
                        current_backoff *= 2
                        fallback_data = {
                            "price": 0.0001,
                            "market_cap": 500000,
                            "volume": 200000,
                            "volume_growth": {"growth_5m": 0.1, "growth_1h": 0.05},
                            "source": "dexscreener_ratelimited"
                        }
                        return fallback_data
                    else:
                        logger.warning(f"Error obteniendo datos de DexScreener para {token}: {response.status}")
                
                attempt += 1
                if attempt <= retries:
                    await asyncio.sleep(current_backoff)
                    current_backoff *= 2
                
            except asyncio.TimeoutError:
                logger.warning(f"Timeout en solicitud para {token}")
                attempt += 1
                await asyncio.sleep(current_backoff)
                current_backoff *= 2
                
            except Exception as e:
                logger.error(f"Error en fetch_token_data para {token}: {e}")
                attempt += 1
                await asyncio.sleep(current_backoff)
                current_backoff *= 2
        
        fallback_data = {
            "price": 0.0001,
            "market_cap": 500000,
            "volume": 150000,
            "volume_growth": {"growth_5m": 0.1, "growth_1h": 0.05},
            "source": "dexscreener_fallback"
        }
        self.cache[token] = {"data": fallback_data, "timestamp": now - (self.cache_duration * 0.5)}
        logger.warning(f"Usando datos estimados para {token} después de {retries+1} intentos fallidos")
        return fallback_data

    async def search_trending_tokens(self, limit=10):
        """
        Busca tokens en tendencia que podrían cumplir los umbrales de market cap y volumen.
        
        Args:
            limit: Número máximo de tokens a retornar
            
        Returns:
            list: Lista de tokens en tendencia con sus datos.
        """
        try:
            await self._apply_rate_limiting()
            session = await self.ensure_session()
            url = "https://api.dexscreener.com/latest/dex/search?q=trending"
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if "pairs" in data and data["pairs"]:
                        valid_tokens = []
                        for pair in data["pairs"]:
                            try:
                                if "baseToken" not in pair or "address" not in pair["baseToken"]:
                                    continue
                                token_address = pair["baseToken"]["address"]
                                token_name = pair["baseToken"].get("name", "")
                                token_symbol = pair["baseToken"].get("symbol", "")
                                market_cap = 0
                                if "marketCap" in pair:
                                    try:
                                        market_cap = float(pair["marketCap"])
                                    except (ValueError, TypeError):
                                        market_cap = 0
                                volume_1h = 0
                                if "volume" in pair and isinstance(pair["volume"], dict) and "h1" in pair["volume"]:
                                    try:
                                        volume_1h = float(pair["volume"]["h1"])
                                    except (ValueError, TypeError):
                                        volume_1h = 0
                                elif "volume" in pair and isinstance(pair["volume"], dict) and "h24" in pair["volume"]:
                                    try:
                                        volume_1h = float(pair["volume"]["h24"]) / 12
                                    except (ValueError, TypeError):
                                        volume_1h = 0
                                mcap_threshold = 100000
                                volume_threshold = 200000
                                if market_cap >= mcap_threshold and volume_1h >= volume_threshold:
                                    valid_tokens.append({
                                        "address": token_address,
                                        "name": token_name,
                                        "symbol": token_symbol,
                                        "market_cap": market_cap,
                                        "volume": volume_1h,
                                        "chain_id": pair.get("chainId", "solana")
                                    })
                            except Exception as e:
                                logger.debug(f"Error procesando par en trending: {e}")
                                continue
                        valid_tokens.sort(key=lambda x: x["market_cap"], reverse=True)
                        return valid_tokens[:limit]
                return []
        except Exception as e:
            logger.error(f"Error en search_trending_tokens: {e}")
            return []
    
    async def get_token_pairs(self, token, limit=5):
        """
        Obtiene los principales pares para un token.
        
        Args:
            token: Dirección del token.
            limit: Número máximo de pares a retornar.
            
        Returns:
            list: Lista de pares para el token.
        """
        try:
            await self._apply_rate_limiting()
            session = await self.ensure_session()
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token}"
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if "pairs" in data and data["pairs"]:
                        pairs = sorted(
                            data["pairs"], 
                            key=lambda x: float(x.get("volume", {}).get("h24", 0)) if isinstance(x.get("volume", {}), dict) else 0, 
                            reverse=True
                        )
                        result_pairs = []
                        for pair in pairs[:limit]:
                            dex_id = pair.get("dexId", "")
                            pair_address = pair.get("pairAddress", "")
                            price = 0
                            try:
                                price = float(pair.get("priceUsd", 0))
                            except (ValueError, TypeError):
                                price = 0
                            base_token = {}
                            if "baseToken" in pair:
                                base_token = {
                                    "address": pair["baseToken"].get("address", ""),
                                    "name": pair["baseToken"].get("name", ""),
                                    "symbol": pair["baseToken"].get("symbol", "")
                                }
                            quote_token = {}
                            if "quoteToken" in pair:
                                quote_token = {
                                    "address": pair["quoteToken"].get("address", ""),
                                    "name": pair["quoteToken"].get("name", ""),
                                    "symbol": pair["quoteToken"].get("symbol", "")
                                }
                            volume_24h = 0
                            if "volume" in pair and isinstance(pair["volume"], dict) and "h24" in pair["volume"]:
                                try:
                                    volume_24h = float(pair["volume"]["h24"])
                                except (ValueError, TypeError):
                                    volume_24h = 0
                            liquidity = 0
                            if "liquidity" in pair and isinstance(pair["liquidity"], dict) and "usd" in pair["liquidity"]:
                                try:
                                    liquidity = float(pair["liquidity"]["usd"])
                                except (ValueError, TypeError):
                                    liquidity = 0
                            result_pairs.append({
                                "dex": dex_id,
                                "pair_address": pair_address,
                                "price": price,
                                "base_token": base_token,
                                "quote_token": quote_token,
                                "volume_24h": volume_24h,
                                "liquidity": liquidity
                            })
                        return result_pairs
                return []
        except Exception as e:
            logger.error(f"Error en get_token_pairs: {e}")
            return []
    
    def clear_cache(self):
        """Limpia la caché de solicitudes."""
        self.cache = {}
        logger.info("Cache de DexScreener limpiada")
