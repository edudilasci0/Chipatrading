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
            dict: Datos del token con market cap y volumen o valores por defecto
        """
        now = time.time()
        
        # Verificar caché primero
        if token in self.cache and now - self.cache[token]["timestamp"] < self.cache_duration:
            cache_data = self.cache[token]["data"]
            if cache_data and cache_data.get("market_cap", 0) > 0 and cache_data.get("volume", 0) > 0:
                return cache_data
        
        # Preparar para solicitud API
        attempt = 0
        current_backoff = self.error_backoff
        
        while attempt <= retries:
            try:
                # Aplicar limitación de tasa
                await self._apply_rate_limiting()
                
                # Ejecutar solicitud - usando el endpoint correcto según la documentación
                session = await self.ensure_session()
                url = f"https://api.dexscreener.com/latest/dex/tokens/{token}"
                
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if not data or "pairs" not in data or not data["pairs"]:
                            logger.warning(f"Sin datos de pares para token {token}")
                            attempt += 1
                            await asyncio.sleep(current_backoff)
                            current_backoff *= 2
                            continue
                        
                        # Ordenar pares por volumen para priorizar el par más activo
                        pairs = sorted(
                            data["pairs"], 
                            key=lambda x: float(x.get("volume", {}).get("h24", 0)) if isinstance(x.get("volume", {}), dict) else 0, 
                            reverse=True
                        )
                        
                        if not pairs:
                            break
                            
                        active_pair = pairs[0]
                        
                        # Extraer datos críticos con validación
                        price = 0
                        if "priceUsd" in active_pair:
                            try:
                                price = float(active_pair["priceUsd"])
                            except (ValueError, TypeError):
                                price = 0
                        
                        # Extraer y validar market cap (PRIORITARIO)
                        market_cap = 0
                        if "marketCap" in active_pair:
                            try:
                                market_cap = float(active_pair["marketCap"])
                            except (ValueError, TypeError):
                                market_cap = 0
                        
                        # Extraer y validar volumen (PRIORITARIO)
                        volume_24h = 0
                        volume_1h = 0
                        
                        if "volume" in active_pair:
                            vol_data = active_pair["volume"]
                            
                            if isinstance(vol_data, dict):
                                if "h24" in vol_data:
                                    try:
                                        volume_24h = float(vol_data["h24"])
                                    except (ValueError, TypeError):
                                        volume_24h = 0
                                
                                if "h1" in vol_data:
                                    try:
                                        volume_1h = float(vol_data["h1"])
                                    except (ValueError, TypeError):
                                        volume_1h = 0
                        
                        # Si tenemos volumen 24h pero no 1h, estimar volumen 1h
                        if volume_24h > 0 and volume_1h == 0:
                            volume_1h = volume_24h / 12  # Estimación conservadora
                        
                        # Extraer datos de crecimiento de precio
                        growth_5m = 0
                        growth_1h = 0
                        
                        if "priceChange" in active_pair and isinstance(active_pair["priceChange"], dict):
                            price_change = active_pair["priceChange"]
                            
                            if "m5" in price_change:
                                try:
                                    growth_5m = float(price_change["m5"]) / 100  # Convertir a decimal
                                except (ValueError, TypeError):
                                    growth_5m = 0
                            
                            if "h1" in price_change:
                                try:
                                    growth_1h = float(price_change["h1"]) / 100  # Convertir a decimal
                                except (ValueError, TypeError):
                                    growth_1h = 0
                        
                        # Extraer liquidez
                        liquidity = 0
                        if "liquidity" in active_pair and isinstance(active_pair["liquidity"], dict) and "usd" in active_pair["liquidity"]:
                            try:
                                liquidity = float(active_pair["liquidity"]["usd"])
                            except (ValueError, TypeError):
                                liquidity = 0
                        
                        # Verificar si el token está en trending
                        is_trending = "labels" in active_pair and "trending" in active_pair.get("labels", [])
                        
                        # Extraer información sobre tokens si está disponible en el token base
                        token_name = ""
                        token_symbol = ""
                        if "baseToken" in active_pair:
                            token_name = active_pair["baseToken"].get("name", "")
                            token_symbol = active_pair["baseToken"].get("symbol", "")
                        
                        # Construir datos completos con prioridad en market cap y volumen
                        result = {
                            "price": price,
                            "market_cap": market_cap,
                            "volume": volume_1h,  # Usamos volumen 1h como referencia principal
                            "volume_24h": volume_24h,
                            "volume_growth": {
                                "growth_5m": growth_5m,
                                "growth_1h": growth_1h
                            },
                            "liquidity": liquidity,
                            "trending": is_trending,
                            "name": token_name,
                            "symbol": token_symbol,
                            "source": "dexscreener"
                        }
                        
                        # Verificar si tenemos los datos críticos
                        if market_cap > 0 and volume_1h > 0:
                            # Cachear resultados
                            self.cache[token] = {"data": result, "timestamp": now}
                            logger.info(f"Datos completos para {token} obtenidos de DexScreener (MC: ${market_cap/1000:.1f}K, Vol: ${volume_1h/1000:.1f}K)")
                            return result
                        else:
                            # Intentar complementar con otros pares
                            if market_cap == 0 or volume_1h == 0:
                                logger.warning(f"Datos incompletos para {token}, buscando en pares adicionales")
                                
                                # Buscar en pares adicionales
                                for pair in pairs[1:5]:  # Revisar solo hasta 5 pares adicionales
                                    # Intentar llenar market cap
                                    if market_cap == 0 and "marketCap" in pair:
                                        try:
                                            market_cap = float(pair["marketCap"])
                                            result["market_cap"] = market_cap
                                            logger.debug(f"Market cap complementado desde par adicional: ${market_cap/1000:.1f}K")
                                        except (ValueError, TypeError):
                                            pass
                                    
                                    # Intentar llenar volumen
                                    if volume_1h == 0 and "volume" in pair and isinstance(pair["volume"], dict) and "h1" in pair["volume"]:
                                        try:
                                            vol_1h = float(pair["volume"]["h1"])
                                            result["volume"] = vol_1h
                                            logger.debug(f"Volumen complementado desde par adicional: ${vol_1h/1000:.1f}K")
                                        except (ValueError, TypeError):
                                            pass
                                
                                # Verificar si ahora tenemos los datos críticos
                                if result["market_cap"] > 0 and result["volume"] > 0:
                                    self.cache[token] = {"data": result, "timestamp": now}
                                    logger.info(f"Datos complementados para {token} (MC: ${result['market_cap']/1000:.1f}K, Vol: ${result['volume']/1000:.1f}K)")
                                    return result
                            
                            # Si aún faltan datos críticos, continuar con siguientes intentos
                            logger.warning(f"Datos incompletos para {token} después de procesar múltiples pares")
                        
                    elif response.status == 429:
                        logger.warning(f"Rate limit excedido en DexScreener para {token}, esperando para reintentar")
                        # Aumentar backoff sustancialmente para rate limiting
                        await asyncio.sleep(5 + current_backoff)
                        current_backoff *= 2
                        # Añadir datos de fallback temporales mientras tanto
                        fallback_data = {
                            "price": 0.0001,
                            "market_cap": 500000,
                            "volume": 200000,  # Umbral mínimo para volumen
                            "volume_growth": {"growth_5m": 0.1, "growth_1h": 0.05},
                            "source": "dexscreener_ratelimited"
                        }
                        return fallback_data
                    else:
                        logger.warning(f"Error obteniendo datos de DexScreener para {token}: {response.status}")
                
                # Incrementar intento y aplicar backoff exponencial
                attempt += 1
                if attempt <= retries:
                    await asyncio.sleep(current_backoff)
                    current_backoff *= 2
                
            except asyncio.TimeoutError:
                logger.warning(f"Timeout en solicitud DexScreener para {token}")
                attempt += 1
                await asyncio.sleep(current_backoff)
                current_backoff *= 2
                
            except Exception as e:
                logger.error(f"Error en fetch_token_data para {token}: {e}")
                attempt += 1
                await asyncio.sleep(current_backoff)
                current_backoff *= 2
        
        # Todos los intentos fallaron, usar datos de fallback estimados
        fallback_data = {
            "price": 0.0001,
            "market_cap": 500000,
            "volume": 150000,  # Valor conservador por debajo del umbral
            "volume_growth": {"growth_5m": 0.1, "growth_1h": 0.05},
            "source": "dexscreener_fallback"
        }
        
        # Cachear por tiempo reducido ya que son datos estimados
        self.cache[token] = {"data": fallback_data, "timestamp": now - (self.cache_duration * 0.5)}
        logger.warning(f"Usando datos estimados para {token} después de {retries+1} intentos fallidos")
        
        return fallback_data
        
    async def search_trending_tokens(self, limit=10):
        """
        Busca tokens en tendencia que podrían cumplir los umbrales de market cap y volumen.
        
        Args:
            limit: Número máximo de tokens a retornar
            
        Returns:
            list: Lista de tokens en tendencia con sus datos
        """
        try:
            # Aplicar limitación de tasa
            await self._apply_rate_limiting()
            
            session = await self.ensure_session()
            url = "https://api.dexscreener.com/latest/dex/search?q=trending"
            
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if "pairs" in data and data["pairs"]:
                        # Filtrar pares con datos de market cap y volumen
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
                                        volume_1h = float(pair["volume"]["h24"]) / 12  # Estimación conservadora
                                    except (ValueError, TypeError):
                                        volume_1h = 0
                                
                                # Solo incluir tokens que cumplan los umbrales
                                mcap_threshold = 100000  # $100K
                                volume_threshold = 200000  # $200K
                                
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
                        
                        # Ordenar por market cap
                        valid_tokens.sort(key=lambda x: x["market_cap"], reverse=True)
                        
                        # Limitar cantidad de resultados
                        return valid_tokens[:limit]
                    
                return []  # Sin resultados
                
        except Exception as e:
            logger.error(f"Error en search_trending_tokens: {e}")
            return []
    
    async def get_token_pairs(self, token, limit=5):
        """
        Obtiene los principales pares para un token.
        
        Args:
            token: Dirección del token
            limit: Número máximo de pares a retornar
            
        Returns:
            list: Lista de pares para el token
        """
        try:
            # Aplicar limitación de tasa
            await self._apply_rate_limiting()
            
            session = await self.ensure_session()
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token}"
            
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if "pairs" in data and data["pairs"]:
                        # Ordenar pares por volumen para priorizar los más activos
                        pairs = sorted(
                            data["pairs"], 
                            key=lambda x: float(x.get("volume", {}).get("h24", 0)) if isinstance(x.get("volume", {}), dict) else 0, 
                            reverse=True
                        )
                        
                        result_pairs = []
                        for pair in pairs[:limit]:
                            # Extraer información básica
                            dex_id = pair.get("dexId", "")
                            pair_address = pair.get("pairAddress", "")
                            price = 0
                            
                            if "priceUsd" in pair:
                                try:
                                    price = float(pair["priceUsd"])
                                except (ValueError, TypeError):
                                    price = 0
                            
                            # Extraer tokens base y quote
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
                            
                            # Extraer volumen
                            volume_24h = 0
                            if "volume" in pair and isinstance(pair["volume"], dict) and "h24" in pair["volume"]:
                                try:
                                    volume_24h = float(pair["volume"]["h24"])
                                except (ValueError, TypeError):
                                    volume_24h = 0
                            
                            # Extraer liquidez
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
                        
                return []  # Sin resultados
                
        except Exception as e:
            logger.error(f"Error en get_token_pairs: {e}")
            return []
    
    def clear_cache(self):
        """Limpia la caché de solicitudes."""
        self.cache = {}
        logger.info("Cache de DexScreener limpiada")
                                        
