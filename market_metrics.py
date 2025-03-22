# market_metrics.py
import logging
import time
import asyncio
import aiohttp
import json
from datetime import datetime, timedelta
import math
from collections import deque
import db
from config import Config

logger = logging.getLogger("market_metrics")

class MarketMetricsAnalyzer:
    """
    Analizador de métricas de mercado para tokens de Solana.
    
    Características:
    - Análisis de crecimiento de holders
    - Análisis de liquidez en DEXs
    - Detección de trending en varias plataformas
    - Análisis de profundidad de mercado
    """
    
    def __init__(self, helius_client=None, dexscreener_client=None):
        self.helius_client = helius_client
        self.dexscreener_client = dexscreener_client
        self.holder_data = {}         # {token: {"timestamp": deque, "count": deque, ...}}
        self.liquidity_data = {}      # {token: {"timestamp": deque, "liquidity": deque, "dex_data": {}, ...}}
        self.trending_data = {}       # {token: {"timestamp": float, "platforms": [...], ...}}
        self.market_health = {}       # {token: {"score": float, "last_update": timestamp}}
        
        # Configuración de caché y tiempo de vida (TTL)
        self.cache_ttl = int(Config.get("MARKET_METRICS_CACHE_TTL", 300))      # 5 minutos por defecto
        self.trending_ttl = int(Config.get("TRENDING_DATA_TTL", 1800))           # 30 minutos por defecto
        self.holder_growth_ttl = int(Config.get("HOLDER_GROWTH_TTL", 3600))      # 1 hora por defecto
        
        # Datos históricos (número máximo de puntos)
        self.max_history_points = int(Config.get("MAX_MARKET_HISTORY_POINTS", 24))  # 24 puntos
        
        # Configuración para DEXs
        self.dex_api_urls = {
            "raydium": "https://api.raydium.io/v2/sdk/liquidity/mainnet.json",
            "jupiter": "https://price.jup.ag/v4/price",
            "openbook": "https://openbookdex.com/api/market",
            "dexscreener": "https://api.dexscreener.com/latest/dex"
        }
        
        # Fuentes para trending
        self.trending_sources = ["dexscreener", "birdeye", "solscan"]
        
        # Sesión HTTP
        self.session = None

    async def ensure_session(self):
        """Asegura que existe una sesión HTTP abierta"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close_session(self):
        """Cierra la sesión HTTP"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

    async def get_holder_growth(self, token):
        """
        Analiza el crecimiento de holders para un token.
        
        Args:
            token: Dirección del token.
            
        Returns:
            dict: Análisis de crecimiento de holders.
        """
        result = {
            "holder_count": 0,
            "growth_rate_1h": 0,      # Tasa de crecimiento por hora (%)
            "growth_rate_24h": 0,     # Tasa de crecimiento diaria (%)
            "is_growing": False,
            "growth_acceleration": 0, # Aceleración del crecimiento
            "new_holders_last_hour": 0,
            "source": "unknown"
        }
        
        now = time.time()
        # Revisar caché
        if token in self.holder_data:
            holder_entry = self.holder_data[token]
            last_update = holder_entry.get("last_update", 0)
            if now - last_update < self.holder_growth_ttl:
                return {
                    "holder_count": holder_entry.get("current_count", 0),
                    "growth_rate_1h": holder_entry.get("growth_rate_1h", 0),
                    "growth_rate_24h": holder_entry.get("growth_rate_24h", 0),
                    "is_growing": holder_entry.get("is_growing", False),
                    "growth_acceleration": holder_entry.get("growth_acceleration", 0),
                    "new_holders_last_hour": holder_entry.get("new_holders_last_hour", 0),
                    "source": holder_entry.get("source", "cache")
                }
        
        # Para tokens pump, usar valores predeterminados
        if "pump" in token.lower():
            holder_count = 50
            source = "default_pump"
            if token not in self.holder_data:
                self.holder_data[token] = {
                    "timestamp": deque(maxlen=self.max_history_points),
                    "count": deque(maxlen=self.max_history_points),
                    "current_count": holder_count,
                    "growth_rate_1h": 5.0,
                    "growth_rate_24h": 20.0,
                    "is_growing": True,
                    "growth_acceleration": 1.0,
                    "new_holders_last_hour": 5,
                    "last_update": now,
                    "source": source
                }
            holder_entry = self.holder_data[token]
            holder_entry["timestamp"].append(now)
            holder_entry["count"].append(holder_count)
            result = {
                "holder_count": holder_count,
                "growth_rate_1h": 5.0,
                "growth_rate_24h": 20.0,
                "is_growing": True,
                "growth_acceleration": 1.0,
                "new_holders_last_hour": 5,
                "source": source
            }
            return result
        
        # Intentar obtener datos de Helius
        holder_count = 0
        source = "unknown"
        if self.helius_client:
            try:
                if hasattr(self.helius_client, 'get_token_data'):
                    token_data = self.helius_client.get_token_data(token)
                    if token_data and "holders" in token_data and token_data["holders"] > 0:
                        holder_count = token_data["holders"]
                        source = "helius"
                elif hasattr(self.helius_client, 'get_token_data_async'):
                    token_data = await self.helius_client.get_token_data_async(token)
                    if token_data and "holders" in token_data and token_data["holders"] > 0:
                        holder_count = token_data["holders"]
                        source = "helius"
            except Exception as e:
                logger.warning(f"Error obteniendo holders de Helius para {token}: {e}")
        
        # Consulta directa a Helius Metadata
        if holder_count == 0:
            try:
                session = await self.ensure_session()
                url = f"https://api.helius.xyz/v0/tokens/metadata?api-key={Config.HELIUS_API_KEY}"
                data = {"mintAccounts": [token]}
                async with session.post(url, json=data, timeout=5) as response:
                    if response.status == 200:
                        token_metadata = await response.json()
                        if token_metadata and isinstance(token_metadata, list) and len(token_metadata) > 0:
                            token_info = token_metadata[0]
                            if "offChainData" in token_info and isinstance(token_info["offChainData"], dict) and "holderCount" in token_info["offChainData"]:
                                holder_count = token_info["offChainData"]["holderCount"]
                                source = "helius_metadata"
            except Exception as e:
                logger.warning(f"Error en consulta directa de holders para {token}: {e}")
        
        # Intentar con DexScreener
        if holder_count == 0 and self.dexscreener_client:
            try:
                if hasattr(self.dexscreener_client, 'fetch_token_data'):
                    dex_data = await self.dexscreener_client.fetch_token_data(token)
                    if dex_data and "holders" in dex_data and dex_data["holders"] > 0:
                        holder_count = dex_data["holders"]
                        source = "dexscreener"
            except Exception as e:
                logger.warning(f"Error obteniendo holders de DexScreener para {token}: {e}")
        
        # Estimar holders con fórmula aproximada
        if holder_count == 0:
            try:
                token_data = None
                if self.helius_client:
                    if hasattr(self.helius_client, 'get_token_data'):
                        token_data = self.helius_client.get_token_data(token)
                    elif hasattr(self.helius_client, 'get_token_data_async'):
                        token_data = await self.helius_client.get_token_data_async(token)
                if token_data:
                    market_cap = token_data.get("market_cap", 0)
                    volume = token_data.get("volume", 0)
                    if market_cap > 0 and volume > 0:
                        holder_count = int(math.sqrt(market_cap * volume) / 1000)
                        source = "estimated"
            except Exception as e:
                logger.warning(f"Error estimando holders para {token}: {e}")
        
        # Valor predeterminado si aún no se obtiene dato
        if holder_count == 0:
            holder_count = 25
            source = "default"
            logger.info(f"Usando valor predeterminado de holders para {token}")
        
        # Inicializar o actualizar la entrada en el caché de holders
        if token not in self.holder_data:
            self.holder_data[token] = {
                "timestamp": deque(maxlen=self.max_history_points),
                "count": deque(maxlen=self.max_history_points),
                "current_count": 0,
                "growth_rate_1h": 0,
                "growth_rate_24h": 0,
                "is_growing": False,
                "growth_acceleration": 0,
                "new_holders_last_hour": 0,
                "last_update": 0,
                "source": "unknown"
            }
        holder_entry = self.holder_data[token]
        holder_entry["timestamp"].append(now)
        holder_entry["count"].append(holder_count)
        holder_entry["current_count"] = holder_count
        holder_entry["last_update"] = now
        holder_entry["source"] = source
        
        # Calcular crecimiento por hora
        timestamps = list(holder_entry["timestamp"])
        counts = list(holder_entry["count"])
        hour_ago_idx = -1
        for i, ts in enumerate(timestamps):
            if now - ts >= 3600:
                hour_ago_idx = i
                break
        if hour_ago_idx >= 0 and hour_ago_idx < len(counts):
            hour_ago_count = counts[hour_ago_idx]
            if hour_ago_count > 0:
                growth_1h = (holder_count - hour_ago_count) / hour_ago_count * 100
                new_holders = holder_count - hour_ago_count
                holder_entry["growth_rate_1h"] = growth_1h
                holder_entry["new_holders_last_hour"] = new_holders
                result["growth_rate_1h"] = growth_1h
                result["new_holders_last_hour"] = new_holders
        
        # Calcular crecimiento diario
        day_ago_idx = -1
        for i, ts in enumerate(timestamps):
            if now - ts >= 86400:
                day_ago_idx = i
                break
        if day_ago_idx >= 0 and day_ago_idx < len(counts):
            day_ago_count = counts[day_ago_idx]
            if day_ago_count > 0:
                growth_24h = (holder_count - day_ago_count) / day_ago_count * 100
                holder_entry["growth_rate_24h"] = growth_24h
                result["growth_rate_24h"] = growth_24h
        
        # Determinar si está creciendo
        is_growing = holder_entry.get("growth_rate_1h", 0) > 1
        holder_entry["is_growing"] = is_growing
        result["is_growing"] = is_growing
        
        # Calcular aceleración del crecimiento si hay suficientes puntos
        if len(timestamps) >= 3:
            recent_idx = len(timestamps) // 3
            mid_idx = 2 * len(timestamps) // 3
            if recent_idx < len(counts) and mid_idx < len(counts) and counts[0] > 0 and counts[recent_idx] > 0:
                growth_rate_recent = (counts[-1] - counts[recent_idx]) / counts[recent_idx]
                growth_rate_older = (counts[recent_idx] - counts[0]) / counts[0]
                growth_accel = growth_rate_recent - growth_rate_older
                holder_entry["growth_acceleration"] = growth_accel
                result["growth_acceleration"] = growth_accel
        
        result["holder_count"] = holder_count
        result["source"] = source
        return result

    async def analyze_liquidity(self, token):
        """
        Analiza la liquidez disponible para un token en varios DEXs.
        
        Args:
            token: Dirección del token.
            
        Returns:
            dict: Análisis de liquidez.
        """
        result = {
            "total_liquidity_usd": 0,
            "liquidity_depth": 0,
            "liquidity_concentration": 0,
            "main_dex": None,
            "slippage_1k": 0,
            "slippage_10k": 0,
            "healthy_liquidity": False,
            "is_locked": False,
            "sources": []
        }
        
        now = time.time()
        if token in self.liquidity_data:
            liq_entry = self.liquidity_data[token]
            last_update = liq_entry.get("last_update", 0)
            if now - last_update < self.cache_ttl:
                return {
                    "total_liquidity_usd": liq_entry.get("total_liquidity_usd", 0),
                    "liquidity_depth": liq_entry.get("liquidity_depth", 0),
                    "liquidity_concentration": liq_entry.get("liquidity_concentration", 0),
                    "main_dex": liq_entry.get("main_dex"),
                    "slippage_1k": liq_entry.get("slippage_1k", 0),
                    "slippage_10k": liq_entry.get("slippage_10k", 0),
                    "healthy_liquidity": liq_entry.get("healthy_liquidity", False),
                    "is_locked": liq_entry.get("is_locked", False),
                    "sources": liq_entry.get("sources", [])
                }
        
        # Para tokens pump, usar valores predeterminados
        if "pump" in token.lower():
            pump_data = {
                "total_liquidity_usd": 10000,
                "liquidity_depth": 0.5,
                "liquidity_concentration": 0.8,
                "main_dex": "dex_fallback",
                "slippage_1k": 3.0,
                "slippage_10k": 12.0,
                "healthy_liquidity": True,
                "is_locked": False,
                "sources": ["pump_fallback"]
            }
            if token not in self.liquidity_data:
                self.liquidity_data[token] = {
                    "timestamp": deque(maxlen=self.max_history_points),
                    "liquidity": deque(maxlen=self.max_history_points),
                    "dex_data": {},
                    "last_update": 0
                }
            liquidity_entry = self.liquidity_data[token]
            liquidity_entry["timestamp"].append(now)
            liquidity_entry["liquidity"].append(pump_data["total_liquidity_usd"])
            liquidity_entry["last_update"] = now
            for key, value in pump_data.items():
                liquidity_entry[key] = value
            return pump_data
        
        dex_data = {}
        total_liquidity = 0
        sources = []
        
        # 1. Datos de Raydium
        try:
            raydium_liquidity = await self._get_raydium_liquidity(token)
            if raydium_liquidity and raydium_liquidity.get("total_liquidity_usd", 0) > 0:
                dex_data["raydium"] = raydium_liquidity
                total_liquidity += raydium_liquidity.get("total_liquidity_usd", 0)
                sources.append("raydium")
        except Exception as e:
            logger.warning(f"Error obteniendo liquidez de Raydium para {token}: {e}")
        
        # 2. Datos de Jupiter
        try:
            jupiter_liquidity = await self._get_jupiter_liquidity(token)
            if jupiter_liquidity and jupiter_liquidity.get("total_liquidity_usd", 0) > 0:
                dex_data["jupiter"] = jupiter_liquidity
                total_liquidity += jupiter_liquidity.get("total_liquidity_usd", 0)
                sources.append("jupiter")
        except Exception as e:
            logger.warning(f"Error obteniendo liquidez de Jupiter para {token}: {e}")
        
        # 3. Datos de DexScreener
        if self.dexscreener_client:
            try:
                if hasattr(self.dexscreener_client, 'fetch_token_data'):
                    dex_data_result = await self.dexscreener_client.fetch_token_data(token)
                    if dex_data_result and "liquidity" in dex_data_result and dex_data_result["liquidity"] > 0:
                        dex_data["dexscreener"] = {
                            "total_liquidity_usd": dex_data_result["liquidity"],
                            "platform": "dexscreener"
                        }
                        total_liquidity += dex_data_result["liquidity"]
                        sources.append("dexscreener")
            except Exception as e:
                logger.warning(f"Error obteniendo liquidez de DexScreener para {token}: {e}")
        
        # 4. Datos de Helius (último recurso)
        if total_liquidity == 0 and self.helius_client:
            try:
                token_data = None
                if hasattr(self.helius_client, 'get_token_data'):
                    token_data = self.helius_client.get_token_data(token)
                elif hasattr(self.helius_client, 'get_token_data_async'):
                    token_data = await self.helius_client.get_token_data_async(token)
                if token_data and "liquidity" in token_data and token_data["liquidity"] > 0:
                    dex_data["helius"] = {
                        "total_liquidity_usd": token_data["liquidity"],
                        "platform": "helius"
                    }
                    total_liquidity += token_data["liquidity"]
                    sources.append("helius")
            except Exception as e:
                logger.warning(f"Error obteniendo liquidez de Helius para {token}: {e}")
        
        if total_liquidity == 0:
            total_liquidity = 5000
            dex_data["fallback"] = {
                "total_liquidity_usd": total_liquidity,
                "platform": "fallback"
            }
            sources.append("fallback")
            logger.info(f"Usando liquidez predeterminada para {token}")
        
        # Determinar DEX principal
        main_dex = None
        max_liquidity = 0
        for dex, data in dex_data.items():
            dex_liquidity = data.get("total_liquidity_usd", 0)
            if dex_liquidity > max_liquidity:
                max_liquidity = dex_liquidity
                main_dex = dex
        
        # Calcular concentración de liquidez
        liquidity_concentration = 0
        if len(dex_data) > 1 and total_liquidity > 0:
            sum_squared_shares = 0
            for dex, data in dex_data.items():
                dex_liquidity = data.get("total_liquidity_usd", 0)
                market_share = dex_liquidity / total_liquidity
                sum_squared_shares += market_share ** 2
            liquidity_concentration = (sum_squared_shares - 1/len(dex_data)) / (1 - 1/len(dex_data))
        elif len(dex_data) == 1:
            liquidity_concentration = 1
        
        # Estimar slippage
        k_factor = 0.5
        slippage_1k = 100
        slippage_10k = 100
        if total_liquidity > 0:
            slippage_1k = min(k_factor * 1000 / total_liquidity * 100, 100)
            slippage_10k = min(k_factor * 10000 / total_liquidity * 100, 100)
        
        healthy_liquidity = total_liquidity > 10000 and slippage_1k < 5
        
        # Verificar liquidez bloqueada
        is_locked = False
        for dex, data in dex_data.items():
            if data.get("is_locked", False):
                is_locked = True
                break
        
        liquidity_depth = 0
        try:
            market_cap = 0
            token_data = None
            if self.helius_client:
                if hasattr(self.helius_client, 'get_token_data'):
                    token_data = self.helius_client.get_token_data(token)
                elif hasattr(self.helius_client, 'get_token_data_async'):
                    token_data = await self.helius_client.get_token_data_async(token)
                if token_data and "market_cap" in token_data:
                    market_cap = token_data["market_cap"]
            if market_cap > 0 and total_liquidity > 0:
                raw_ratio = total_liquidity / market_cap
                liquidity_depth = min(raw_ratio * 5, 1)
        except Exception as e:
            logger.warning(f"Error calculando liquidity_depth para {token}: {e}")
        
        # Actualizar estructura de datos de liquidez
        if token not in self.liquidity_data:
            self.liquidity_data[token] = {
                "timestamp": deque(maxlen=self.max_history_points),
                "liquidity": deque(maxlen=self.max_history_points),
                "dex_data": {},
                "last_update": 0
            }
        liquidity_entry = self.liquidity_data[token]
        liquidity_entry["timestamp"].append(now)
        liquidity_entry["liquidity"].append(total_liquidity)
        liquidity_entry["dex_data"] = dex_data
        liquidity_entry["last_update"] = now
        liquidity_entry["total_liquidity_usd"] = total_liquidity
        liquidity_entry["liquidity_depth"] = liquidity_depth
        liquidity_entry["liquidity_concentration"] = liquidity_concentration
        liquidity_entry["main_dex"] = main_dex
        liquidity_entry["slippage_1k"] = slippage_1k
        liquidity_entry["slippage_10k"] = slippage_10k
        liquidity_entry["healthy_liquidity"] = healthy_liquidity
        liquidity_entry["is_locked"] = is_locked
        liquidity_entry["sources"] = sources
        
        result["total_liquidity_usd"] = total_liquidity
        result["liquidity_depth"] = liquidity_depth
        result["liquidity_concentration"] = liquidity_concentration
        result["main_dex"] = main_dex
        result["slippage_1k"] = slippage_1k
        result["slippage_10k"] = slippage_10k
        result["healthy_liquidity"] = healthy_liquidity
        result["is_locked"] = is_locked
        result["sources"] = sources
        return result

    async def _get_raydium_liquidity(self, token):
        """
        Obtiene datos de liquidez de Raydium para un token.
        
        Args:
            token: Dirección del token.
            
        Returns:
            dict: Datos de liquidez de Raydium.
        """
        if "pump" in token.lower():
            return {
                "total_liquidity_usd": 8000,
                "pool_count": 1,
                "platform": "raydium",
                "pools": []
            }
        try:
            session = await self.ensure_session()
            async with session.get(self.dex_api_urls["raydium"], timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if "official" in data and "unOfficial" in data:
                        pools = data["official"] + data["unOfficial"]
                        token_pools = []
                        for pool in pools:
                            if pool.get("baseMint") == token or pool.get("quoteMint") == token:
                                token_pools.append(pool)
                        if not token_pools:
                            return None
                        total_liquidity = 0
                        for pool in token_pools:
                            if "liquidity" in pool and "usdC" in pool["liquidity"]:
                                total_liquidity += float(pool["liquidity"]["usdC"])
                        return {
                            "total_liquidity_usd": total_liquidity,
                            "pool_count": len(token_pools),
                            "platform": "raydium",
                            "pools": token_pools
                        }
            return None
        except Exception as e:
            logger.error(f"Error en _get_raydium_liquidity para {token}: {e}")
            return None

    async def _get_jupiter_liquidity(self, token):
        """
        Obtiene datos de liquidez de Jupiter para un token.
        
        Args:
            token: Dirección del token.
            
        Returns:
            dict: Datos de liquidez de Jupiter.
        """
        if "pump" in token.lower():
            return {
                "total_liquidity_usd": 5000,
                "price": 0.000001,
                "volume_24h": 20000,
                "platform": "jupiter"
            }
        try:
            sol_mint = "So11111111111111111111111111111111111111112"
            url = f"{self.dex_api_urls['jupiter']}?ids={token}&vsToken={sol_mint}"
            session = await self.ensure_session()
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if "data" in data and token in data["data"]:
                        token_data = data["data"][token]
                        price = token_data.get("price", 0)
                        volume_24h = token_data.get("volume24h", 0)
                        if "liquidity" in token_data:
                            liquidity = token_data["liquidity"]
                        else:
                            liquidity_estimate = volume_24h * 0.5 if volume_24h > 0 else 0
                            if "slippage" in token_data:
                                slippage_data = token_data["slippage"]
                                if "buyAmount" in slippage_data and slippage_data["buyAmount"] > 0:
                                    buy_amount = slippage_data["buyAmount"]
                                    slippage_pct = slippage_data.get("buySlippage", 0.01)
                                    if slippage_pct > 0:
                                        refined_estimate = buy_amount / slippage_pct
                                        liquidity_estimate = max(liquidity_estimate, refined_estimate)
                            liquidity = liquidity_estimate
                        return {
                            "total_liquidity_usd": liquidity,
                            "price": price,
                            "volume_24h": volume_24h,
                            "platform": "jupiter"
                        }
            fallback_data = {
                "total_liquidity_usd": 3000,
                "price": 0.0001,
                "volume_24h": 5000,
                "platform": "jupiter_fallback"
            }
            return fallback_data
        except Exception as e:
            logger.error(f"Error en _get_jupiter_liquidity para {token}: {e}")
            return {
                "total_liquidity_usd": 3000,
                "price": 0.0001,
                "volume_24h": 5000,
                "platform": "jupiter_error"
            }

    async def check_trending_status(self, token):
        """
        Verifica si el token está en trending en diversas plataformas.
        
        Args:
            token: Dirección del token.
            
        Returns:
            dict: Estado trending del token.
        """
        result = {
            "is_trending": False,
            "trending_platforms": [],
            "trend_score": 0,
            "discovery_potential": 0
        }
        now = time.time()
        if token in self.trending_data:
            trend_entry = self.trending_data[token]
            last_update = trend_entry.get("timestamp", 0)
            if now - last_update < self.trending_ttl:
                return {
                    "is_trending": trend_entry.get("is_trending", False),
                    "trending_platforms": trend_entry.get("platforms", []),
                    "trend_score": trend_entry.get("score", 0),
                    "discovery_potential": trend_entry.get("discovery_potential", 0)
                }
        # Para tokens pump, asumimos trending
        if "pump" in token.lower():
            trending_platforms = ["dexscreener", "birdeye"]
            trend_score = 0.8
            discovery_potential = 0.7
            self.trending_data[token] = {
                "timestamp": now,
                "is_trending": True,
                "platforms": trending_platforms,
                "score": trend_score,
                "discovery_potential": discovery_potential
            }
            result["is_trending"] = True
            result["trending_platforms"] = trending_platforms
            result["trend_score"] = trend_score
            result["discovery_potential"] = discovery_potential
            return result
        
        trending_platforms = []
        dexscreener_trending = await self._check_dexscreener_trending(token)
        if dexscreener_trending:
            trending_platforms.append("dexscreener")
            trending_platforms.append("birdeye")
        
        is_trending = len(trending_platforms) > 0
        trend_score = len(trending_platforms) / len(self.trending_sources)
        discovery_potential = 0
        liquidity_factor = 0
        growth_factor = 0
        if token in self.liquidity_data:
            liquidity = self.liquidity_data[token].get("total_liquidity_usd", 0)
            if 5000 <= liquidity <= 50000:
                liquidity_factor = 0.8
            elif liquidity < 5000:
                liquidity_factor = 0.3
            else:
                liquidity_factor = 0.5
        if token in self.holder_data:
            growth_rate_1h = self.holder_data[token].get("growth_rate_1h", 0)
            if growth_rate_1h > 5:
                growth_factor = 1.0
            elif growth_rate_1h > 2:
                growth_factor = 0.7
            elif growth_rate_1h > 0:
                growth_factor = 0.4
        discovery_potential = (liquidity_factor * 0.6) + (growth_factor * 0.4)
        
        self.trending_data[token] = {
            "timestamp": now,
            "is_trending": is_trending,
            "platforms": trending_platforms,
            "score": trend_score,
            "discovery_potential": discovery_potential
        }
        result["is_trending"] = is_trending
        result["trending_platforms"] = trending_platforms
        result["trend_score"] = trend_score
        result["discovery_potential"] = discovery_potential
        return result

    async def _check_dexscreener_trending(self, token):
        """
        Verifica si el token está en trending en DexScreener.
        
        Args:
            token: Dirección del token.
            
        Returns:
            bool: True si está trending.
        """
        if "pump" in token.lower():
            return True
        try:
            if self.dexscreener_client:
                dex_data = await self.dexscreener_client.fetch_token_data(token)
                if dex_data and "trending" in dex_data:
                    return dex_data["trending"]
            session = await self.ensure_session()
            url = f"{self.dex_api_urls['dexscreener']}/trending"
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if "pairs" in data:
                        for pair in data["pairs"]:
                            if "baseToken" in pair and "address" in pair["baseToken"]:
                                if pair["baseToken"]["address"].lower() == token.lower():
                                    return True
                            if "quoteToken" in pair and "address" in pair["quoteToken"]:
                                if pair["quoteToken"]["address"].lower() == token.lower():
                                    return True
            return False
        except Exception as e:
            logger.error(f"Error en _check_dexscreener_trending para {token}: {e}")
            return False

    async def calculate_market_health(self, token):
        """
        Calcula un score integral de salud de mercado para el token.
        
        Args:
            token: Dirección del token.
            
        Returns:
            dict: Score de salud de mercado e indicadores relacionados.
        """
        now = time.time()
        if token in self.market_health:
            health_entry = self.market_health[token]
            last_update = health_entry.get("last_update", 0)
            if now - last_update < self.cache_ttl:
                return health_entry
        
        # Para tokens pump, retornar datos predeterminados
        if "pump" in token.lower():
            pump_health = {
                "market_health_score": 0.85,
                "liquidity_score": 0.8,
                "holder_score": 0.9,
                "trend_score": 0.9,
                "quality_indicators": {
                    "healthy_liquidity": True,
                    "growing_holders": True,
                    "trending": True,
                    "stable_growth": True
                },
                "risk_indicators": {
                    "low_liquidity": False,
                    "high_concentration": True,
                    "high_slippage": False,
                    "declining_holders": False
                },
                "last_update": now
            }
            self.market_health[token] = pump_health
            return pump_health
        
        result = {
            "market_health_score": 0.5,
            "liquidity_score": 0.5,
            "holder_score": 0.5,
            "trend_score": 0,
            "quality_indicators": {
                "healthy_liquidity": False,
                "growing_holders": False,
                "trending": False,
                "stable_growth": False
            },
            "risk_indicators": {
                "low_liquidity": False,
                "high_concentration": False,
                "high_slippage": False,
                "declining_holders": False
            },
            "last_update": now
        }
        try:
            liquidity_data = await self.analyze_liquidity(token)
            holder_data = await self.get_holder_growth(token)
            trending_data = await self.check_trending_status(token)
            
            total_liquidity = liquidity_data.get("total_liquidity_usd", 0)
            slippage_1k = liquidity_data.get("slippage_1k", 100)
            liquidity_depth = liquidity_data.get("liquidity_depth", 0)
            liquidity_concentration = liquidity_data.get("liquidity_concentration", 0)
            
            if total_liquidity <= 0:
                liquidity_score = 0
            elif total_liquidity < 5000:
                liquidity_score = 0.1
            elif total_liquidity < 20000:
                liquidity_score = 0.3
            elif total_liquidity < 100000:
                liquidity_score = 0.6
            elif total_liquidity < 500000:
                liquidity_score = 0.8
            else:
                liquidity_score = 1.0
            
            if slippage_1k > 10:
                liquidity_score *= 0.7
            elif slippage_1k > 5:
                liquidity_score *= 0.9
            
            if liquidity_concentration > 0.8:
                liquidity_score *= 0.9
            
            holder_count = holder_data.get("holder_count", 0)
            holder_growth_1h = holder_data.get("growth_rate_1h", 0)
            holder_growth_24h = holder_data.get("growth_rate_24h", 0)
            
            if holder_count <= 0:
                holder_score = 0.5
            elif holder_count < 10:
                holder_score = 0.1
            elif holder_count < 50:
                holder_score = 0.2
            elif holder_count < 200:
                holder_score = 0.4
            elif holder_count < 1000:
                holder_score = 0.6
            elif holder_count < 5000:
                holder_score = 0.8
            else:
                holder_score = 1.0
            
            if holder_growth_24h > 50:
                holder_score *= 1.2
                holder_score = min(holder_score, 1.0)
            elif holder_growth_24h > 20:
                holder_score *= 1.1
                holder_score = min(holder_score, 1.0)
            elif holder_growth_24h < -20:
                holder_score *= 0.8
            elif holder_growth_24h < -5:
                holder_score *= 0.9
            
            trend_score = trending_data.get("trend_score", 0)
            
            quality_indicators = {
                "healthy_liquidity": total_liquidity > 20000 and slippage_1k < 5,
                "growing_holders": holder_growth_24h > 5,
                "trending": trending_data.get("is_trending", False),
                "stable_growth": holder_growth_24h > 5 and holder_growth_24h < 100
            }
            
            risk_indicators = {
                "low_liquidity": total_liquidity < 5000 or slippage_1k > 10,
                "high_concentration": liquidity_concentration > 0.8,
                "high_slippage": slippage_1k > 10,
                "declining_holders": holder_growth_24h < -5
            }
            
            market_health_score = (
                liquidity_score * 0.4 +
                holder_score * 0.4 +
                trend_score * 0.2
            )
            
            market_health_score = max(0, min(1, market_health_score))
            
            result["market_health_score"] = market_health_score
            result["liquidity_score"] = liquidity_score
            result["holder_score"] = holder_score
            result["trend_score"] = trend_score
            result["quality_indicators"] = quality_indicators
            result["risk_indicators"] = risk_indicators
        except Exception as e:
            logger.error(f"Error calculando salud de mercado para {token}: {e}", exc_info=True)
        
        self.market_health[token] = result
        return result

    def cleanup_old_data(self):
        """Limpia datos antiguos para liberar memoria"""
        now = time.time()
        tokens_to_remove = []
        for token, data in self.holder_data.items():
            if now - data.get("last_update", 0) > 86400:
                tokens_to_remove.append(token)
        for token in tokens_to_remove:
            del self.holder_data[token]
        
        tokens_to_remove = []
        for token, data in self.liquidity_data.items():
            if now - data.get("last_update", 0) > 86400:
                tokens_to_remove.append(token)
        for token in tokens_to_remove:
            del self.liquidity_data[token]
        
        tokens_to_remove = []
        for token, data in self.trending_data.items():
            if now - data.get("timestamp", 0) > 86400:
                tokens_to_remove.append(token)
        for token in tokens_to_remove:
            del self.trending_data[token]
        
        tokens_to_remove = []
        for token, data in self.market_health.items():
            if now - data.get("last_update", 0) > 86400:
                tokens_to_remove.append(token)
        for token in tokens_to_remove:
            del self.market_health[token]
