# dex_monitor.py
import time
import logging
import asyncio
import aiohttp
import json
from typing import Dict, List, Optional, Any, Tuple
from config import Config

logger = logging.getLogger("dex_monitor")

class DexMonitor:
    """
    Monitor para consultar las APIs de Raydium y Jupiter en Solana.
    Proporciona datos sobre liquidez, precios y rutas de swap.
    """
    
    def __init__(self):
        self.cache = {}
        self.cache_ttl = int(Config.get("DEX_CACHE_TTL", 60))  # 60 segundos por defecto
        self.session = None
        
        # URLs de las APIs
        self.raydium_api_url = "https://api.raydium.io/v2/sdk/liquidity/mainnet.json"
        self.jupiter_api_url = "https://price.jup.ag/v4/price"
        self.jupiter_quote_api_url = "https://quote-api.jup.ag/v4/quote"
        
        # Token USDC (referencia para precios)
        self.usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        # Token SOL wrapped (para swaps)
        self.wsol_mint = "So11111111111111111111111111111111111111112"
    
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
    
    def _get_cache(self, key: str):
        """Obtiene un valor de la caché si está disponible y no ha expirado"""
        if key in self.cache:
            entry = self.cache[key]
            if time.time() - entry["timestamp"] < self.cache_ttl:
                return entry["data"]
        return None
    
    def _set_cache(self, key: str, data: Any):
        """Guarda un valor en la caché con timestamp actual"""
        self.cache[key] = {
            "data": data,
            "timestamp": time.time()
        }
    
    async def get_raydium_pools(self) -> List[Dict]:
        """
        Obtiene todos los pools de liquidez de Raydium.
        
        Returns:
            List[Dict]: Lista de pools de liquidez
        """
        cache_key = "raydium_pools"
        cached_data = self._get_cache(cache_key)
        if cached_data:
            return cached_data
        
        try:
            session = await self.ensure_session()
            async with session.get(self.raydium_api_url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    pools = []
                    if "official" in data:
                        pools.extend(data["official"])
                    if "unOfficial" in data:
                        pools.extend(data["unOfficial"])
                    
                    self._set_cache(cache_key, pools)
                    return pools
                else:
                    logger.warning(f"Error al obtener pools de Raydium: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error en get_raydium_pools: {e}")
            return []
    
    async def get_raydium_liquidity(self, token_mint: str) -> Dict:
        """
        Obtiene información de liquidez de Raydium para un token específico.
        
        Args:
            token_mint: Dirección del token
            
        Returns:
            Dict: Información de liquidez
        """
        cache_key = f"raydium_liquidity_{token_mint}"
        cached_data = self._get_cache(cache_key)
        if cached_data:
            return cached_data
        
        pools = await self.get_raydium_pools()
        token_pools = []
        
        for pool in pools:
            if (pool.get("baseMint") == token_mint or pool.get("quoteMint") == token_mint):
                token_pools.append(pool)
        
        if not token_pools:
            return {
                "total_liquidity_usd": 0,
                "pool_count": 0,
                "platform": "raydium",
                "pools": []
            }
        
        total_liquidity = 0
        for pool in token_pools:
            if "liquidity" in pool and "usdC" in pool["liquidity"]:
                pool_liquidity = float(pool["liquidity"]["usdC"])
                total_liquidity += pool_liquidity
        
        result = {
            "total_liquidity_usd": total_liquidity,
            "pool_count": len(token_pools),
            "platform": "raydium",
            "pools": token_pools
        }
        
        self._set_cache(cache_key, result)
        return result
    
    async def get_jupiter_price(self, token_mint: str) -> Dict:
        """
        Obtiene el precio actual de un token a través de Jupiter.
        
        Args:
            token_mint: Dirección del token
            
        Returns:
            Dict: Información de precio
        """
        cache_key = f"jupiter_price_{token_mint}"
        cached_data = self._get_cache(cache_key)
        if cached_data:
            return cached_data
        
        try:
            url = f"{self.jupiter_api_url}?ids={token_mint}&vsToken={self.usdc_mint}"
            session = await self.ensure_session()
            
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if "data" in data and token_mint in data["data"]:
                        token_data = data["data"][token_mint]
                        result = {
                            "price": token_data.get("price", 0),
                            "volume_24h": token_data.get("volume24h", 0),
                            "market_price": token_data.get("price", 0),
                            "platform": "jupiter"
                        }
                        
                        # Intentar obtener slippage y liquidez
                        if "slippage" in token_data:
                            slippage = token_data["slippage"]
                            result["slippage_1k"] = slippage.get("buySlippage1k", 0) * 100
                            result["slippage_10k"] = slippage.get("buySlippage10k", 0) * 100
                            result["slippage_100k"] = slippage.get("buySlippage100k", 0) * 100
                        
                        # Estimar liquidez basada en slippage o volumen
                        if "liquidity" in token_data:
                            result["liquidity"] = token_data["liquidity"]
                        else:
                            # Estimación rudimentaria de liquidez basada en volumen 24h
                            result["liquidity"] = result["volume_24h"] * 0.5
                            
                            # Refinamiento basado en slippage si está disponible
                            if "slippage" in token_data and "buyAmount" in token_data["slippage"]:
                                buy_amount = token_data["slippage"]["buyAmount"]
                                slippage_pct = token_data["slippage"].get("buySlippage", 0.01)
                                if slippage_pct > 0:
                                    refined_estimate = buy_amount / slippage_pct
                                    result["liquidity"] = max(result["liquidity"], refined_estimate)
                        
                        self._set_cache(cache_key, result)
                        return result
                
                return {"price": 0, "volume_24h": 0, "platform": "jupiter"}
        except Exception as e:
            logger.error(f"Error en get_jupiter_price para {token_mint}: {e}")
            return {"price": 0, "volume_24h": 0, "platform": "jupiter"}
    
    async def get_jupiter_route(self, 
                                input_mint: str, 
                                output_mint: str, 
                                amount_in: float) -> Dict:
        """
        Obtiene la mejor ruta de swap a través de Jupiter.
        
        Args:
            input_mint: Token de entrada
            output_mint: Token de salida
            amount_in: Cantidad de entrada en unidades del token
            
        Returns:
            Dict: Información de la ruta de swap
        """
        cache_key = f"jupiter_route_{input_mint}_{output_mint}_{amount_in}"
        cached_data = self._get_cache(cache_key)
        if cached_data:
            return cached_data
        
        try:
            # Convertir a unidades lamports (multiplicar por 10^decimals)
            amount_in_lamports = int(amount_in * 1000000)  # Asumiendo 6 decimales como USDC
            
            url = f"{self.jupiter_quote_api_url}?inputMint={input_mint}&outputMint={output_mint}&amount={amount_in_lamports}&slippageBps=50"
            session = await self.ensure_session()
            
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    self._set_cache(cache_key, data)
                    return data
                else:
                    logger.warning(f"Error obteniendo ruta de Jupiter: {response.status}")
                    return {}
        except Exception as e:
            logger.error(f"Error en get_jupiter_route: {e}")
            return {}
    
    async def calculate_slippage(self, token_mint: str, amounts_usd: List[float]) -> Dict[str, float]:
        """
        Calcula el slippage para diferentes cantidades de swap.
        
        Args:
            token_mint: Dirección del token
            amounts_usd: Lista de cantidades en USD para calcular slippage
            
        Returns:
            Dict[str, float]: Porcentajes de slippage para cada cantidad
        """
        result = {}
        
        try:
            # Obtener precio actual como referencia
            price_data = await self.get_jupiter_price(token_mint)
            current_price = price_data.get("price", 0)
            
            if current_price <= 0:
                # Sin precio válido, no podemos calcular slippage
                return {f"slippage_{int(amount)}": 100 for amount in amounts_usd}
            
            for amount_usd in amounts_usd:
                # Calcular cantidad de tokens equivalente al monto USD
                amount_tokens = amount_usd / current_price
                
                # Obtener ruta de swap del token al USDC
                route_data = await self.get_jupiter_route(token_mint, self.usdc_mint, amount_tokens)
                
                if not route_data or "outAmount" not in route_data:
                    result[f"slippage_{int(amount_usd)}"] = 100
                    continue
                
                # Calcular precio efectivo con slippage
                out_amount = int(route_data["outAmount"]) / 1000000  # Convertir a USDC (6 decimales)
                effective_price = out_amount / amount_tokens
                
                # Calcular slippage como porcentaje
                slippage_pct = abs((effective_price - current_price) / current_price) * 100
                result[f"slippage_{int(amount_usd)}"] = slippage_pct
            
            return result
        except Exception as e:
            logger.error(f"Error en calculate_slippage para {token_mint}: {e}")
            return {f"slippage_{int(amount)}": 100 for amount in amounts_usd}
    
    async def get_combined_liquidity_data(self, token_mint: str) -> Dict:
        """
        Obtiene datos combinados de liquidez de Raydium y Jupiter.
        
        Args:
            token_mint: Dirección del token
            
        Returns:
            Dict: Datos combinados de liquidez
        """
        cache_key = f"combined_liquidity_{token_mint}"
        cached_data = self._get_cache(cache_key)
        if cached_data:
            return cached_data
        
        # Obtener datos de ambas plataformas en paralelo
        raydium_task = asyncio.create_task(self.get_raydium_liquidity(token_mint))
        jupiter_task = asyncio.create_task(self.get_jupiter_price(token_mint))
        
        raydium_data = await raydium_task
        jupiter_data = await jupiter_task
        
        # Calcular slippage para diferentes cantidades
        slippage_task = asyncio.create_task(self.calculate_slippage(token_mint, [1000, 10000]))
        slippage_data = await slippage_task
        
        # Combinar datos
        raydium_liquidity = raydium_data.get("total_liquidity_usd", 0)
        jupiter_liquidity = jupiter_data.get("liquidity", 0)
        
        # Usar el valor más alto como más confiable
        total_liquidity = max(raydium_liquidity, jupiter_liquidity)
        
        # Si ambos tienen valores, promediar pero con más peso al más alto
        if raydium_liquidity > 0 and jupiter_liquidity > 0:
            max_value = max(raydium_liquidity, jupiter_liquidity)
            min_value = min(raydium_liquidity, jupiter_liquidity)
            total_liquidity = max_value * 0.7 + min_value * 0.3
        
        result = {
            "total_liquidity_usd": total_liquidity,
            "price": jupiter_data.get("price", 0),
            "volume_24h": jupiter_data.get("volume_24h", 0),
            "slippage_1k": slippage_data.get("slippage_1000", jupiter_data.get("slippage_1k", 0)),
            "slippage_10k": slippage_data.get("slippage_10000", jupiter_data.get("slippage_10k", 0)),
            "sources": [],
            "pools": raydium_data.get("pools", [])
        }
        
        # Agregar fuentes
        if raydium_liquidity > 0:
            result["sources"].append("raydium")
        if jupiter_liquidity > 0:
            result["sources"].append("jupiter")
        
        # Calcular profundidad de liquidez (por ejemplo, ratio de liquidez vs volumen)
        if result["volume_24h"] > 0:
            result["liquidity_depth"] = total_liquidity / result["volume_24h"]
        else:
            result["liquidity_depth"] = 0
        
        # Calcular health score
        if total_liquidity > 0:
            slippage_factor = min(1, 10 / (result["slippage_10k"] if result["slippage_10k"] > 0 else 100))
            volume_factor = min(1, result["volume_24h"] / 100000)
            result["health_score"] = (slippage_factor * 0.7) + (volume_factor * 0.3)
        else:
            result["health_score"] = 0
        
        self._set_cache(cache_key, result)
        return result

    def cleanup_cache(self):
        """Limpia entradas antiguas de la caché"""
        now = time.time()
        keys_to_remove = []
        
        for key, entry in self.cache.items():
            if now - entry["timestamp"] > self.cache_ttl * 2:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.cache[key]
        
        logger.debug(f"Limpieza de caché: eliminadas {len(keys_to_remove)} entradas")
