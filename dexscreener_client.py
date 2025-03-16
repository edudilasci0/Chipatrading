import time
import asyncio
import aiohttp
import logging

logger = logging.getLogger("dexscreener_client")

class DexScreenerClient:
    """
    Cliente minimalista para DexScreener
    """
    def __init__(self):
        self.cache = {}
        self.cache_duration = 60  # 1 minuto de caché
        self.request_timestamps = []
        self.rate_limit = 10  # 10 peticiones por minuto

    async def _apply_rate_limiting(self):
        now = time.time()
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
        if len(self.request_timestamps) >= self.rate_limit:
            oldest = min(self.request_timestamps)
            wait_time = 60 - (now - oldest) + 0.2
            if wait_time > 0:
                await asyncio.sleep(wait_time)
        self.request_timestamps.append(time.time())

    async def fetch_token_data(self, token):
        now = time.time()
        if token in self.cache and now - self.cache[token]["timestamp"] < self.cache_duration:
            return self.cache[token]["data"]
        await self._apply_rate_limiting()
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.dexscreener.com/latest/dex/tokens/{token}"
                async with session.get(url, timeout=5) as response:
                    if response.status == 200:
                        data = await response.json()
                        if "pairs" in data and data["pairs"]:
                            pairs = sorted(data["pairs"], key=lambda x: float(x["volume"].get("h24", 0)), reverse=True)
                            active_pair = pairs[0]
                            h1_vol = float(active_pair["volume"].get("h1", 0))
                            mcap = float(active_pair.get("marketCap", 0))
                            price = float(active_pair.get("priceUsd", 0))
                            growth_1h = active_pair.get("priceChange", {}).get("h1", 0) / 100 if "priceChange" in active_pair else 0
                            growth_5m = active_pair.get("priceChange", {}).get("m5", 0) / 100 if "priceChange" in active_pair else 0
                            result = {
                                "price": price,
                                "market_cap": mcap,
                                "volume": h1_vol,
                                "volume_growth": {"growth_5m": growth_5m, "growth_1h": growth_1h},
                                "source": "dexscreener"
                            }
                            self.cache[token] = {"data": result, "timestamp": now}
                            logger.info(f"Datos para {token} obtenidos de DexScreener")
                            return result
                    else:
                        logger.warning(f"Error obteniendo datos de DexScreener para {token}: {response.status}")
            return None
        except Exception as e:
            logger.error(f"Error en fetch_token_data para {token}: {e}")
            return None
