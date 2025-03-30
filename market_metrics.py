import asyncio
import logging
from config import Config

logger = logging.getLogger("market_metrics")

class MarketMetricsAnalyzer:
    def __init__(self, dexscreener_client=None):
        self.dexscreener_client = dexscreener_client

    async def fetch_token_data(self, token: str) -> dict:
        try:
            if self.dexscreener_client:
                data = await self.dexscreener_client.fetch_token_data(token)
                if data:
                    return data
            logger.warning(f"No se pudieron obtener datos de mercado para {token}")
        except Exception as e:
            logger.error(f"Error en fetch_token_data para {token}: {e}")
        return {
            "price": 0.00001,
            "market_cap": 1000000,
            "volume": 10000,
            "liquidity": 5000,
            "holders": 25,
            "volume_growth": {"growth_5m": 0.1, "growth_1h": 0.05},
            "source": "default"
        }
