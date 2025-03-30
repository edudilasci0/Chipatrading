import asyncio
import logging
from config import Config

logger = logging.getLogger("market_metrics")

class MarketMetricsAnalyzer:
    def __init__(self, dexscreener_client=None):
        self.dexscreener_client = dexscreener_client
        # Se elimina helius_client

    async def get_holder_growth(self, token: str) -> dict:
        """
        Obtiene el crecimiento de holders para un token usando DexScreener.
        """
        try:
            if self.dexscreener_client:
                data = await self.dexscreener_client.fetch_token_data(token)
                if data:
                    # Supongamos que DexScreener nos devuelve holderCount o similar
                    return {
                        "holder_count": data.get("holders", 0),
                        "growth_rate_1h": data.get("holderGrowthRate1h", 0)
                    }
            return {"holder_count": 0, "growth_rate_1h": 0}
        except Exception as e:
            logger.error(f"Error en get_holder_growth para {token}: {e}")
            return {"holder_count": 0, "growth_rate_1h": 0}
