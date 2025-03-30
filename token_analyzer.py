import asyncio
import logging
from config import Config

logger = logging.getLogger("token_analyzer")

class TokenAnalyzer:
    def __init__(self, dexscreener_client=None):
        self.dexscreener_client = dexscreener_client

    async def update_price_data(self, token: str, current_price: float = None, current_volume: float = None, market_data: dict = None) -> dict:
        """
        Actualiza y devuelve datos actualizados para un token usando DexScreener.
        """
        try:
            if self.dexscreener_client:
                data = await self.dexscreener_client.fetch_token_data(token)
                if data:
                    return data
            return {"price": 0.00001, "market_cap": 0, "volume": 0}
        except Exception as e:
            logger.error(f"Error en update_price_data para {token}: {e}")
            return {"price": 0.00001, "market_cap": 0, "volume": 0}
