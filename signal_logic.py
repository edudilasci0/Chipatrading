import time
import asyncio
import logging
from config import Config
import db
from dexscreener_client import DexScreenerClient  # Importamos el nuevo módulo

logger = logging.getLogger("signal_logic")

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, gmgn_client=None, rugcheck_api=None, ml_predictor=None, pattern_detector=None):
        self.scoring_system = scoring_system
        self.helius_client = helius_client
        self.gmgn_client = gmgn_client
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.pattern_detector = pattern_detector
        
        # Inicializar DexScreener como fuente alternativa
        self.dexscreener_client = DexScreenerClient()
        
        self.performance_tracker = None
        self.token_candidates = {}
        self.recent_signals = []
        self.last_signal_check = time.time()
        self.watched_tokens = set()
        self._init_token_type_scores()

    def _init_token_type_scores(self):
        """
        Inicializa las puntuaciones base por tipo de token.
        """
        self.token_type_scores = {
            "meme": 1.2,
            "daily_runner": 1.3,
            "standard": 1.0
        }

    async def get_token_market_data(self, token):
        """
        Obtiene datos de mercado utilizando múltiples APIs,
        intentando cada una en orden hasta obtener datos válidos.
        """
        data = None
        source = "none"
        
        # 1. Intentar con Helius
        if self.helius_client:
            try:
                data = self.helius_client.get_token_data(token)
                if data and data.get("market_cap", 0) > 0:
                    source = "helius"
                    logger.debug(f"Datos de {token} obtenidos de Helius")
            except Exception as e:
                logger.warning(f"Error API Helius para {token}: {str(e)[:100]}")
        
        # 2. Intentar con GMGN si Helius falló
        if not data or data.get("market_cap", 0) == 0:
            if self.gmgn_client:
                try:
                    gmgn_data = self.gmgn_client.get_market_data(token)
                    if gmgn_data and gmgn_data.get("market_cap", 0) > 0:
                        data = gmgn_data
                        source = "gmgn"
                        logger.debug(f"Datos de {token} obtenidos de GMGN")
                except Exception as e:
                    logger.warning(f"Error API GMGN para {token}: {str(e)[:100]}")
        
        # 3. Intentar con DexScreener si las anteriores fallan
        if not data or data.get("market_cap", 0) == 0:
            if hasattr(self, 'dexscreener_client') and self.dexscreener_client:
                try:
                    dex_data = await self.dexscreener_client.fetch_token_data(token)
                    if dex_data and dex_data.get("market_cap", 0) > 0:
                        data = dex_data
                        source = "dexscreener"
                        logger.debug(f"Datos de {token} obtenidos de DexScreener")
                except Exception as e:
                    logger.warning(f"Error API DexScreener para {token}: {str(e)[:100]}")
        
        # 4. Si ninguna API funciona, usar valores predeterminados seguros
        if not data or data.get("market_cap", 0) == 0:
            data = {
                "price": 0.00001,
                "market_cap": 1000000,  # 1M por defecto
                "volume": 10000,
                "volume_growth": {"growth_5m": 0.1, "growth_1h": 0.05},
                "estimated": True
            }
            source = "default"
            logger.warning(f"Usando datos predeterminados para {token} - todas las APIs fallaron")
        
        data["source"] = source
        return data

    # ... Resto de métodos de SignalLogic (process_transaction, _process_candidates, _generate_signals, etc.)
