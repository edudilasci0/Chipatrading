import logging
from core.dexscreener_client import DexScreenerClient
from core.market_metrics import MarketMetricsAnalyzer
from core.token_analyzer import TokenAnalyzer
from core.trader_profiler import TraderProfiler
from core.db import DB
from utils.config import Config

logger = logging.getLogger("signal_logic")


class SignalLogic:
    def __init__(self, wallet_tracker=None, scoring_system=None, performance_tracker=None):
        self.dexscreener = DexScreenerClient()
        self.metrics_analyzer = MarketMetricsAnalyzer(self.dexscreener)
        self.token_analyzer = TokenAnalyzer(self.dexscreener)
        self.trader_profiler = TraderProfiler()
        self.db = DB()
        self.wallet_tracker = wallet_tracker
        self.scoring_system = scoring_system
        self.performance_tracker = performance_tracker

    def _verify_and_signal(self, tx_data):
        token = tx_data["token"]
        wallet = tx_data["wallet"]

        # Nuevos umbrales reducidos
        mcap_threshold = float(Config.get("MCAP_THRESHOLD", "50000"))
        volume_threshold = float(Config.get("VOLUME_THRESHOLD", "100000"))

        market_data = self.dexscreener.get_token_data(token)
        if not market_data:
            logger.warning(f"Sin datos de mercado para token {token}")
            return False

        market_cap = market_data.get("marketCap", 0)
        volume = market_data.get("volume", 0)

        wallet_score = self.db.get_wallet_score(wallet)

        logger.info(
            f"Verificando token {token} - MC: ${market_cap/1000:.1f}K (umbral: ${mcap_threshold/1000:.1f}K), Vol: ${volume/1000:.1f}K (umbral: ${volume_threshold/1000:.1f}K)"
        )
        logger.debug(
            f"[DEBUG] Token {token} | MC: {market_cap}, Vol: {volume}, Score: {wallet_score}, Wallet: {wallet}"
        )

        if market_cap < mcap_threshold or volume < volume_threshold:
            return False

        logger.info(f"✅ Señal generada para token {token}")
        return True

    def process_transaction(self, tx_data):
        try:
            if self._verify_and_signal(tx_data):
                self.db.save_signal(tx_data)
                if self.performance_tracker:
                    self.performance_tracker.track(tx_data)
                logger.info(f"🔔 Señal procesada para {tx_data['token']}")
        except Exception as e:
            logger.error(f"Error procesando transacción: {e}")
