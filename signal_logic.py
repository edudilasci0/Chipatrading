import time
import asyncio
import logging
from config import Config
import db

logger = logging.getLogger("signal_logic")

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, gmgn_client=None, rugcheck_api=None, ml_predictor=None, pattern_detector=None):
        # Inicialización de componentes
        self.scoring_system = scoring_system
        self.helius_client = helius_client
        self.gmgn_client = gmgn_client
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.pattern_detector = pattern_detector
        self.performance_tracker = None
        self.token_candidates = {}
        self.recent_signals = []
        self.last_signal_check = time.time()
        self.last_cleanup = time.time()
        self.watched_tokens = set()

    def process_transaction(self, tx_data):
        # [Lógica de procesamiento actual, sin cambios relevantes]
        try:
            # Validación, actualización de estructuras y logs detallados...
            # (código similar al anterior)
            pass
        except Exception as e:
            logger.error(f"Error procesando transacción en SignalLogic: {e}", exc_info=True)

    def _classify_token_opportunity(self, token, recent_txs, market_data):
        """
        Clasifica oportunidades con énfasis en memecoins y daily runners.
        Devuelve 'daily_runner', 'meme', o None según los criterios.
        """
        token_type = None
        # Ejemplo: Si el crecimiento de volumen es muy alto y market cap es bajo, es daily_runner
        if market_data.get("market_cap", 0) < 5_000_000 and market_data.get("volume_growth", {}).get("growth_5m", 0) > 0.3:
            token_type = "daily_runner"
        # Si se detectan keywords de memecoin (usando lista similar a _classify_token_type)
        meme_keywords = ["pepe", "doge", "shib", "inu", "moon", "elon", "wojak"]
        for tx in recent_txs:
            token_name = tx.get("token_name", "").lower()
            if any(keyword in token_name for keyword in meme_keywords):
                token_type = "meme"
                break
        return token_type

    def _monitor_rapid_volume_changes(self, token, candidate):
        """
        Monitorea cambios rápidos en el volumen. Si la tasa de cambio de volumen supera cierto umbral,
        se marca la oportunidad.
        """
        txs = candidate.get("transactions", [])
        if len(txs) < 2:
            return False
        # Calcular la diferencia de volumen entre las últimas dos transacciones
        vol_diff = abs(txs[-1]["amount_usd"] - txs[-2]["amount_usd"])
        threshold = float(Config.get("RAPID_VOLUME_THRESHOLD", 5000))
        if vol_diff > threshold:
            logger.info(f"Rápido cambio de volumen detectado en {token}: Δ${vol_diff:.2f}")
            return True
        return False

    async def get_token_market_data(self, token):
        """
        Obtiene datos de mercado con mejor integración con Helius y manejo de fallbacks.
        """
        data = None
        source = "none"
        if self.helius_client:
            try:
                data = self.helius_client.get_token_data(token)
                if data and data.get("market_cap", 0) > 0:
                    source = "helius"
                    logger.debug(f"Datos de {token} obtenidos de Helius")
            except Exception as e:
                logger.error(f"Error API Helius para {token}: {e}", exc_info=True)
        if not data or data.get("market_cap", 0) == 0:
            if self.gmgn_client:
                try:
                    gmgn_data = self.gmgn_client.get_market_data(token)
                    if gmgn_data and gmgn_data.get("market_cap", 0) > 0:
                        data = gmgn_data
                        source = "gmgn"
                        logger.debug(f"Datos de {token} obtenidos de GMGN")
                except Exception as e:
                    logger.error(f"Error API GMGN para {token}: {e}", exc_info=True)
        if not data:
            data = {"price": 0, "market_cap": 0, "volume": 0, "volume_growth": {"growth_5m": 0, "growth_1h": 0}, "estimated": True}
            source = "none"
            logger.debug(f"Datos por defecto usados para {token}")
        data["source"] = source
        return data

    async def _process_candidates(self):
        """
        Procesa candidatos para generar señales, priorizando daily runners y aplicando lógica para señales inmediatas.
        """
        try:
            now = time.time()
            window = float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
            cutoff = now - window
            candidates = []
            for token, data in list(self.token_candidates.items()):
                recent_txs = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
                if not recent_txs or token in self.watched_tokens:
                    continue
                trader_count = len(data["wallets"])
                volume_usd = sum(tx["amount_usd"] for tx in recent_txs)
                buy_txs = [tx for tx in recent_txs if tx.get("type") == "BUY"]
                buy_ratio = len(buy_txs) / max(1, len(recent_txs))
                timestamps = [tx["timestamp"] for tx in recent_txs]
                tx_velocity = len(recent_txs) / max(1, (max(timestamps) - min(timestamps))/60)
                
                # Clasificar oportunidad usando el nuevo método
                token_opportunity = self._classify_token_opportunity(token, recent_txs, await self.get_token_market_data(token))
                
                # Priorizar daily runners y aplicar lógica de señales inmediatas si se detecta cambio rápido de volumen
                is_immediate = self._monitor_rapid_volume_changes(token, data)
                if token_opportunity in ["daily_runner", "meme"] or is_immediate:
                    logger.info(f"Señal inmediata detectada para {token} ({token_opportunity})")
                    # Aquí se puede activar lógica de señal inmediata sin relajar umbrales
                # Cálculo de confianza, integración con scoring, etc.
                # [Continuar con la lógica existente y añadir prioridad para daily runners]
                candidate = {
                    "token": token,
                    "confidence": self.scoring_system.compute_confidence(
                        [self.scoring_system.get_score(w) for w in data["wallets"]],
                        volume_usd, 
                        (await self.get_token_market_data(token)).get("market_cap", 0),
                        recent_volume_growth=(await self.get_token_market_data(token)).get("volume_growth", {}).get("growth_5m", 0),
                        token_type=token_opportunity,
                        whale_activity=data.get("whale_activity", False)
                    ),
                    "trader_count": trader_count,
                    "volume_usd": volume_usd,
                    "recent_transactions": recent_txs,
                    "tx_velocity": tx_velocity,
                    "token_type": token_opportunity
                }
                candidates.append(candidate)
            candidates.sort(key=lambda x: x["confidence"], reverse=True)
            # Se pueden aplicar filtros adicionales para daily runners de alta calidad
            await self._generate_signals(candidates)
        except Exception as e:
            logger.error(f"Error en _process_candidates: {e}", exc_info=True)

    async def _generate_signals(self, candidates):
        """
        Genera señales a partir de candidatos procesados.
        (Se mantiene la estructura original, integrando la lógica de señales inmediatas)
        """
        # [Implementación similar a la versión anterior]
        pass

    async def check_signals_periodically(self):
        """
        Ejecuta periódicamente la verificación de señales.
        """
        while True:
            try:
                if time.time() - self.last_signal_check > 60:
                    await self._process_candidates()
                    self.last_signal_check = time.time()
            except Exception as e:
                logger.error(f"Error en check_signals_periodically: {e}", exc_info=True)
            await asyncio.sleep(10)
