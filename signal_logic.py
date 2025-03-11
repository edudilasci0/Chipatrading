import time
import asyncio
from config import Config

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, gmgn_client=None, rugcheck_api=None, ml_predictor=None):
        """
        Inicializa la lógica de señales.
        Se inyectan clientes para obtener datos de mercado.
        """
        self.scoring_system = scoring_system
        self.helius_client = helius_client
        self.gmgn_client = gmgn_client
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.performance_tracker = None
        self.token_candidates = {}  # Estructura: {token: {wallets, transactions, last_update, volume_usd}}
        self.recent_signals = []    # Lista de (token, timestamp, confidence, signal_id)
        self.last_signal_check = time.time()

    async def check_signals_periodically(self, interval=30):
        """
        Ejecuta la verificación de señales de forma periódica.
        """
        while True:
            try:
                await self._process_candidates()
            except Exception as e:
                print(f"Error en check_signals_periodically: {e}")
            await asyncio.sleep(interval)

    async def _process_candidates(self):
        """
        Procesa los tokens candidatos para generar señales.
        Esta función es un ejemplo; integra aquí la lógica de tu aplicación.
        """
        now = time.time()
        # Usar el valor de configuración o un valor por defecto (9 minutos)
        window_seconds = float(Config.get("SIGNAL_WINDOW_SECONDS", 540))
        cutoff = now - window_seconds
        candidates = []

        for token, data in list(self.token_candidates.items()):
            try:
                recent_txs = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
                if not recent_txs:
                    continue

                trader_count = len(data["wallets"])
                volume_usd = sum(tx["amount_usd"] for tx in recent_txs)
                # Aquí puedes calcular la confianza según tu lógica; por ejemplo, se asigna un valor base:
                confidence = 0.5

                candidate = {
                    "token": token,
                    "confidence": confidence,
                    "trader_count": trader_count,
                    "volume_usd": volume_usd,
                    "recent_transactions": recent_txs,
                    "initial_price": 0  # Valor de ejemplo; ajústalo según tu lógica
                }
                candidates.append(candidate)
            except Exception as e:
                print(f"Error procesando candidato {token}: {e}")

        candidates.sort(key=lambda x: x["confidence"], reverse=True)
        await self._generate_signals(candidates)

    async def _generate_signals(self, candidates):
        """
        Genera señales basadas en los candidatos procesados.
        Aquí se debe integrar la lógica para guardar la señal y enviar notificaciones.
        """
        for candidate in candidates:
            print(f"✅ Señal generada para {candidate['token']} con confianza {candidate['confidence']:.2f}")
            # Aquí agregar lógica para guardar la señal en la base de datos y enviar alertas (p.ej., por Telegram)
