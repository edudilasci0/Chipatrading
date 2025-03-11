import time
import asyncio
from config import Config

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, gmgn_client=None, rugcheck_api=None, ml_predictor=None):
        """
        Inicializa la lógica de señales.
        Se inyectan clientes para obtener datos de mercado y otros servicios.
        """
        self.scoring_system = scoring_system
        self.helius_client = helius_client
        self.gmgn_client = gmgn_client  # Cliente GMGN como respaldo
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.performance_tracker = None
        self.token_candidates = {}  # Estructura: {token: {wallets, transactions, last_update, volume_usd}}
        self.recent_signals = []    # Lista de (token, timestamp, confidence, signal_id)
        self.last_signal_check = time.time()

    async def check_signals_periodically(self, interval=30):
        """
        Verifica y procesa candidatos para generar señales de forma periódica.
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
        Esta función es un ejemplo básico; integra aquí la lógica de tu aplicación.
        """
        now = time.time()
        # Usamos SIGNAL_WINDOW_SECONDS del Config (o 540 segundos por defecto)
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
                # Ejemplo: calcular un porcentaje de compras sobre todas las transacciones
                buy_txs = [tx for tx in recent_txs if tx["type"] == "BUY"]
                buy_percentage = len(buy_txs) / max(1, len(recent_txs))
                
                # Condiciones mínimas basadas en la configuración
                min_traders = int(Config.get("MIN_TRADERS_FOR_SIGNAL", 2))
                min_volume = float(Config.get("MIN_VOLUME_USD", 2000))
                if trader_count < min_traders or volume_usd < min_volume:
                    continue

                # Se pueden agregar llamadas a clientes para obtener datos de mercado, etc.
                # En este ejemplo se asigna un valor base
                confidence = 0.5

                # (Aquí se puede agregar lógica adicional: obtener datos de mercado, ajustar por memecoin, etc.)

                candidate = {
                    "token": token,
                    "confidence": confidence,
                    "trader_count": trader_count,
                    "volume_usd": volume_usd,
                    "recent_transactions": recent_txs,
                    "initial_price": 0  # Valor de ejemplo
                }
                candidates.append(candidate)
            except Exception as e:
                print(f"Error procesando candidato {token}: {e}")

        # Ordenar candidatos por confianza (de mayor a menor)
        candidates.sort(key=lambda x: x["confidence"], reverse=True)
        await self._generate_signals(candidates)

    async def _generate_signals(self, candidates):
        """
        Genera señales basadas en los candidatos procesados.
        Aquí se puede integrar la lógica para almacenar señales y enviar notificaciones.
        """
        for candidate in candidates:
            print(f"✅ Señal generada para {candidate['token']} con confianza {candidate['confidence']:.2f}")
            # Aquí se puede agregar la lógica para guardar la señal en la base de datos
            # y enviar alertas por Telegram, etc.
