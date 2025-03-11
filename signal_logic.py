import time
import asyncio
from config import Config
import logging

logger = logging.getLogger("signal_logic")

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

    def get_active_candidates_count(self):
        """
        Retorna el número de tokens candidatos que están siendo monitoreados actualmente.
        
        Returns:
            int: Número de tokens candidatos activos
        """
        return len(self.token_candidates)

    def process_transaction(self, tx_data):
        """
        Procesa una transacción para actualizar los candidatos de señales.
        
        Args:
            tx_data: Datos de la transacción
        """
        try:
            if not tx_data:
                return
                
            token = tx_data.get("token")
            wallet = tx_data.get("wallet")
            amount_usd = tx_data.get("amount_usd", 0)
            
            if not token or not wallet or amount_usd < Config.MIN_TRANSACTION_USD:
                return
                
            timestamp = tx_data.get("timestamp", int(time.time()))
            tx_data["timestamp"] = timestamp  # Asegurar que hay timestamp
            
            # Inicializar estructura para token si no existe
            if token not in self.token_candidates:
                self.token_candidates[token] = {
                    "wallets": set(),
                    "transactions": [],
                    "last_update": timestamp,
                    "volume_usd": 0
                }
            
            # Actualizar datos
            self.token_candidates[token]["wallets"].add(wallet)
            self.token_candidates[token]["transactions"].append(tx_data)
            self.token_candidates[token]["last_update"] = timestamp
            self.token_candidates[token]["volume_usd"] += amount_usd
            
            # Limpieza periódica de transacciones antiguas (mayor a 24h)
            if timestamp % 100 == 0:  # Cada 100 transacciones aproximadamente
                self._cleanup_old_data()
                
        except Exception as e:
            logger.error(f"Error procesando transacción: {e}")
    
    def _cleanup_old_data(self):
        """
        Elimina datos antiguos (transacciones de más de 24h) para no acumular memoria.
        """
        now = time.time()
        cutoff = now - 86400  # 24 horas
        
        tokens_to_remove = []
        for token, data in self.token_candidates.items():
            # Filtrar transacciones recientes
            recent_txs = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
            
            if not recent_txs:
                tokens_to_remove.append(token)
            else:
                # Actualizar con solo transacciones recientes
                self.token_candidates[token]["transactions"] = recent_txs
                
                # Recalcular wallets activas basadas en transacciones recientes
                active_wallets = {tx["wallet"] for tx in recent_txs}
                self.token_candidates[token]["wallets"] = active_wallets
                
                # Recalcular volumen
                volume_usd = sum(tx["amount_usd"] for tx in recent_txs)
                self.token_candidates[token]["volume_usd"] = volume_usd
        
        # Eliminar tokens sin actividad reciente
        for token in tokens_to_remove:
            del self.token_candidates[token]
            
        if tokens_to_remove:
            logger.info(f"Limpiados {len(tokens_to_remove)} tokens sin actividad reciente")

    async def check_signals_periodically(self, interval=30):
        """
        Verifica y procesa candidatos para generar señales de forma periódica.
        """
        while True:
            try:
                await self._process_candidates()
            except Exception as e:
                logger.error(f"Error en check_signals_periodically: {e}")
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
                logger.error(f"Error procesando candidato {token}: {e}")

        # Ordenar candidatos por confianza (de mayor a menor)
        candidates.sort(key=lambda x: x["confidence"], reverse=True)
        await self._generate_signals(candidates)

    async def _generate_signals(self, candidates):
        """
        Genera señales basadas en los candidatos procesados.
        Aquí se puede integrar la lógica para almacenar señales y enviar notificaciones.
        """
        for candidate in candidates:
            logger.info(f"✅ Señal generada para {candidate['token']} con confianza {candidate['confidence']:.2f}")
            # Aquí se puede agregar la lógica para guardar la señal en la base de datos
            # y enviar alertas por Telegram, etc.
