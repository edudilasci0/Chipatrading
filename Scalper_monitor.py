import time
import asyncio
import logging
from config import Config

logger = logging.getLogger("scalper_monitor")

class ScalperActivityMonitor:
    """
    Clase para seguir y analizar la actividad de traders scalpers.
    
    Funcionalidades:
      1. Mantener registro de tokens descubiertos por scalpers conocidos.
      2. Clasificar descubrimientos por nivel de confianza.
      3. Procesar transacciones recibidas.
      4. Proveer el método get_emerging_tokens() para tokens en fase alfa temprana.
      5. Limpiar periódicamente los datos antiguos.
      6. Compatible con la arquitectura actual.
    """
    
    def __init__(self):
        # Diccionario: {token: {confidence, transactions, first_seen, last_update}}
        self.scalper_tokens = {}
        # Lista de wallets de scalpers conocidos (configurable en Config)
        self.known_scalpers = Config.get("KNOWN_SCALPERS", "").split(",")
        # Tiempo (en segundos) para retener datos (por defecto 24h)
        self.data_retention = int(Config.get("SCALPER_DATA_RETENTION_SECONDS", 86400))
        # Intervalo de limpieza (por defecto cada 1 hora)
        self.cleanup_interval = int(Config.get("SCALPER_CLEANUP_INTERVAL_SECONDS", 3600))
        asyncio.create_task(self._periodic_cleanup())
        logger.info("ScalperActivityMonitor inicializado.")
    
    def process_transaction(self, tx_data):
        """
        Procesa una transacción para actualizar el registro de actividad de scalpers.
        Solo se consideran transacciones de wallets en known_scalpers.
        """
        try:
            wallet = tx_data.get("wallet")
            token = tx_data.get("token")
            amount = float(tx_data.get("amount_usd", 0))
            timestamp = tx_data.get("timestamp", time.time())
            
            if wallet not in self.known_scalpers:
                return
            
            # Calcular un nivel de confianza basado en el monto (ejemplo simple)
            confidence = min(amount / 1000, 1.0)
            
            if token in self.scalper_tokens:
                record = self.scalper_tokens[token]
                record["confidence"] = max(record["confidence"], confidence)
                record["transactions"].append(tx_data)
                record["last_update"] = timestamp
            else:
                self.scalper_tokens[token] = {
                    "confidence": confidence,
                    "transactions": [tx_data],
                    "first_seen": timestamp,
                    "last_update": timestamp
                }
            logger.debug(f"ScalperActivityMonitor: token {token} actualizado (confianza {confidence:.2f}).")
        except Exception as e:
            logger.error(f"Error en ScalperActivityMonitor.process_transaction: {e}", exc_info=True)
    
    def get_emerging_tokens(self, confidence_threshold=0.7):
        """
        Retorna una lista de tokens emergentes (fase alfa temprana) cuyo nivel de confianza
        sea mayor o igual al umbral indicado.
        """
        emerging = []
        for token, data in self.scalper_tokens.items():
            if data["confidence"] >= confidence_threshold:
                emerging.append({
                    "token": token,
                    "confidence": data["confidence"],
                    "transactions": len(data["transactions"]),
                    "first_seen": data["first_seen"]
                })
        emerging.sort(key=lambda x: x["confidence"], reverse=True)
        return emerging
    
    async def _periodic_cleanup(self):
        """
        Limpia periódicamente los registros que exceden el tiempo de retención.
        """
        while True:
            try:
                now = time.time()
                tokens_to_remove = [token for token, data in self.scalper_tokens.items() 
                                    if now - data["last_update"] > self.data_retention]
                for token in tokens_to_remove:
                    del self.scalper_tokens[token]
                    logger.info(f"ScalperActivityMonitor: token {token} eliminado por inactividad.")
            except Exception as e:
                logger.error(f"Error en ScalperActivityMonitor._periodic_cleanup: {e}", exc_info=True)
            await asyncio.sleep(self.cleanup_interval)
