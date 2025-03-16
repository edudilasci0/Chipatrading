# scalper_monitor.py
import time
import asyncio
import logging
from config import Config

logger = logging.getLogger("scalper_monitor")

class ScalperActivityMonitor:
    """
    Clase para seguir y analizar la actividad de traders scalpers.
    """
    
    def __init__(self):
        self.scalper_tokens = {}
        self.known_scalpers = Config.get("KNOWN_SCALPERS", "").split(",")
        self.data_retention = int(Config.get("SCALPER_DATA_RETENTION_SECONDS", 86400))
        self.cleanup_interval = int(Config.get("SCALPER_CLEANUP_INTERVAL_SECONDS", 3600))
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
        logger.info("ScalperActivityMonitor inicializado.")
    
    def process_transaction(self, tx_data):
        try:
            wallet = tx_data.get("wallet")
            token = tx_data.get("token")
            amount = float(tx_data.get("amount_usd", 0))
            timestamp = tx_data.get("timestamp", time.time())
            
            if wallet not in self.known_scalpers:
                return
            
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
    
    def stop_cleanup(self):
        """
        Detiene la tarea de limpieza periódica.
        """
        if self._cleanup_task:
            self._cleanup_task.cancel()
