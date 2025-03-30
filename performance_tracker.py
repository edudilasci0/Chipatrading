import asyncio
import time
import logging
from typing import Dict, Any
import db
from config import Config

logger = logging.getLogger("performance_tracker")

class PerformanceTracker:
    def __init__(self, dexscreener_client=None, dex_monitor=None, market_metrics=None):
        self.dexscreener_client = dexscreener_client
        self.dex_monitor = dex_monitor
        self.market_metrics = market_metrics
        self.signal_performance = {}
        self.last_prices = {}
        self.signal_updates = {}
        self.dead_signals = set()
        self.running = False
        self.shutdown_flag = False
        logger.info("PerformanceTracker inicializado con servicios avanzados")

    async def start(self):
        if self.running:
            return
        self.running = True
        # Iniciar tareas de seguimiento periódicas si es necesario
        logger.info("PerformanceTracker iniciado con detector de señales muertas")
        # Si se tienen tareas periódicas, se pueden iniciar aquí
        # Por ejemplo: asyncio.create_task(self._periodic_signal_update())

    def add_signal(self, token: str, signal_info: Dict[str, Any]):
        self.signal_performance[token] = signal_info
        self.last_prices[token] = signal_info.get("initial_price", 0)
        logger.info(f"Señal agregada para {token}")

    def get_signal_performance_summary(self, token: str) -> Dict[str, Any]:
        if token not in self.signal_performance:
            return {}
        data = self.signal_performance[token]
        initial_price = data.get("initial_price", 0)
        last_price = self.last_prices.get(token, 0)
        current_gain = ((last_price - initial_price) / initial_price * 100) if initial_price > 0 else 0
        return {
            "token": token,
            "initial_price": initial_price,
            "current_price": last_price,
            "current_gain": current_gain,
            "status": "active" if token not in self.dead_signals else "dead"
        }
