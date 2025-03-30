import asyncio
import time
import logging
from datetime import datetime
import db
from config import Config
from telegram_utils import send_performance_report

logger = logging.getLogger("performance_tracker")

class PerformanceTracker:
    TRACK_INTERVALS = [
        (3, "3m"),
        (5, "5m"),
        (10, "10m"),
        (30, "30m"),
        (60, "1h"),
        (120, "2h"),
        (240, "4h"),
        (1440, "24h")
    ]
    
    def __init__(self, dexscreener_client=None, dex_monitor=None, market_metrics=None):
        self.dexscreener_client = dexscreener_client
        self.dex_monitor = dex_monitor
        self.market_metrics = market_metrics
        self.signal_performance = {}
        self.last_prices = {}
        self.signal_updates = {}
        self.early_stage_monitoring = {}
        self.dead_signals = set()
        self.dead_signal_task = None
        self.running = False
        self.shutdown_flag = False
        logger.info("PerformanceTracker inicializado con servicios avanzados")
    
    async def start(self):
        if self.running:
            return
        self.running = True
        self.shutdown_flag = False
        # Para simplificar, se omite la detección de señales muertas si ya no se utiliza whale_detector
        # self.dead_signal_task = asyncio.create_task(self._periodic_dead_signal_detection())
        logger.info("PerformanceTracker iniciado")
    
    async def stop(self):
        self.shutdown_flag = True
        if self.dead_signal_task:
            self.dead_signal_task.cancel()
            try:
                await self.dead_signal_task
            except asyncio.CancelledError:
                pass
        self.running = False
        logger.info("PerformanceTracker detenido")
    
    def add_signal(self, token, signal_info):
        timestamp = int(time.time())
        performance_data = {
            "timestamp": timestamp,
            "initial_price": signal_info.get("initial_price", 0),
            "performances": {},
            "max_price": signal_info.get("initial_price", 0),
            "max_gain": 0,
            "confidence": signal_info.get("confidence", 0),
            "traders_count": signal_info.get("traders_count", 0),
            "total_volume": signal_info.get("total_volume", 0),
            "signal_id": signal_info.get("signal_id"),
            "token_name": signal_info.get("token_name", ""),
            "known_traders": signal_info.get("known_traders", []),
            "last_update": timestamp,
            "is_dead": False,
            "death_reason": None
        }
        self.signal_performance[token] = performance_data
        self.last_prices[token] = signal_info.get("initial_price", 0)
        self.signal_updates[token] = timestamp
        self.early_stage_monitoring[token] = True
        asyncio.create_task(self._track_performance(token))
        logger.info(f"Iniciado seguimiento para {token} con precio inicial ${signal_info.get('initial_price', 0)}")
    
    async def _track_performance(self, token):
        # Implementación simplificada: Simula seguimiento de rendimiento en intervalos
        for minutes, label in self.TRACK_INTERVALS:
            try:
                await asyncio.sleep(minutes * 60)
                if token not in self.signal_performance or self.shutdown_flag:
                    break
                performance_entry = {
                    "price": self.last_prices.get(token, 0),
                    "percent_change": 0,  # Calcular según evolución
                    "timestamp": int(time.time())
                }
                self.signal_performance[token]["performances"][label] = performance_entry
                self.signal_performance[token]["last_update"] = time.time()
                logger.info(f"Actualización para {token} ({label})")
            except Exception as e:
                logger.error(f"Error en seguimiento de {token} a {label}: {e}")
    
    def get_signal_performance_summary(self, token):
        if token not in self.signal_performance:
            return None
        data = self.signal_performance[token]
        initial_price = data.get("initial_price", 0)
        last_price = self.last_prices.get(token, 0)
        current_gain = ((last_price - initial_price) / initial_price) * 100 if initial_price > 0 and last_price > 0 else 0
        return {
            "token": token,
            "signal_id": data.get("signal_id", ""),
            "initial_price": initial_price,
            "current_price": last_price,
            "current_gain": current_gain,
            "max_gain": data.get("max_gain", 0),
            "is_dead": data.get("is_dead", False),
            "death_reason": data.get("death_reason"),
            "elapsed_time": int(time.time() - data.get("timestamp", 0))
        }
