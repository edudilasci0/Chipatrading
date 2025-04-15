#!/usr/bin/env python3
# signal_logic.py - Lógica central para detección de señales en el bot de trading

import time
import asyncio
import logging
from typing import Dict, Any, Optional
from config import Config
import db

from market_metrics import MarketMetricsAnalyzer
from token_analyzer import TokenAnalyzer
from trader_profiler import TraderProfiler
from telegram_utils import send_enhanced_signal

logger = logging.getLogger("signal_logic")

class SignalLogic:
    def __init__(self, dexscreener_client):
        """
        Inicialización simplificada que solo requiere el cliente DexScreener
        """
        self.dexscreener_client = dexscreener_client
        self.token_candidates = {}
        self.recent_signals = []
        self.last_signal_check = time.time()
        self.watched_tokens = set()
        
        # Inicializar analizadores
        self.market_metrics = MarketMetricsAnalyzer(dexscreener_client=dexscreener_client)
        self.token_analyzer = TokenAnalyzer(dexscreener_client=dexscreener_client)
        self.trader_profiler = TraderProfiler()
        
        # Configuración de umbrales
        self.min_market_cap = float(Config.get_setting("mcap_threshold", "50000"))  # Reducido a $50K
        self.min_volume = float(Config.get_setting("volume_threshold", "100000"))   # Reducido a $100K
        self.min_transaction_usd = float(Config.MIN_TRANSACTION_USD)
        
        # Iniciar monitoreo periódico
        asyncio.create_task(self.periodic_monitoring())
        
    async def process_transaction(self, tx_data: Dict[str, Any]) -> None:
        """
        Procesa una transacción y genera señales si es necesario
        """
        try:
            if not self._validate_transaction(tx_data):
                return
                
            token = tx_data["token"]
            wallet = tx_data["wallet"]
            amount_usd = float(tx_data["amount_usd"])
            
            # Obtener datos de mercado
            market_data = await self.get_token_market_data(token)
            if not market_data:
                return
                
            # Verificar criterios básicos
            if not self._check_market_criteria(market_data):
                return
                
            # Generar señal
            await self._generate_signal(token, wallet, amount_usd, market_data)
            
        except Exception as e:
            logger.error(f"Error procesando transacción: {e}")
            
    def _validate_transaction(self, tx_data: Dict[str, Any]) -> bool:
        """
        Valida los datos básicos de la transacción
        """
        required_fields = ["token", "wallet", "amount_usd", "type"]
        if not all(field in tx_data for field in required_fields):
            return False
            
        if tx_data["token"] in ["native", "So11111111111111111111111111111111111111112"]:
            return False
            
        if float(tx_data["amount_usd"]) < self.min_transaction_usd:
            return False
            
        return True
        
    def _check_market_criteria(self, market_data: Dict[str, Any]) -> bool:
        """
        Verifica si el token cumple con los criterios de mercado
        """
        market_cap = float(market_data.get("marketCap", 0))
        volume_24h = float(market_data.get("volume24h", 0))
        
        return market_cap >= self.min_market_cap and volume_24h >= self.min_volume
        
    async def get_token_market_data(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene datos de mercado para un token
        """
        try:
            return await self.dexscreener_client.get_token_data(token)
        except Exception as e:
            logger.error(f"Error obteniendo datos de mercado para {token}: {e}")
            return None
            
    async def _generate_signal(self, token: str, wallet: str, amount_usd: float, market_data: Dict[str, Any]) -> None:
        """
        Genera una señal de trading
        """
        try:
            signal_data = {
                "token": token,
                "wallet": wallet,
                "amount_usd": amount_usd,
                "market_cap": market_data.get("marketCap"),
                "volume_24h": market_data.get("volume24h"),
                "price": market_data.get("price"),
                "timestamp": time.time()
            }
            
            # Guardar en base de datos
            db.save_signal(signal_data)
            
            # Enviar notificación
            await send_enhanced_signal(signal_data)
            
            logger.info(f"Señal generada para {token} por wallet {wallet}")
            
        except Exception as e:
            logger.error(f"Error generando señal: {e}")
            
    async def periodic_monitoring(self):
        """
        Monitoreo periódico de tokens
        """
        while True:
            try:
                for token in self.watched_tokens:
                    market_data = await self.get_token_market_data(token)
                    if market_data and self._check_market_criteria(market_data):
                        await self._generate_signal(token, "system", 0, market_data)
                        
                await asyncio.sleep(300)  # Revisar cada 5 minutos
                
            except Exception as e:
                logger.error(f"Error en monitoreo periódico: {e}")
                await asyncio.sleep(60)  # Esperar 1 minuto en caso de error
