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
from risk_manager import RiskManager

# Configurar logging más detallado
logger = logging.getLogger("signal_logic")
logger.setLevel(logging.DEBUG)

# Añadir handler para consola si no existe
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

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
        
        # Inicializar Risk Manager
        self.risk_manager = RiskManager()
        
        # Configuración de umbrales
        self.min_market_cap = float(Config.get("mcap_threshold", "50000"))  # Reducido a $50K
        self.min_volume = float(Config.get("volume_threshold", "100000"))   # Reducido a $100K
        self.min_transaction_usd = float(Config.MIN_TRANSACTION_USD)
        
        logger.info(f"SignalLogic inicializado con umbrales: Market Cap=${self.min_market_cap}, Volumen=${self.min_volume}, Min Trans=${self.min_transaction_usd}")
        
        # Iniciar monitoreo periódico
        asyncio.create_task(self.periodic_monitoring())
        
    async def process_transaction(self, tx_data: Dict[str, Any]) -> None:
        """
        Procesa una transacción y genera señales si es necesario
        """
        try:
            logger.debug(f"Procesando transacción: {tx_data}")
            
            if not self._validate_transaction(tx_data):
                logger.debug(f"Transacción no válida: {tx_data}")
                return
                
            token = tx_data["token"]
            wallet = tx_data["wallet"]
            amount_usd = float(tx_data["amount_usd"])
            
            logger.info(f"Transacción válida recibida: Token={token}, Wallet={wallet}, Amount=${amount_usd}")
            
            # Obtener datos de mercado
            market_data = await self.get_token_market_data(token)
            if not market_data:
                logger.warning(f"No se pudieron obtener datos de mercado para {token}")
                return
                
            logger.debug(f"Datos de mercado para {token}: {market_data}")
            
            # Verificar criterios básicos
            if not self._check_market_criteria(market_data):
                logger.info(f"Token {token} no cumple criterios de mercado: MC=${market_data.get('marketCap', 0)}, Vol=${market_data.get('volume24h', 0)}")
                return
                
            logger.info(f"Token {token} cumple criterios de mercado. Generando señal...")
            
            # Preparar datos de señal
            signal_data = {
                "token": token,
                "wallet": wallet,
                "amount_usd": amount_usd,
                "market_cap": market_data.get("marketCap"),
                "volume_24h": market_data.get("volume24h"),
                "price": market_data.get("price"),
                "timestamp": time.time()
            }
            
            # Calcular tamaño del trade
            trade_size = self.risk_manager.calculate_trade_size(token, signal_data)
            if not trade_size:
                logger.warning(f"No se pudo calcular tamaño de trade para {token}")
                return
                
            # Verificar si se puede abrir el trade
            if not self.risk_manager.can_open_trade(token, trade_size):
                logger.warning(f"No se puede abrir trade para {token} por restricciones de riesgo")
                return
                
            # Añadir tamaño del trade a la señal
            signal_data["trade_size"] = trade_size
            
            # Generar señal
            await self._generate_signal(signal_data)
            
        except Exception as e:
            logger.error(f"Error procesando transacción: {e}", exc_info=True)
            
    def _validate_transaction(self, tx_data: Dict[str, Any]) -> bool:
        """
        Valida los datos básicos de la transacción
        """
        required_fields = ["token", "wallet", "amount_usd", "type"]
        if not all(field in tx_data for field in required_fields):
            logger.debug(f"Transacción inválida: faltan campos requeridos. Campos presentes: {list(tx_data.keys())}")
            return False
            
        if tx_data["token"] in ["native", "So11111111111111111111111111111111111111112"]:
            logger.debug(f"Transacción inválida: token nativo {tx_data['token']}")
            return False
            
        amount = float(tx_data["amount_usd"])
        if amount < self.min_transaction_usd:
            logger.debug(f"Transacción inválida: monto ${amount} menor que mínimo ${self.min_transaction_usd}")
            return False
            
        logger.debug(f"Transacción válida: {tx_data}")
        return True
        
    def _check_market_criteria(self, market_data: Dict[str, Any]) -> bool:
        """
        Verifica si el token cumple con los criterios de mercado
        """
        market_cap = float(market_data.get("marketCap", 0))
        volume_24h = float(market_data.get("volume24h", 0))
        
        meets_mcap = market_cap >= self.min_market_cap
        meets_volume = volume_24h >= self.min_volume
        
        logger.debug(f"Criterios de mercado para token: MC=${market_cap} (min=${self.min_market_cap}), Vol=${volume_24h} (min=${self.min_volume})")
        logger.debug(f"Resultado: MC={'✅' if meets_mcap else '❌'}, Vol={'✅' if meets_volume else '❌'}")
        
        return meets_mcap and meets_volume
        
    async def get_token_market_data(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene datos de mercado para un token
        """
        try:
            logger.debug(f"Obteniendo datos de mercado para {token}")
            data = await self.dexscreener_client.fetch_token_data(token)
            if data:
                logger.debug(f"Datos obtenidos para {token}: {data}")
            else:
                logger.warning(f"No se obtuvieron datos para {token}")
            return data
        except Exception as e:
            logger.error(f"Error obteniendo datos de mercado para {token}: {e}", exc_info=True)
            return None
            
    async def _generate_signal(self, signal_data: Dict[str, Any]) -> None:
        """
        Genera una señal de trading
        """
        try:
            token = signal_data["token"]
            wallet = signal_data["wallet"]
            trade_size = signal_data["trade_size"]
            
            logger.info(f"Generando señal para {token} por wallet {wallet}")
            logger.debug(f"Datos de señal: {signal_data}")
            
            # Guardar en base de datos
            db.save_signal(
                token=signal_data["token"],
                trader_count=1,  # Por ahora asumimos un trader individual
                confidence=0.5,  # Valor por defecto de confianza
                initial_price=signal_data["price"],
                market_cap=signal_data.get("market_cap", 0),
                volume=signal_data.get("volume_24h", 0)
            )
            logger.info(f"Señal guardada en base de datos para {token}")
            
            # Registrar trade en Risk Manager
            self.risk_manager.register_trade(token, {
                "size": trade_size,
                "entry_price": signal_data["price"],
                "timestamp": signal_data["timestamp"]
            })
            
            # Enviar notificación
            await send_enhanced_signal(
                token=token,
                confidence=0.7,  # Valor por defecto de confianza
                tx_velocity=1.0,  # Valor por defecto
                traders=[wallet],  # Lista con el wallet que generó la señal
                market_cap=signal_data.get("market_cap"),
                initial_price=signal_data.get("price")
            )
            logger.info(f"Notificación enviada para {token}")
            
            logger.info(f"✅ Señal generada exitosamente para {token} por wallet {wallet}")
            
        except Exception as e:
            logger.error(f"Error generando señal: {e}", exc_info=True)
            
    async def periodic_monitoring(self):
        """
        Monitoreo periódico de tokens
        """
        logger.info("Iniciando monitoreo periódico de tokens")
        while True:
            try:
                logger.debug(f"Monitoreando {len(self.watched_tokens)} tokens")
                for token in self.watched_tokens:
                    logger.debug(f"Revisando token {token}")
                    market_data = await self.get_token_market_data(token)
                    if market_data and self._check_market_criteria(market_data):
                        logger.info(f"Token {token} cumple criterios en monitoreo periódico")
                        
                        # Preparar datos de señal
                        signal_data = {
                            "token": token,
                            "wallet": "system",
                            "amount_usd": 0,
                            "market_cap": market_data.get("marketCap"),
                            "volume_24h": market_data.get("volume24h"),
                            "price": market_data.get("price"),
                            "timestamp": time.time()
                        }
                        
                        # Calcular tamaño del trade
                        trade_size = self.risk_manager.calculate_trade_size(token, signal_data)
                        if trade_size and self.risk_manager.can_open_trade(token, trade_size):
                            signal_data["trade_size"] = trade_size
                            await self._generate_signal(signal_data)
                        else:
                            logger.debug(f"Token {token} no cumple criterios de riesgo")
                    else:
                        logger.debug(f"Token {token} no cumple criterios en monitoreo periódico")
                        
                logger.debug("Esperando 5 minutos para próximo ciclo de monitoreo")
                await asyncio.sleep(300)  # Revisar cada 5 minutos
                
            except Exception as e:
                logger.error(f"Error en monitoreo periódico: {e}", exc_info=True)
                await asyncio.sleep(60)  # Esperar 1 minuto en caso de error
