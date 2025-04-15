#!/usr/bin/env python3
# risk_manager.py - Gestión de riesgo para el bot de trading

import logging
from typing import Dict, Any, Optional
from config import Config

logger = logging.getLogger("risk_manager")

class RiskManager:
    def __init__(self):
        """
        Inicializa el gestor de riesgo con configuraciones básicas
        """
        # Configuración de riesgo
        self.max_portfolio_risk = float(Config.get_setting("max_portfolio_risk", "0.10"))  # 10% máximo del portfolio
        self.max_trade_risk = float(Config.get_setting("max_trade_risk", "0.02"))  # 2% máximo por trade
        self.min_trade_size = float(Config.get_setting("min_trade_size", "100"))  # $100 mínimo por trade
        self.max_trade_size = float(Config.get_setting("max_trade_size", "1000"))  # $1000 máximo por trade
        
        # Estado actual
        self.active_trades = {}  # token -> trade_info
        self.total_risk = 0.0
        self.portfolio_value = 0.0
        
        logger.info(f"RiskManager inicializado: Max Portfolio Risk={self.max_portfolio_risk*100}%, Max Trade Risk={self.max_trade_risk*100}%")
        
    def calculate_trade_size(self, token: str, signal_data: Dict[str, Any]) -> Optional[float]:
        """
        Calcula el tamaño óptimo del trade basado en el riesgo
        """
        try:
            # Obtener datos del token
            market_cap = float(signal_data.get("market_cap", 0))
            volume_24h = float(signal_data.get("volume_24h", 0))
            
            if market_cap == 0 or volume_24h == 0:
                logger.warning(f"No se pueden calcular métricas para {token}: MC={market_cap}, Vol={volume_24h}")
                return None
                
            # Calcular riesgo base (asumiendo pérdida total)
            base_risk = self.portfolio_value * self.max_trade_risk
            
            # Ajustar por liquidez (no más del 1% del volumen 24h)
            max_by_volume = volume_24h * 0.01
            
            # Ajustar por capitalización (no más del 0.1% del market cap)
            max_by_mcap = market_cap * 0.001
            
            # Tomar el mínimo de los límites
            trade_size = min(base_risk, max_by_volume, max_by_mcap)
            
            # Aplicar límites mínimos y máximos
            trade_size = max(self.min_trade_size, min(trade_size, self.max_trade_size))
            
            logger.info(f"Cálculo de tamaño para {token}:")
            logger.info(f"  - Riesgo base: ${base_risk:.2f}")
            logger.info(f"  - Límite por volumen: ${max_by_volume:.2f}")
            logger.info(f"  - Límite por market cap: ${max_by_mcap:.2f}")
            logger.info(f"  - Tamaño final: ${trade_size:.2f}")
            
            return trade_size
            
        except Exception as e:
            logger.error(f"Error calculando tamaño de trade para {token}: {e}", exc_info=True)
            return None
            
    def can_open_trade(self, token: str, trade_size: float) -> bool:
        """
        Verifica si se puede abrir un nuevo trade
        """
        try:
            # Verificar riesgo total
            new_total_risk = self.total_risk + (trade_size / self.portfolio_value)
            if new_total_risk > self.max_portfolio_risk:
                logger.warning(f"No se puede abrir trade para {token}: excede riesgo máximo del portfolio")
                return False
                
            # Verificar número de trades activos
            if len(self.active_trades) >= 5:  # Máximo 5 trades simultáneos
                logger.warning(f"No se puede abrir trade para {token}: máximo de trades activos alcanzado")
                return False
                
            logger.info(f"Trade aprobado para {token}: tamaño=${trade_size:.2f}, riesgo total={new_total_risk*100:.1f}%")
            return True
            
        except Exception as e:
            logger.error(f"Error verificando trade para {token}: {e}", exc_info=True)
            return False
            
    def register_trade(self, token: str, trade_info: Dict[str, Any]) -> None:
        """
        Registra un nuevo trade
        """
        try:
            self.active_trades[token] = trade_info
            self.total_risk += trade_info["size"] / self.portfolio_value
            logger.info(f"Trade registrado para {token}: ${trade_info['size']:.2f}")
            
        except Exception as e:
            logger.error(f"Error registrando trade para {token}: {e}", exc_info=True)
            
    def close_trade(self, token: str, pnl: float) -> None:
        """
        Cierra un trade y actualiza métricas
        """
        try:
            if token in self.active_trades:
                trade_info = self.active_trades.pop(token)
                self.total_risk -= trade_info["size"] / self.portfolio_value
                logger.info(f"Trade cerrado para {token}: PnL=${pnl:.2f}")
                
        except Exception as e:
            logger.error(f"Error cerrando trade para {token}: {e}", exc_info=True)
            
    def update_portfolio_value(self, new_value: float) -> None:
        """
        Actualiza el valor del portfolio
        """
        self.portfolio_value = new_value
        logger.info(f"Valor del portfolio actualizado: ${new_value:.2f}")
        
    def get_stats(self) -> Dict[str, Any]:
        """
        Retorna estadísticas actuales
        """
        return {
            "portfolio_value": self.portfolio_value,
            "total_risk": self.total_risk,
            "active_trades": len(self.active_trades),
            "max_portfolio_risk": self.max_portfolio_risk,
            "max_trade_risk": self.max_trade_risk
        } 