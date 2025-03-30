import asyncio
import time
import logging
from datetime import datetime, timedelta
import math
from collections import deque, defaultdict
import db
from config import Config
import telegram_utils

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
    
    def __init__(self, token_data_service=None, dex_monitor=None, market_metrics=None, whale_detector=None):
        self.token_data_service = token_data_service
        self.dex_monitor = dex_monitor
        self.market_metrics = market_metrics
        self.whale_detector = whale_detector
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
        self.dead_signal_task = asyncio.create_task(self._periodic_dead_signal_detection())
        logger.info("PerformanceTracker iniciado con detector de señales muertas")
    
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
        initial_price = self._get_token_price(token)
        if initial_price == 0 and signal_info.get("initial_price"):
            initial_price = signal_info.get("initial_price")
        performance_data = {
            "timestamp": timestamp,
            "initial_price": initial_price,
            "min_price": initial_price if initial_price > 0 else 0,
            "initial_time": timestamp,
            "performances": {},
            "max_price": initial_price if initial_price > 0 else 0,
            "max_gain": 0,
            "confidence": signal_info.get("confidence", 0),
            "traders_count": signal_info.get("traders_count", 0),
            "total_volume": signal_info.get("total_volume", 0),
            "signal_id": signal_info.get("signal_id", None),
            "token_name": signal_info.get("token_name", ""),
            "known_traders": signal_info.get("known_traders", []),
            "liquidity_initial": 0,
            "holder_count_initial": 0,
            "whale_activity": False,
            "liquidity_change": 0,
            "holder_growth": 0,
            "volume_change": 0,
            "last_update": timestamp,
            "is_dead": False,
            "death_reason": None
        }
        if self.dex_monitor:
            asyncio.create_task(self._get_initial_liquidity(token, performance_data))
        if self.market_metrics:
            asyncio.create_task(self._get_initial_holders(token, performance_data))
        self.signal_performance[token] = performance_data
        self.last_prices[token] = initial_price
        self.signal_updates[token] = timestamp
        self.early_stage_monitoring[token] = True
        asyncio.create_task(self._track_performance(token))
        logger.info(f"Iniciado seguimiento para token {token} con precio inicial ${initial_price}")
    
    async def _get_initial_liquidity(self, token, performance_data):
        try:
            if self.dex_monitor:
                liquidity_data = await self.dex_monitor.get_combined_liquidity_data(token)
                performance_data["liquidity_initial"] = liquidity_data.get("total_liquidity_usd", 0)
                logger.debug(f"Liquidez inicial para {token}: ${performance_data['liquidity_initial']}")
        except Exception as e:
            logger.error(f"Error obteniendo liquidez inicial para {token}: {e}")
    
    async def _get_initial_holders(self, token, performance_data):
        try:
            if self.market_metrics:
                holder_data = await self.market_metrics.get_holder_growth(token)
                performance_data["holder_count_initial"] = holder_data.get("holder_count", 0)
                logger.debug(f"Holders iniciales para {token}: {performance_data['holder_count_initial']}")
        except Exception as e:
            logger.error(f"Error obteniendo holders iniciales para {token}: {e}")
    
    async def _track_performance(self, token):
        if self.early_stage_monitoring.get(token, False):
            await self._early_stage_monitoring(token)
        for minutes, label in self.TRACK_INTERVALS:
            try:
                await asyncio.sleep(minutes * 60)
                if token not in self.signal_performance or self.shutdown_flag:
                    logger.warning(f"Token {token} ya no está en seguimiento, cancelando monitor")
                    break
                if self.signal_performance[token].get("is_dead", False):
                    logger.info(f"Señal para {token} marcada como muerta, deteniendo seguimiento")
                    break
                performance_data = await self._gather_performance_data(token)
                if performance_data["current_price"] < self.signal_performance[token]["min_price"]:
                    self.signal_performance[token]["min_price"] = performance_data["current_price"]
                initial_price = self.signal_performance[token]["initial_price"]
                percent_change = ((performance_data["current_price"] - initial_price) / initial_price) * 100 if initial_price > 0 else 0
                if performance_data["current_price"] > self.signal_performance[token]["max_price"]:
                    self.signal_performance[token]["max_price"] = performance_data["current_price"]
                    max_gain = ((performance_data["current_price"] - initial_price) / initial_price) * 100 if initial_price > 0 else 0
                    self.signal_performance[token]["max_gain"] = max_gain
                performance_entry = {
                    "price": performance_data["current_price"],
                    "percent_change": percent_change,
                    "timestamp": int(time.time()),
                    "liquidity": performance_data.get("liquidity", 0),
                    "volume": performance_data.get("volume", 0),
                    "holder_count": performance_data.get("holder_count", 0),
                    "whale_activity": performance_data.get("whale_activity", False)
                }
                self.signal_performance[token]["performances"][label] = performance_entry
                self.signal_performance[token]["last_update"] = time.time()
                volume = performance_data.get("volume", 0)
                if volume > 1000000:
                    volume_display = f"${volume/1000000:.2f}M"
                elif volume > 1000:
                    volume_display = f"${volume/1000:.2f}K"
                else:
                    volume_display = f"${volume:.2f}"
                telegram_utils.send_performance_report(
                    token=token,
                    signal_id=self.signal_performance[token].get("signal_id", ""),
                    timeframe=label,
                    percent_change=percent_change,
                    volatility=performance_data.get("volatility", 0),
                    trend=performance_data.get("trend", "Neutral"),
                    volume_display=volume_display,
                    traders_count=len(performance_data.get("active_traders", [])),
                    whale_activity=performance_data.get("whale_activity", False),
                    liquidity_change=performance_data.get("liquidity_change", 0)
                )
                self._save_performance_data(token, label, percent_change, performance_data)
                logger.info(f"Actualización para {token} ({label}): {percent_change:.2f}% | Liq: ${performance_data.get('liquidity', 0):.2f} | Vol: {volume_display}")
            except Exception as e:
                logger.error(f"🚨 Error en seguimiento de {token} a {label}: {e}")
    
    async def _early_stage_monitoring(self, token):
        early_intervals = [3, 8, 15, 25]
        for minutes in early_intervals:
            try:
                await asyncio.sleep(minutes * 60)
                if token not in self.signal_performance or self.shutdown_flag:
                    return
                if self.signal_performance[token].get("is_dead", False):
                    logger.info(f"Señal para {token} marcada como muerta durante monitoreo temprano")
                    return
                now = time.time()
                signal_time = self.signal_performance[token]["timestamp"]
                if now - signal_time > 3600:
                    self.early_stage_monitoring[token] = False
                    return
                performance_data = await self._gather_performance_data(token)
                self.last_prices[token] = performance_data["current_price"]
                self.signal_updates[token] = now
                initial_price = self.signal_performance[token]["initial_price"]
                if initial_price > 0:
                    percent_change = ((performance_data["current_price"] - initial_price) / initial_price) * 100
                    if abs(percent_change) > 10:
                        from telegram_utils import send_telegram_message
                        emoji = "🚀" if percent_change > 10 else "⚠️"
                        message = (
                            f"*{emoji} Actualización Rápida #{self.signal_performance[token].get('signal_id', '')}*\n\n"
                            f"Token: `{token}`\n"
                            f"Cambio: *{percent_change:.2f}%*\n"
                            f"Tiempo desde señal: {int((now - signal_time) / 60)} min\n\n"
                            f"[Ver en Birdeye](https://birdeye.so/token/{token}?chain=solana)"
                        )
                        send_telegram_message(message)
                logger.debug(f"Monitoreo temprano para {token}: {percent_change:.2f}% después de {int((now - signal_time) / 60)} min")
                if percent_change < -30:
                    self.mark_signal_as_dead(token, "Reversión rápida")
                    return
            except Exception as e:
                logger.error(f"Error en monitoreo temprano de {token}: {e}")
    
    async def _gather_performance_data(self, token):
        result = {
            "current_price": await self._async_get_token_price(token),
            "timestamp": int(time.time())
        }
        tasks = []
        if self.dex_monitor:
            tasks.append(self._get_liquidity_data(token, result))
        if self.market_metrics:
            tasks.append(self._get_market_data(token, result))
        if self.whale_detector:
            tasks.append(self._get_whale_activity(token, result))
        if hasattr(self, 'token_analyzer') and self.token_analyzer:
            tasks.append(self._get_technical_analysis(token, result))
        if tasks:
            await asyncio.gather(*tasks)
        if token in self.signal_performance:
            initial_liquidity = self.signal_performance[token].get("liquidity_initial", 0)
            current_liquidity = result.get("liquidity", 0)
            if initial_liquidity > 0 and current_liquidity > 0:
                result["liquidity_change"] = ((current_liquidity - initial_liquidity) / initial_liquidity) * 100
            initial_holders = self.signal_performance[token].get("holder_count_initial", 0)
            current_holders = result.get("holder_count", 0)
            if initial_holders > 0 and current_holders > 0:
                result["holder_growth"] = ((current_holders - initial_holders) / initial_holders) * 100
        return result
    
    async def _get_liquidity_data(self, token, result):
        try:
            liquidity_data = await self.dex_monitor.get_combined_liquidity_data(token)
            if liquidity_data:
                result["liquidity"] = liquidity_data.get("total_liquidity_usd", 0)
                result["volume"] = liquidity_data.get("volume_24h", 0)
                result["slippage_1k"] = liquidity_data.get("slippage_1k", 0)
                result["slippage_10k"] = liquidity_data.get("slippage_10k", 0)
        except Exception as e:
            logger.error(f"Error obteniendo datos de liquidez para {token}: {e}")
    
    async def _get_market_data(self, token, result):
        try:
            holder_data = await self.market_metrics.get_holder_growth(token)
            if holder_data:
                result["holder_count"] = holder_data.get("holder_count", 0)
                result["holder_growth_rate_1h"] = holder_data.get("growth_rate_1h", 0)
            trending_data = await self.market_metrics.check_trending_status(token)
            if trending_data:
                result["is_trending"] = trending_data.get("is_trending", False)
                result["trending_platforms"] = trending_data.get("trending_platforms", [])
        except Exception as e:
            logger.error(f"Error obteniendo datos de mercado para {token}: {e}")
    
    async def _get_whale_activity(self, token, result):
        try:
            from db import get_token_transactions
            recent_transactions = get_token_transactions(token, hours=1)
            whale_report = await self.whale_detector.detect_large_transactions(
                token, recent_transactions, result.get("market_cap", 0)
            )
            result["whale_activity"] = whale_report.get("has_whale_activity", False)
            result["whale_transactions"] = whale_report.get("whale_transactions", [])
            result["whale_impact_score"] = whale_report.get("impact_score", 0)
        except Exception as e:
            logger.error(f"Error obteniendo actividad de ballenas para {token}: {e}")
    
    async def _get_technical_analysis(self, token, result):
        try:
            if "current_price" in result:
                await self.token_analyzer.update_price_data(token, result["current_price"])
            volume_patterns = await self.token_analyzer.analyze_volume_patterns(token)
            if volume_patterns:
                result["volume_trend"] = volume_patterns.get("volume_trend", "neutral")
                result["volume_surge"] = volume_patterns.get("volume_surge", False)
            price_patterns = await self.token_analyzer.detect_price_patterns(token)
            if price_patterns:
                result["trend"] = price_patterns.get("trend", "neutral")
                result["volatility"] = price_patterns.get("volatility", 0)
                result["rsi"] = price_patterns.get("rsi", 50)
                result["support_levels"] = price_patterns.get("support_levels", [])
                result["resistance_levels"] = price_patterns.get("resistance_levels", [])
        except Exception as e:
            logger.error(f"Error obteniendo análisis técnico para {token}: {e}")
    
    async def _async_get_token_price(self, token):
        if self.token_data_service:
            try:
                if hasattr(self.token_data_service, 'get_token_price'):
                    price = await self.token_data_service.get_token_price(token)
                    if price and price > 0:
                        self.last_prices[token] = price
                        return price
                elif hasattr(self.token_data_service, 'get_token_data_async'):
                    token_data = await self.token_data_service.get_token_data_async(token)
                    if token_data and 'price' in token_data and token_data['price'] > 0:
                        price = token_data['price']
                        self.last_prices[token] = price
                        return price
            except Exception as e:
                logger.error(f"Error en token_data_service.get_token_price para {token}: {e}")
        if self.dex_monitor:
            try:
                liquidity_data = await self.dex_monitor.get_combined_liquidity_data(token)
                if liquidity_data and liquidity_data.get("price", 0) > 0:
                    price = liquidity_data["price"]
                    self.last_prices[token] = price
                    return price
            except Exception as e:
                logger.warning(f"Error obteniendo precio desde Dex Monitor para {token}: {e}")
        return self.last_prices.get(token, 0)
    
    def _get_token_price(self, token):
        try:
            if token in self.last_prices:
                return self.last_prices.get(token, 0)
            if self.token_data_service and hasattr(self.token_data_service, 'get_token_data'):
                token_data = self.token_data_service.get_token_data(token)
                if token_data and 'price' in token_data and token_data['price'] > 0:
                    self.last_prices[token] = token_data['price']
                    return token_data['price']
        except Exception as e:
            logger.error(f"Error obteniendo precio para {token}: {e}")
        return 0
    
    def _save_performance_data(self, token, timeframe, percent_change, performance_data):
        try:
            signal_data = self.signal_performance.get(token, {})
            db.save_signal_performance(
                token=token,
                signal_id=signal_data.get("signal_id"),
                timeframe=timeframe,
                percent_change=percent_change,
                confidence=signal_data.get('confidence', 0),
                traders_count=signal_data.get('traders_count', 0),
                extra_data={
                    "liquidity": performance_data.get("liquidity", 0),
                    "volume": performance_data.get("volume", 0),
                    "whale_activity": performance_data.get("whale_activity", False),
                    "holder_count": performance_data.get("holder_count", 0),
                    "is_trending": performance_data.get("is_trending", False)
                }
            )
        except Exception as e:
            logger.error(f"🚨 Error guardando datos para {token}: {e}")
    
    def mark_signal_as_dead(self, token, reason="No especificado"):
        if token not in self.signal_performance:
            return
        self.signal_performance[token]["is_dead"] = True
        self.signal_performance[token]["death_reason"] = reason
        self.signal_performance[token]["death_time"] = time.time()
        signal_id = self.signal_performance[token].get("signal_id", "")
        try:
            db.update_signal_status(signal_id, "dead", reason)
        except Exception as e:
            logger.error(f"Error actualizando estado de señal en BD: {e}")
        try:
            message = (
                f"⚰️ *Señal Finalizada* #{signal_id}\n\n"
                f"Token: `{token}`\n"
                f"Razón: {reason}\n"
                f"Rendimiento final: {self._get_final_performance(token)}\n"
                f"Máximo alcanzado: {self.signal_performance[token].get('max_gain', 0):.2f}%\n"
            )
            from telegram_utils import send_telegram_message
            send_telegram_message(message)
        except Exception as e:
            logger.error(f"Error enviando notificación de señal muerta: {e}")
        logger.info(f"Señal {signal_id} marcada como muerta. Razón: {reason}")
    
    def _get_final_performance(self, token):
        if token not in self.signal_performance:
            return "N/A"
        data = self.signal_performance[token]
        initial_price = data.get("initial_price", 0)
        last_price = self.last_prices.get(token, 0)
        if last_price == 0:
            performances = data.get("performances", {})
            if performances:
                for tf in ["24h", "4h", "2h", "1h", "30m", "10m", "5m", "3m"]:
                    if tf in performances:
                        last_price = performances[tf].get("price", 0)
                        if last_price > 0:
                            break
        if initial_price <= 0 or last_price <= 0:
            return "N/A"
        percent_change = ((last_price - initial_price) / initial_price) * 100
        if percent_change > 50:
            emoji = "🚀"
        elif percent_change > 20:
            emoji = "🔥"
        elif percent_change > 0:
            emoji = "✅"
        elif percent_change > -20:
            emoji = "⚠️"
        else:
            emoji = "❌"
        return f"{emoji} *{percent_change:.2f}%*"
    
    def get_signal_performance_summary(self, token):
        if not self.performance_tracker:
            return None
        return self.performance_tracker.get_signal_performance_summary(token)
    
    def get_active_signals_count(self):
        return sum(1 for token, data in self.signal_performance.items() if not data.get("is_dead", False))
