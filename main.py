import os
import sys
import time
import signal
import asyncio
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("chipatrading")

from config import Config
from wallet_tracker import WalletTracker
from cielo_api import CieloAPI
from scoring import ScoringSystem
from signal_logic import SignalLogic, add_healthcheck
from performance_tracker import PerformanceTracker
from telegram_utils import send_telegram_message, fix_telegram_commands, fix_on_cielo_message
from scalper_monitor import ScalperActivityMonitor
import db

bot_running = True

async def cleanup_discoveries_periodically(scalper_monitor, interval=3600):
    """Limpia periódicamente descubrimientos antiguos (si fuera necesario)"""
    while True:
        try:
            # Aquí podrías agregar alguna limpieza específica si es necesario
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Error en cleanup_discoveries_periodically: {e}", exc_info=True)
            await asyncio.sleep(60)

# Función para optimizar el cálculo de confianza
def optimize_signal_confidence():
    def compute_optimized_confidence(self, wallet_scores, volume_1h, market_cap, 
                                     recent_volume_growth=0, token_type=None, 
                                     whale_activity=False, tx_velocity=0):
        if not wallet_scores:
            return 0.0
        exp_scores = [min(score ** 1.5, 12.0) for score in wallet_scores]
        weighted_avg = sum(exp_scores) / (len(exp_scores) * (Config.MAX_SCORE ** 1.5)) * Config.MAX_SCORE
        score_factor = weighted_avg / Config.MAX_SCORE

        unique_wallets = len(wallet_scores)
        wallet_diversity = min(unique_wallets / 10.0, 1.0)
        high_quality_traders = sum(1 for score in wallet_scores if score > 8.0)
        elite_traders = sum(1 for score in wallet_scores if score > 9.0)
        quality_ratio = (high_quality_traders + (elite_traders * 2)) / max(1, len(wallet_scores))
        quality_factor = min(quality_ratio * 1.5, 1.0)
        elite_bonus = min(elite_traders * 0.1, 0.3)
        
        tx_velocity_normalized = min(tx_velocity / 20.0, 1.0)
        pump_dump_risk = 0
        if tx_velocity > 15 and elite_traders == 0 and high_quality_traders / max(1, len(wallet_scores)) < 0.2:
            pump_dump_risk = 0.3
            
        wallet_factor = (score_factor * 0.4) + (wallet_diversity * 0.3) + (quality_factor * 0.2) + elite_bonus - pump_dump_risk

        if token_type == "meme":
            growth_factor = min(recent_volume_growth * 3.0, 1.0)
            market_cap_threshold = 10_000_000
        else:
            growth_factor = min(recent_volume_growth * 1.5, 1.0)
            market_cap_threshold = 5_000_000
            
        market_factor = 0.8
        if market_cap > 0:
            if market_cap < market_cap_threshold:
                market_factor = 0.9
            elif market_cap > 100_000_000:
                market_factor = 0.6
                
        tx_velocity_factor = 0
        if token_type == "meme" and tx_velocity > 5:
            tx_velocity_factor = min(0.2, tx_velocity / 25.0)
        elif tx_velocity > 10:
            tx_velocity_factor = min(0.15, tx_velocity / 30.0)

        weighted_score = (wallet_factor * 0.65) + (market_factor * 0.35) + tx_velocity_factor
        
        if whale_activity:
            weighted_score *= 1.1
            
        if token_type and token_type.lower() in self.token_type_scores:
            multiplier = self.token_type_scores[token_type.lower()]
            weighted_score *= multiplier

        import math
        def sigmoid_normalize(x, center=0.5, steepness=8):
            return 1 / (1 + math.exp(-steepness * (x - center)))
            
        normalized = max(0.1, min(1.0, sigmoid_normalize(weighted_score, 0.5, 8)))
        return round(normalized, 3)
    return compute_optimized_confidence

# Función para detectar tokens emergentes (alpha)
def enhance_alpha_detection():
    async def detect_emerging_alpha_tokens(self):
        try:
            now = time.time()
            cutoff = now - 3600  # Sólo tokens activos en la última hora
            alpha_candidates = []
            for token, data in self.token_candidates.items():
                if data["first_seen"] < cutoff:
                    continue
                if len(data["wallets"]) < 2:
                    continue
                has_elite_trader = False
                trader_scores = []
                for wallet in data["wallets"]:
                    score = self.scoring_system.get_score(wallet)
                    trader_scores.append(score)
                    if score > 9.0:
                        has_elite_trader = True
                if not has_elite_trader and len(data["wallets"]) < 3:
                    continue
                market_data = await self.get_token_market_data(token)
                if market_data.get("market_cap", 0) > 20_000_000:
                    continue
                volume_1h = market_data.get("volume", 0)
                if volume_1h < 1000:
                    continue
                avg_score = sum(trader_scores) / len(trader_scores) if trader_scores else 0
                tx_count = len(data["transactions"])
                alpha_score = ((avg_score / 10.0) * 0.4 +
                               (min(len(data["wallets"]) / 5.0, 1.0) * 0.2) +
                               (min(volume_1h / 5000.0, 1.0) * 0.2) +
                               (0.2 if has_elite_trader else 0))
                alpha_candidates.append({
                    "token": token,
                    "alpha_score": alpha_score,
                    "traders_count": len(data["wallets"]),
                    "elite_traders": has_elite_trader,
                    "first_seen": data["first_seen"],
                    "volume_1h": volume_1h,
                    "market_cap": market_data.get("market_cap", 0)
                })
            alpha_candidates.sort(key=lambda x: x["alpha_score"], reverse=True)
            return [c for c in alpha_candidates if c["alpha_score"] > 0.7]
        except Exception as e:
            logger.error(f"Error detectando tokens alfa: {e}", exc_info=True)
            return []
    return detect_emerging_alpha_tokens

async def main():
    global bot_running
    try:
        print("\n==== INICIANDO TRADING BOT ====")
        print(f"Fecha/hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        Config.check_required_config()
        db.init_db()
        
        # Cargar wallets
        wallet_tracker = WalletTracker()
        wallets = wallet_tracker.get_wallets()
        print(f"✅ Cargadas {len(wallets)} wallets para monitoreo")
        logger.info(f"Wallets cargadas: {wallets}")
        
        scoring_system = ScoringSystem()
        
        helius_client = None
        if Config.HELIUS_API_KEY:
            from helius_client import HeliusClient
            helius_client = HeliusClient(Config.HELIUS_API_KEY)
            logger.info("✅ Cliente Helius inicializado")
        
        gmgn_client = None
        try:
            from gmgn_client import GMGNClient
            gmgn_client = GMGNClient()
            logger.info("✅ Cliente GMGN inicializado")
        except Exception as e:
            logger.warning(f"No se pudo inicializar cliente GMGN: {e}")
        
        signal_logic = SignalLogic(
            scoring_system=scoring_system,
            helius_client=helius_client,
            gmgn_client=gmgn_client
        )
        # Agregar referencia a wallet_tracker
        signal_logic.wallet_tracker = wallet_tracker
        
        # Aplicar optimizaciones
        signal_logic.compute_confidence = optimize_signal_confidence().__get__(signal_logic, SignalLogic)
        signal_logic.detect_emerging_alpha_tokens = enhance_alpha_detection().__get__(signal_logic, SignalLogic)
        if not hasattr(signal_logic, 'get_active_candidates_count'):
            signal_logic.get_active_candidates_count = lambda: len(signal_logic.token_candidates)
        
        performance_tracker = PerformanceTracker(token_data_service=helius_client)
        signal_logic.performance_tracker = performance_tracker
        
        scalper_monitor = ScalperActivityMonitor()
        
        # Iniciar healthcheck
        health_check = add_healthcheck()(signal_logic, scalper_monitor, db)
        
        # Iniciar bot de Telegram
        telegram_commands = fix_telegram_commands()
        is_bot_active = await telegram_commands(Config.TELEGRAM_BOT_TOKEN, Config.TELEGRAM_CHAT_ID, signal_logic)
        send_telegram_message("🚀 <b>Trading Bot Iniciado</b><br>Monitoreando transacciones en Solana...")
        
        # Crear tareas asíncronas
        tasks = [
            asyncio.create_task(signal_logic.check_signals_periodically()),
            asyncio.create_task(cleanup_discoveries_periodically(scalper_monitor)),
            asyncio.create_task(health_check.check_health_periodically())
        ]
        
        cielo_client = CieloAPI(Config.CIELO_API_KEY)
        cielo_message_handler = fix_on_cielo_message()
        # Preparar callback para Cielo
        def on_message_callback(message):
            return cielo_message_handler(message, wallet_tracker, scoring_system, signal_logic, scalper_monitor)
        
        cielo_task = asyncio.create_task(
            cielo_client.run_forever_wallets(
                wallets,
                on_message_callback,
                {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
            )
        )
        tasks.append(cielo_task)
        
        logger.info(f"✅ Bot iniciado y funcionando con {len(tasks)} tareas")
        
        # Supervisión de tareas
        while bot_running:
            for i, task in enumerate(tasks):
                if task.done():
                    try:
                        err = task.exception()
                        if err:
                            logger.error(f"Tarea #{i} falló: {err}")
                            if i == 0:
                                tasks[i] = asyncio.create_task(signal_logic.check_signals_periodically())
                                logger.info("Tarea de verificación de señales reiniciada")
                            elif i == 1:
                                tasks[i] = asyncio.create_task(cleanup_discoveries_periodically(scalper_monitor))
                                logger.info("Tarea de limpieza reiniciada")
                            elif i == 2:
                                tasks[i] = asyncio.create_task(health_check.check_health_periodically())
                                logger.info("Tarea de healthcheck reiniciada")
                            elif i == 3:
                                tasks[i] = asyncio.create_task(
                                    cielo_client.run_forever_wallets(
                                        wallets,
                                        on_message_callback,
                                        {"chains": ["solana"], "tx_types": ["swap", "transfer"]}
                                    )
                                )
                                logger.info("Tarea de WebSocket Cielo reiniciada")
                    except Exception as e:
                        logger.error(f"Error verificando tarea #{i}: {e}", exc_info=True)
            active_tokens = len(signal_logic.token_candidates)
            signals_today = db.count_signals_today()
            logger.info(f"Estado del bot: {active_tokens} tokens monitoreados, {signals_today} señales hoy")
            await asyncio.sleep(30)
            
    except Exception as e:
        logger.error(f"Error crítico en main: {e}", exc_info=True)
        send_telegram_message(f"⚠️ <b>Error Crítico</b>: El bot se ha detenido: {e}")
        sys.exit(1)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido por el usuario")
    finally:
        loop.close()
