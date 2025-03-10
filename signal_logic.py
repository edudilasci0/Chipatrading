import time
import asyncio
from config import Config
import db
from telegram_utils import send_telegram_message

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, rugcheck_api=None, ml_predictor=None):
        """
        Inicializa la lógica de señales usando exclusivamente Helius.
        """
        self.scoring_system = scoring_system
        self.helius_client = helius_client  # Usamos Helius para obtener datos on-chain
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.performance_tracker = None
        self.token_candidates = {}
        self.recent_signals = []
        self.last_signal_check = time.time()

    def add_transaction(self, wallet, token, amount_usd, tx_type):
        now = int(time.time())
        if token in Config.IGNORE_TOKENS:
            print(f"⚠️ Token {token} en lista de ignorados, no se procesará")
            return
        if token not in self.token_candidates:
            self.token_candidates[token] = {
                "wallets": set(),
                "transactions": [],
                "last_update": now,
                "volume_usd": 0
            }
        self.token_candidates[token]["wallets"].add(wallet)
        self.token_candidates[token]["transactions"].append({
            "wallet": wallet,
            "amount_usd": amount_usd,
            "type": tx_type,
            "timestamp": now
        })
        self.token_candidates[token]["volume_usd"] += amount_usd
        self.token_candidates[token]["last_update"] = now
        print(f"Procesando transacción para token {token}: {tx_type} ${amount_usd} por {wallet}")

    async def check_signals_periodically(self, interval=30):
        while True:
            try:
                await self._process_candidates()
                await asyncio.sleep(interval)
            except Exception as e:
                print(f"Error en check_signals_periodically: {e}")
                await asyncio.sleep(interval)

    def get_active_candidates_count(self):
        now = time.time()
        window_seconds = float(Config.get("signal_window_seconds", Config.SIGNAL_WINDOW_SECONDS))
        cutoff = now - window_seconds
        return sum(1 for data in self.token_candidates.values() if data["last_update"] > cutoff)

    def get_recent_signals(self, hours=24):
        cutoff = time.time() - (hours * 3600)
        return [(token, ts, conf, sig_id) for token, ts, conf, sig_id in self.recent_signals if ts > cutoff]

    async def _process_candidates(self):
        now = time.time()
        window_seconds = float(Config.get("signal_window_seconds", Config.SIGNAL_WINDOW_SECONDS))
        cutoff = now - window_seconds
        candidates = []
        processed = 0
        skipped = 0

        for token, data in list(self.token_candidates.items()):
            try:
                recent_txs = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
                if not recent_txs:
                    skipped += 1
                    continue
                trader_count = len(data["wallets"])
                volume_usd = sum(tx["amount_usd"] for tx in recent_txs)
                buy_txs = [tx for tx in recent_txs if tx["type"] == "BUY"]
                buy_percentage = len(buy_txs) / max(1, len(recent_txs))
                min_traders = int(Config.get("min_traders_for_signal", Config.MIN_TRADERS_FOR_SIGNAL))
                min_volume = float(Config.get("min_volume_usd", Config.MIN_VOLUME_USD))
                if trader_count < min_traders or volume_usd < min_volume:
                    skipped += 1
                    continue
                tx_rate = len(recent_txs) / window_seconds
                try:
                    helius_data = await asyncio.to_thread(self.helius_client.get_token_transactions, token, "5m")
                    one_min_data = await asyncio.to_thread(self.helius_client.get_token_transactions, token, "1m")
                    volume = helius_data.get("volume", 0) if helius_data else 0
                    market_cap = helius_data.get("market_cap", 0) if helius_data else 0
                    price = helius_data.get("price", 0) if helius_data else 0
                    vol_growth = helius_data.get("volume_growth", {}) if helius_data else {}
                    volume_1m = one_min_data.get("volume", 0) if one_min_data else 0
                except Exception as e:
                    print(f"Error obteniendo datos Helius para {token}: {e}")
                    volume = 0
                    market_cap = 0
                    price = 0
                    vol_growth = {}
                    volume_1m = 0
                token_type = None
                if market_cap < 5_000_000 and vol_growth.get("growth_5m", 0) > 0.3:
                    token_type = "meme"
                elif market_cap < 1_000_000 and tx_rate > 5:
                    token_type = "new"
                trader_scores = [self.scoring_system.get_score(w) for w in data["wallets"]]
                confidence = self.scoring_system.compute_confidence(
                    wallet_scores=trader_scores,
                    volume_1h=volume,
                    market_cap=market_cap,
                    recent_volume_growth=vol_growth.get("growth_5m", 0),
                    token_type=token_type
                )
                memecoin_config = Config.MEMECOIN_CONFIG
                is_memecoin = (
                    tx_rate > memecoin_config["TX_RATE_THRESHOLD"] and
                    vol_growth.get("growth_5m", 0) > memecoin_config["VOLUME_GROWTH_THRESHOLD"] and
                    market_cap < 10_000_000
                )
                has_whale_activity = any(tx["amount_usd"] > 10000 for tx in recent_txs)
                if is_memecoin:
                    confidence *= 1.5
                    print(f"🔥 Boost aplicado a {token} por características de memecoin")
                elif has_whale_activity:
                    confidence *= 1.2
                    print(f"🐋 Boost aplicado a {token} por actividad de ballenas")
                candidates.append({
                    "token": token,
                    "confidence": confidence,
                    "ml_prediction": 0.5,
                    "trader_count": trader_count,
                    "volume_usd": volume_usd,
                    "recent_transactions": recent_txs,
                    "market_cap": market_cap,
                    "volume_1h": volume,
                    "volume_growth": vol_growth,
                    "buy_percentage": buy_percentage,
                    "trader_scores": trader_scores,
                    "initial_price": price,
                    "token_type": token_type,
                    "tx_rate": tx_rate,
                    "has_whale_activity": has_whale_activity,
                    "is_memecoin": is_memecoin
                })
                processed += 1
            except Exception as e:
                print(f"Error procesando candidato {token}: {e}")
        if self.ml_predictor and self.ml_predictor.model:
            for candidate in candidates:
                try:
                    ml_features = {
                        "num_traders": candidate["trader_count"],
                        "num_transactions": len(candidate["recent_transactions"]),
                        "total_volume_usd": candidate["volume_usd"],
                        "avg_volume_per_trader": candidate["volume_usd"] / max(1, candidate["trader_count"]),
                        "buy_ratio": candidate["buy_percentage"],
                        "tx_velocity": candidate["tx_rate"],
                        "avg_trader_score": sum(candidate["trader_scores"]) / max(1, len(candidate["trader_scores"])),
                        "max_trader_score": max(candidate["trader_scores"]) if candidate["trader_scores"] else 0,
                        "market_cap": candidate["market_cap"],
                        "volume_1h": candidate["volume_1h"],
                        "volume_growth_5m": candidate.get("volume_growth", {}).get("growth_5m", 0),
                        "volume_growth_1h": candidate.get("volume_growth", {}).get("growth_1h", 0),
                        "tx_rate": candidate["tx_rate"],
                        "whale_flag": 1 if candidate["has_whale_activity"] else 0,
                        "is_meme": 1 if candidate["is_memecoin"] else 0
                    }
                    prediction = self.ml_predictor.predict_success(ml_features)
                    candidate["ml_prediction"] = prediction
                    if prediction > 0.7:
                        candidate["confidence"] = min(1.0, candidate["confidence"] * (1 + (prediction - 0.5)))
                    elif prediction < 0.3:
                        candidate["confidence"] = candidate["confidence"] * (0.5 + prediction)
                except Exception as e:
                    print(f"Error en predicción ML para {candidate['token']}: {e}")
        candidates.sort(key=lambda x: x["confidence"], reverse=True)
        print(f"Analizados {processed + skipped} tokens ({processed} procesados, {skipped} omitidos)")
        await self._generate_signals(candidates)

    async def _validate_token_safety_async(self, token):
        if not Config.get("ENABLE_RUGCHECK_FILTERING", False):
            return True
        if not self.rugcheck_api:
            return True
        try:
            min_score = int(Config.get("rugcheck_min_score", 50))
            is_safe = self.rugcheck_api.validate_token_safety(token, min_score)
            if not is_safe:
                print(f"⚠️ Token {token} no pasó validación de seguridad")
            return is_safe
        except Exception as e:
            print(f"Error validando token {token}: {e}")
            return True

    async def _generate_signals(self, candidates):
        now = time.time()
        min_confidence = float(Config.get("min_confidence_threshold", Config.MIN_CONFIDENCE_THRESHOLD))
        throttling = int(Config.get("signal_throttling", Config.SIGNAL_THROTTLING))
        signals_last_hour = db.count_signals_last_hour()
        if signals_last_hour >= throttling:
            print(f"Límite de señales alcanzado ({signals_last_hour}/{throttling}), no generando más señales")
            return
        signals_generated = 0
        for candidate in candidates:
            token = candidate["token"]
            confidence = candidate["confidence"]
            trader_count = candidate["trader_count"]
            initial_price = candidate.get("initial_price", 0)
            already_signaled = any(t == token for t, ts, _, _ in self.recent_signals if now - ts < 3600)
            if already_signaled or confidence < min_confidence:
                continue
            try:
                signal_id = db.save_signal(token, trader_count, confidence, initial_price)
                self.recent_signals.append((token, now, confidence, signal_id))
                if len(self.recent_signals) > 100:
                    self.recent_signals = self.recent_signals[-100:]
                message = self._format_signal_message(candidate, signal_id)
                send_telegram_message(message)
                if self.performance_tracker:
                    signal_info = {
                        "confidence": confidence,
                        "trader_count": trader_count,
                        "total_volume": candidate.get("volume_usd", 0),
                        "signal_id": signal_id
                    }
                    self.performance_tracker.add_signal(token, signal_info)
                signals_generated += 1
                print(f"✅ Señal generada para {token} con confianza {confidence:.2f}")
            except Exception as e:
                print(f"Error generando señal para {token}: {e}")
            if signals_generated >= (throttling - signals_last_hour):
                break

    def _format_signal_message(self, candidate, signal_id):
        token = candidate["token"]
        confidence = candidate["confidence"]
        trader_count = candidate["trader_count"]
        volume_usd = candidate.get("volume_usd", 0)
        market_cap = candidate.get("market_cap", 0)
        buy_percentage = candidate.get("buy_percentage", 0)
        ml_prediction = candidate.get("ml_prediction", 0.5)
        neobullx_link = f"https://neo.bullx.io/terminal?chainId=1399811149&address={token}"
        if confidence > 0.8:
            confidence_emoji = "🚀"
        elif confidence > 0.6:
            confidence_emoji = "🔥"
        else:
            confidence_emoji = "✅"
        message = (
            f"*{confidence_emoji} Nueva Señal #{signal_id}*\n\n"
            f"Token: `{token}`\n"
            f"Confianza: *{confidence:.2f}*\n"
            f"Predicción ML: *{ml_prediction:.2f}*\n\n"
            f"*📊 Datos:*\n"
            f"• Traders: `{trader_count}`\n"
            f"• Volumen: `${volume_usd:,.2f}`\n"
            f"• Market Cap: `${market_cap:,.2f}`\n"
            f"• % Compras: `{buy_percentage*100:.1f}%`\n\n"
            f"*🔍 Enlace Neo BullX:*\n"
            f"• [Ver en Neo BullX]({neobullx_link})\n\n"
            f"_Señal generada por ChipaTrading_"
        )
        return message
