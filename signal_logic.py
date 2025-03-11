import time
from config import Config

class SignalLogic:
    def __init__(self, scoring_system=None, helius_client=None, gmgn_client=None, rugcheck_api=None, ml_predictor=None):
        self.scoring_system = scoring_system
        self.helius_client = helius_client
        self.gmgn_client = gmgn_client  # Nuevo cliente GMGN
        self.rugcheck_api = rugcheck_api
        self.ml_predictor = ml_predictor
        self.performance_tracker = None
        self.token_candidates = {}  # {token: {wallets, transactions, last_update, volume_usd}}
        self.recent_signals = []    # Lista de (token, timestamp, confidence, signal_id)
        self.last_signal_check = time.time()

    async def get_token_market_data(self, token):
        data = None
        source = "none"
        # 1. Intentar con Helius
        if self.helius_client:
            data = self.helius_client.get_token_data(token)
            if data and data.get("market_cap", 0) > 0:
                source = "helius"
        # 2. Intentar con GMGN
        if (not data or data.get("market_cap", 0) == 0) and self.gmgn_client:
            gmgn_data = self.gmgn_client.get_market_data(token)
            if gmgn_data and gmgn_data.get("market_cap", 0) > 0:
                data = gmgn_data
                source = "gmgn"
        # 3. Fallback: estimar a partir de transacciones
        if not data or data.get("market_cap", 0) == 0:
            if token in self.token_candidates:
                recent_txs = self.token_candidates[token].get("transactions", [])
                now = time.time()
                window_seconds = float(Config.get("SIGNAL_WINDOW_SECONDS", Config.SIGNAL_WINDOW_SECONDS))
                cutoff = now - window_seconds
                recent_txs = [tx for tx in recent_txs if tx["timestamp"] > cutoff]
                if recent_txs:
                    volume_est = sum(tx["amount_usd"] for tx in recent_txs)
                    data = {
                        "price": 0,
                        "market_cap": 0,
                        "volume": volume_est,
                        "volume_growth": {"growth_5m": 0, "growth_1h": 0},
                        "estimated": True
                    }
                    source = "estimated"
        if not data:
            data = {
                "price": 0,
                "market_cap": 0,
                "volume": 0,
                "volume_growth": {"growth_5m": 0, "growth_1h": 0},
                "estimated": True
            }
            source = "none"
        data["source"] = source
        return data

    async def _process_candidates(self):
        now = time.time()
        window_seconds = float(Config.get("SIGNAL_WINDOW_SECONDS", Config.SIGNAL_WINDOW_SECONDS))
        cutoff = now - window_seconds
        candidates = []

        for token, data in list(self.token_candidates.items()):
            try:
                recent_txs = [tx for tx in data["transactions"] if tx["timestamp"] > cutoff]
                if not recent_txs:
                    continue
                trader_count = len(data["wallets"])
                volume_usd = sum(tx["amount_usd"] for tx in recent_txs)
                buy_txs = [tx for tx in recent_txs if tx["type"] == "BUY"]
                buy_percentage = len(buy_txs) / max(1, len(recent_txs))
                
                min_traders = int(Config.get("MIN_TRADERS_FOR_SIGNAL", Config.MIN_TRADERS_FOR_SIGNAL))
                min_volume = float(Config.get("MIN_VOLUME_USD", Config.MIN_VOLUME_USD))
                if trader_count < min_traders or volume_usd < min_volume:
                    continue

                # Obtener datos de mercado usando fallback
                market_data = await self.get_token_market_data(token)
                market_cap = market_data.get("market_cap", 0)
                vol_growth = market_data.get("volume_growth", {})

                tx_rate = len(recent_txs) / window_seconds

                token_type = None
                if self.gmgn_client and self.gmgn_client.is_memecoin(token):
                    token_type = "meme"
                elif vol_growth.get("growth_5m", 0) > 0.2 and market_cap < 5_000_000:
                    token_type = "meme"

                trader_scores = [self.scoring_system.get_score(w) for w in data["wallets"]]
                confidence = self.scoring_system.compute_confidence(
                    wallet_scores=trader_scores,
                    volume_1h=market_data.get("volume", 0),
                    market_cap=market_cap,
                    recent_volume_growth=vol_growth.get("growth_5m", 0),
                    token_type=token_type
                )

                config = Config.MEMECOIN_CONFIG
                is_memecoin = (tx_rate > config["TX_RATE_THRESHOLD"] and 
                               vol_growth.get("growth_5m", 0) > config["VOLUME_GROWTH_THRESHOLD"] and 
                               market_cap < 10_000_000)
                whale_threshold = 10000
                if recent_txs and max(tx["amount_usd"] for tx in recent_txs) > whale_threshold:
                    is_memecoin = True

                if is_memecoin:
                    confidence *= 1.5

                candidates.append({
                    "token": token,
                    "confidence": confidence,
                    "ml_prediction": 0.5,
                    "trader_count": trader_count,
                    "volume_usd": volume_usd,
                    "recent_transactions": recent_txs,
                    "market_cap": market_cap,
                    "volume_1h": market_data.get("volume", 0),
                    "volume_growth": vol_growth,
                    "buy_percentage": buy_percentage,
                    "trader_scores": trader_scores,
                    "initial_price": market_data.get("price", 0),
                    "data_source": market_data.get("source", "unknown")
                })
            except Exception as e:
                print(f"Error procesando candidato {token}: {e}")

        candidates.sort(key=lambda x: x["confidence"], reverse=True)
        await self._generate_signals(candidates)
