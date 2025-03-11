import time
import math
import db
from config import Config

class ScoringSystem:
    """
    Sistema de scoring para evaluar la calidad de los traders
    y calcular la confianza de las señales.
    """

    def __init__(self):
        self.local_cache = {}
        self.last_cache_cleanup = time.time()
        self.wallet_tx_count = {}
        self.boosters = {}  # {wallet: {'active': bool, 'multiplier': float, 'expires': timestamp, 'reason': str}}
        self.token_type_scores = {}
        self.wallet_token_buys = {}  # Tracking para profit/loss
        self.wallet_profits = {}     # Historial de profit por wallet
        self._init_token_type_boosters()

    def _init_token_type_boosters(self):
        self.token_type_scores = {
            "meme": 1.2,
            "defi": 1.1,
            "nft": 1.15,
            "gaming": 1.1,
            "ai": 1.2,
            "new": 1.25
        }

    def get_score(self, wallet):
        if wallet not in self.local_cache:
            self.local_cache[wallet] = db.get_wallet_score(wallet)
        base_score = self.local_cache[wallet]
        if wallet in self.boosters and self.boosters[wallet]['active']:
            if time.time() < self.boosters[wallet]['expires']:
                return base_score * self.boosters[wallet]['multiplier']
            else:
                self.boosters[wallet]['active'] = False
        return base_score

    def update_score_on_trade(self, wallet, tx_data):
        tx_type = tx_data.get("type", "")
        token = tx_data.get("token", "")
        amount_usd = tx_data.get("amount_usd", 0)
        current_score = self.get_score(wallet)
        if wallet not in self.wallet_tx_count:
            self.wallet_tx_count[wallet] = 0
        self.wallet_tx_count[wallet] += 1
        wallet_token_key = f"{wallet}:{token}"
        if tx_type == "BUY":
            self.wallet_token_buys[wallet_token_key] = {
                'timestamp': time.time(),
                'amount_usd': amount_usd
            }
            if amount_usd > 10000:
                score_increment = Config.BUY_SCORE_INCREASE * 3
            elif amount_usd > 5000:
                score_increment = Config.BUY_SCORE_INCREASE * 2
            elif amount_usd > 1000:
                score_increment = Config.BUY_SCORE_INCREASE * 1.5
            else:
                score_increment = Config.BUY_SCORE_INCREASE
            new_score = current_score + score_increment
        elif tx_type == "SELL":
            profit_factor = 1.0
            if wallet_token_key in self.wallet_token_buys:
                buy_data = self.wallet_token_buys[wallet_token_key]
                buy_amount = buy_data['amount_usd']
                buy_time = buy_data['timestamp']
                hold_time_hours = (time.time() - buy_time) / 3600
                if amount_usd > buy_amount:
                    profit_percent = (amount_usd - buy_amount) / buy_amount
                    if hold_time_hours < 1:
                        profit_factor = 1.5 + (profit_percent * 2)
                    elif hold_time_hours < 24:
                        profit_factor = 1.2 + profit_percent
                    else:
                        profit_factor = 1.1 + (profit_percent * 0.5)
                    if wallet not in self.wallet_profits:
                        self.wallet_profits[wallet] = []
                    self.wallet_profits[wallet].append({
                        'token': token,
                        'profit_percent': profit_percent,
                        'hold_time_hours': hold_time_hours,
                        'timestamp': time.time()
                    })
                    print(f"📈 Trader {wallet} realizó profit de {profit_percent:.2%} en {hold_time_hours:.1f}h")
                else:
                    loss_percent = (buy_amount - amount_usd) / buy_amount
                    profit_factor = 1.0 - (loss_percent * 0.5)
                    if hold_time_hours > 48:
                        profit_factor = max(profit_factor, 0.9)
                    print(f"📉 Trader {wallet} vendió con pérdida de {loss_percent:.2%}")
            if amount_usd > 5000:
                score_increment = Config.SELL_SCORE_INCREASE * 2 * profit_factor
            else:
                score_increment = Config.SELL_SCORE_INCREASE * profit_factor
            new_score = current_score + score_increment
        else:
            return

        new_score = max(Config.MIN_SCORE, min(Config.MAX_SCORE, new_score))
        mid_score = (Config.MAX_SCORE + Config.MIN_SCORE) / 2
        if new_score > mid_score:
            decay_factor = 0.995
            new_score = mid_score + (new_score - mid_score) * decay_factor

        self.local_cache[wallet] = new_score
        db.update_wallet_score(wallet, new_score)
        self._apply_performance_boosters(wallet)
        if self.wallet_tx_count[wallet] % 50 == 0:
            self.add_score_booster(wallet, 1.2, 86400, "Actividad constante")
            print(f"🔥 Booster de actividad para {wallet} aplicado (+20% por 24h)")
        print(f"📊 Score de {wallet} actualizado: {current_score:.1f} → {new_score:.1f}")
        if time.time() - self.last_cache_cleanup > 3600:
            self.cleanup_cache()

    def _apply_performance_boosters(self, wallet):
        if wallet not in self.wallet_profits:
            return
        recent_profits = [p for p in self.wallet_profits[wallet] if time.time() - p['timestamp'] < 2592000]
        if len(recent_profits) >= 3:
            avg_profit = sum(p['profit_percent'] for p in recent_profits) / len(recent_profits)
            if avg_profit > 0.2:
                boost_multiplier = 1.3
                boost_duration = 172800
                boost_reason = f"Profit consistente ({avg_profit:.1%})"
                self.add_score_booster(wallet, boost_multiplier, boost_duration, boost_reason)
            quick_profits = [p for p in recent_profits if p['hold_time_hours'] < 2 and p['profit_percent'] > 0.3]
            if len(quick_profits) >= 2:
                boost_multiplier = 1.5
                boost_duration = 259200
                boost_reason = "Trader de momentum (ganancias rápidas)"
                self.add_score_booster(wallet, boost_multiplier, boost_duration, boost_reason)

    def compute_confidence(self, wallet_scores, volume_1h, market_cap, recent_volume_growth=0, token_type=None):
        if not wallet_scores:
            return 0.0

        exp_scores = [score ** 1.5 for score in wallet_scores]
        weighted_avg = sum(exp_scores) / (len(exp_scores) * (Config.MAX_SCORE ** 1.5)) * Config.MAX_SCORE
        score_factor = weighted_avg / Config.MAX_SCORE

        unique_wallets = len(wallet_scores)
        wallet_diversity = min(unique_wallets / 10, 1.0)

        high_quality_traders = sum(1 for score in wallet_scores if score > 7.0)
        elite_traders = sum(1 for score in wallet_scores if score > 9.0)
        quality_ratio = (high_quality_traders + (elite_traders * 2)) / max(1, len(wallet_scores))
        quality_factor = min(quality_ratio * 1.5, 1.0)
        elite_bonus = min(elite_traders * 0.1, 0.3)

        wallet_factor = (score_factor * 0.4) + (wallet_diversity * 0.3) + (quality_factor * 0.2) + elite_bonus

        if token_type == "meme":
            growth_factor = min(recent_volume_growth * 3.0, 1.0)
        else:
            growth_factor = min(recent_volume_growth * 1.5, 1.0)

        if market_cap <= 0:
            mc_factor = 0.3
        else:
            low_optimal = 500000
            high_optimal = 10000000
            if market_cap < Config.MIN_MARKETCAP:
                mc_factor = 0.3
            elif market_cap < low_optimal:
                normalized = (market_cap - Config.MIN_MARKETCAP) / (low_optimal - Config.MIN_MARKETCAP)
                mc_factor = 0.3 + (normalized * 0.5)
            elif market_cap <= high_optimal:
                mc_factor = 0.8
            elif market_cap <= Config.MAX_MARKETCAP:
                normalized = (Config.MAX_MARKETCAP - market_cap) / (Config.MAX_MARKETCAP - high_optimal)
                mc_factor = 0.5 + (normalized * 0.3)
            else:
                mc_factor = 0.5

        vol_factor = min(volume_1h / Config.VOL_NORMALIZATION_FACTOR, 1.0)

        if recent_volume_growth <= 0:
            growth_factor_final = 0.2
        elif recent_volume_growth < 0.05:
            growth_factor_final = 0.3 + (recent_volume_growth * 4)
        elif recent_volume_growth < 0.2:
            growth_factor_final = 0.5 + (recent_volume_growth * 2)
        else:
            growth_factor_final = 0.9 + min((recent_volume_growth - 0.2) * 0.5, 0.1)

        market_factor = (vol_factor * 0.4) + (mc_factor * 0.3) + (growth_factor_final * 0.3)

        weighted_score = (wallet_factor * 0.65) + (market_factor * 0.35)

        if token_type and token_type.lower() in self.token_type_scores:
            multiplier = self.token_type_scores[token_type.lower()]
            weighted_score *= multiplier
            print(f"🏷️ Booster aplicado para tipo {token_type}: x{multiplier}")

        def sigmoid_normalize(x, center=0.5, steepness=8):
            return 1 / (1 + math.exp(-steepness * (x - center)))

        normalized = max(0.1, min(1.0, sigmoid_normalize(weighted_score, 0.5, 8)))
        return round(normalized, 3)

    def cleanup_cache(self, max_size=1000):
        if len(self.local_cache) > max_size:
            self.local_cache = {}
            print(f"🧹 Cache de scores limpiada (superó {max_size} entradas)")
        now = time.time()
        expired_wallets = [wallet for wallet, data in self.boosters.items() if now > data['expires']]
        for wallet in expired_wallets:
            del self.boosters[wallet]
        old_data_cutoff = now - 7776000  # 90 días
        for wallet in self.wallet_profits:
            self.wallet_profits[wallet] = [p for p in self.wallet_profits[wallet] if p['timestamp'] > old_data_cutoff]
        buy_cutoff = now - 604800  # 7 días
        keys_to_remove = [k for k, v in self.wallet_token_buys.items() if v['timestamp'] < buy_cutoff]
        for key in keys_to_remove:
            del self.wallet_token_buys[key]
        if expired_wallets or keys_to_remove:
            print(f"🧹 Limpiados {len(expired_wallets)} boosters expirados y {len(keys_to_remove)} registros antiguos")
        self.last_cache_cleanup = time.time()

    def add_score_booster(self, wallet, multiplier, duration_seconds, reason=None):
        if wallet not in self.boosters:
            self.boosters[wallet] = {
                'active': True,
                'multiplier': multiplier,
                'expires': time.time() + duration_seconds,
                'reason': reason
            }
        else:
            self.boosters[wallet]['active'] = True
            self.boosters[wallet]['multiplier'] = max(self.boosters[wallet]['multiplier'], multiplier)
            self.boosters[wallet]['expires'] = time.time() + duration_seconds
            if reason:
                self.boosters[wallet]['reason'] = reason
        reason_str = f" - {reason}" if reason else ""
        print(f"🔥 Booster para {wallet}: x{multiplier} por {duration_seconds/3600:.1f}h{reason_str}")

    def get_wallet_profit_stats(self, wallet):
        if wallet not in self.wallet_profits or not self.wallet_profits[wallet]:
            return None
        profits = self.wallet_profits[wallet]
        recent_cutoff = time.time() - 2592000  # 30 días
        recent_profits = [p for p in profits if p['timestamp'] > recent_cutoff]
        if not recent_profits:
            return None
        avg_profit = sum(p['profit_percent'] for p in recent_profits) / len(recent_profits)
        max_profit = max(p['profit_percent'] for p in recent_profits)
        success_rate = len([p for p in recent_profits if p['profit_percent'] > 0]) / len(recent_profits)
        avg_hold_time = sum(p['hold_time_hours'] for p in recent_profits) / len(recent_profits)
        return {
            'avg_profit': avg_profit,
            'max_profit': max_profit,
            'success_rate': success_rate,
            'trade_count': len(recent_profits),
            'avg_hold_time': avg_hold_time
        }

    def get_top_traders(self, limit=10, min_trades=3):
        trader_stats = []
        for wallet, profits in self.wallet_profits.items():
            recent_cutoff = time.time() - 2592000  # 30 días
            recent_profits = [p for p in profits if p['timestamp'] > recent_cutoff]
            if len(recent_profits) < min_trades:
                continue
            avg_profit = sum(p['profit_percent'] for p in recent_profits) / len(recent_profits)
            success_rate = len([p for p in recent_profits if p['profit_percent'] > 0]) / len(recent_profits)
            trader_stats.append({
                'wallet': wallet,
                'avg_profit': avg_profit,
                'success_rate': success_rate,
                'trade_count': len(recent_profits),
                'score': self.get_score(wallet)
            })
        sorted_traders = sorted(trader_stats, key=lambda x: x['avg_profit'], reverse=True)
        return sorted_traders[:limit]
