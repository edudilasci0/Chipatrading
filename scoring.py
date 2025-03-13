import time
import math
import db
from config import Config

class ScoringSystem:
    def __init__(self):
        self.local_cache = {}
        self.last_cache_cleanup = time.time()
        self.wallet_tx_count = {}
        self.boosters = {}
        self.token_type_scores = {}
        self.wallet_token_buys = {}
        self.wallet_profits = {}
        self._init_token_type_boosters()

    def _init_token_type_boosters(self):
        self.token_type_scores = {
            "meme": 1.3,
            "defi": 1.1,
            "nft": 1.15,
            "gaming": 1.1,
            "ai": 1.2,
            "new": 1.25,
            "exchange": 1.0,
            "stable": 0.9
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
        # [Implementación similar, con decay y boosters]
        pass

    def compute_confidence(self, wallet_scores, volume_1h, market_cap, recent_volume_growth=0, token_type=None, whale_activity=False):
        if not wallet_scores:
            return 0.0
        exp_scores = [score ** 1.5 for score in wallet_scores]
        weighted_avg = sum(exp_scores) / (len(exp_scores) * (Config.MAX_SCORE ** 1.5)) * Config.MAX_SCORE
        score_factor = weighted_avg / Config.MAX_SCORE

        unique_wallets = len(wallet_scores)
        wallet_diversity = min(unique_wallets / 10.0, 1.0)
        high_quality_traders = sum(1 for score in wallet_scores if score > 8.0)
        elite_traders = sum(1 for score in wallet_scores if score > 9.0)
        quality_ratio = (high_quality_traders + (elite_traders * 2)) / max(1, len(wallet_scores))
        quality_factor = min(quality_ratio * 1.5, 1.0)
        elite_bonus = min(elite_traders * 0.1, 0.3)
        wallet_factor = (score_factor * 0.4) + (wallet_diversity * 0.3) + (quality_factor * 0.2) + elite_bonus

        if token_type == "meme":
            growth_factor = min(recent_volume_growth * 3.0, 1.0)
        else:
            growth_factor = min(recent_volume_growth * 1.5, 1.0)

        # [Cálculo del market_factor similar al original...]
        market_factor = 0.7  # Simplificado para este ejemplo

        weighted_score = (wallet_factor * 0.65) + (market_factor * 0.35)
        if whale_activity:
            weighted_score *= 1.1
        if token_type and token_type.lower() in self.token_type_scores:
            multiplier = self.token_type_scores[token_type.lower()]
            weighted_score *= multiplier

        def sigmoid_normalize(x, center=0.5, steepness=8):
            return 1 / (1 + math.exp(-steepness * (x - center)))
        normalized = max(0.1, min(1.0, sigmoid_normalize(weighted_score, 0.5, 8)))
        return round(normalized, 3)

    def get_all_scores(self):
        """
        Devuelve un diccionario con todos los scores conocidos para filtrado.
        """
        return self.local_cache

    def get_trader_name_from_wallet(self, wallet):
        """
        Retorna un nombre humano para la wallet, si se tiene esa información.
        Se puede consultar una base de datos o usar un mapeo predefinido.
        """
        mapping = db.get_trader_names_mapping()  # Supuesto método en DB
        return mapping.get(wallet, wallet)
