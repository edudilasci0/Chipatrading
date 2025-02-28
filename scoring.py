# scoring.py
import db

class ScoringSystem:
    """
    Sistema de scoring con BD:
    +0.1 si BUY, +0.2 si SELL, saturado en [0..10].
    """

    def __init__(self):
        self.local_cache = {}

    def get_score(self, wallet):
        if wallet not in self.local_cache:
            self.local_cache[wallet] = db.get_wallet_score(wallet)
        return self.local_cache[wallet]

    def update_score_on_trade(self, wallet, tx_data):
        tx_type = tx_data.get("type", "")
        current_score = self.get_score(wallet)
        if tx_type == "BUY":
            new_score = current_score + 0.1
        else:
            new_score = current_score + 0.2

        new_score = max(0, min(10, new_score))
        self.local_cache[wallet] = new_score
        db.update_wallet_score(wallet, new_score)

    def compute_confidence(self, wallet_scores, volume_1h, market_cap):
        """
        Combina el avg wallet score, factor marketcap, y volumen 1h.
        """
        if not wallet_scores:
            return 0.0

        avg_score = sum(wallet_scores) / len(wallet_scores)
        score_factor = avg_score / 10.0

        # Market cap factor
        if market_cap < 100000:
            mc_factor = 0.0
        elif market_cap > 500000000:
            mc_factor = 0.0
        else:
            mc_factor = 1.0

        # volume_1h => normalizar
        vol_factor = min(volume_1h / 10000.0, 1.0)

        confidence = (score_factor + mc_factor + vol_factor) / 3
        return round(confidence, 3)
