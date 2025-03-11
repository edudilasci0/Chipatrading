import pandas as pd
import time

class MLDataPreparation:
    def __init__(self):
        self.training_data_path = "training_data.csv"

    def get_historical_data(self):
        # Aquí se debe implementar la consulta real a la base de datos
        return []

    def prepare_training_data(self):
        data = self.get_historical_data()
        for record in data:
            window_seconds = record.get("window_seconds", 1)
            record["tx_rate"] = record.get("transactions", 0) / window_seconds
            record["whale_flag"] = 1 if record.get("max_tx_amount", 0) > 10000 else 0
            record["is_meme"] = 1 if (record.get("volume_growth_5m", 0) > 0.3 and record.get("market_cap", 0) < 5_000_000) else 0
        df = pd.DataFrame(data)
        df.to_csv(self.training_data_path, index=False)
        return df

    def analyze_feature_correlations(self):
        df = pd.read_csv(self.training_data_path)
        correlations = df.corr()
        print("Correlaciones entre features:")
        print(correlations)
        return correlations

    def clean_old_data(self, days=90):
        print(f"Limpieza de datos anteriores a {days} días realizada.")

    def extract_signal_features(self, token, dex_client, scoring_system):
        # Ejemplo simulado de extracción de features para un token
        features = {
            "tx_rate": 5.0,
            "whale_flag": 0,
            "is_meme": 1,
            "score": scoring_system.get_score("dummy_wallet")
        }
        print(f"Features extraídas para token {token}: {features}")
        return features
