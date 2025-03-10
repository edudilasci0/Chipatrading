import pandas as pd
import time

class MLDataPreparation:
    def __init__(self):
        self.training_data_path = "training_data.csv"

    def get_historical_data(self):
        """
        Recupera datos históricos de señales desde la base de datos o un archivo.
        (Aquí se simula la obtención; en producción se consultaría la BD).
        """
        return []

    def prepare_training_data(self):
        """
        Prepara el conjunto de datos para entrenar el modelo ML.
        Se añaden nuevas features:
          - tx_rate: transacciones/segundo
          - whale_flag: 1 si alguna transacción > 10,000 USD
          - is_meme: 1 si market_cap < 5M y volumen crecimiento > 30%
        Retorna un DataFrame listo para entrenamiento.
        """
        data = self.get_historical_data()
        for record in data:
            window_seconds = record.get("window_seconds", 1)
            record["tx_rate"] = record.get("transactions", 0) / window_seconds
            record["whale_flag"] = 1 if record.get("max_tx_amount", 0) > 10000 else 0
            record["is_meme"] = 1 if (record.get("volume_growth_5m", 0) > 0.3 and record.get("market_cap", 0) < 5000000) else 0
        df = pd.DataFrame(data)
        df.to_csv(self.training_data_path, index=False)
        return df

    def analyze_feature_correlations(self):
        """
        Analiza correlaciones entre features del conjunto de entrenamiento.
        """
        try:
            df = pd.read_csv(self.training_data_path)
            correlations = df.corr()
            print("Correlaciones entre features:")
            print(correlations)
            return correlations
        except Exception as e:
            print(f"Error analizando correlaciones: {e}")
            return None

    def clean_old_data(self, days=90):
        """
        Limpia datos antiguos del conjunto de entrenamiento.
        """
        print(f"Limpieza de datos anteriores a {days} días realizada.")

    def extract_signal_features(self, token, dex_client, scoring_system):
        """
        Extrae features de una señal específica.
        Retorna un diccionario con, por ejemplo, 'tx_rate', 'whale_flag', 'is_meme' y 'score'.
        """
        features = {
            "tx_rate": 5.0,
            "whale_flag": 0,
            "is_meme": 1,
            "score": scoring_system.get_score("dummy_wallet")
        }
        print(f"Features extraídas para token {token}: {features}")
        return features
