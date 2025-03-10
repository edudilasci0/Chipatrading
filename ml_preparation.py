import pandas as pd
import time

class MLDataPreparation:
    def __init__(self):
        # Ruta donde se almacenarán los datos de entrenamiento (opcional)
        self.training_data_path = "training_data.csv"

    def get_historical_data(self):
        """
        Recupera datos históricos de señales desde la base de datos o un archivo.
        Aquí se simula la obtención de datos; en producción se consultarían registros reales.
        Retorna una lista de diccionarios con los datos históricos.
        """
        # Ejemplo simulado; en la práctica, se recuperarán datos de la BD.
        return []

    def prepare_training_data(self):
        """
        Prepara el conjunto de datos para entrenar el modelo ML.
        Se añaden nuevas features:
          - tx_rate: Número de transacciones por segundo (calculado como transacciones / window_seconds).
          - whale_flag: 1 si alguna transacción supera un umbral (por ejemplo, 10,000 USD).
          - is_meme: 1 si el token tiene características de memecoin (por ejemplo, crecimiento > 30% y market cap < 5M).
        Retorna un DataFrame listo para entrenamiento.
        """
        data = self.get_historical_data()
        for record in data:
            window_seconds = record.get("window_seconds", 1)
            record["tx_rate"] = record.get("transactions", 0) / window_seconds
            record["whale_flag"] = 1 if record.get("max_tx_amount", 0) > 10000 else 0
            record["is_meme"] = 1 if (record.get("volume_growth_5m", 0) > 0.3 and record.get("market_cap", 0) < 5_000_000) else 0
        df = pd.DataFrame(data)
        # Guardar en CSV para análisis o para futuros entrenamientos (opcional)
        df.to_csv(self.training_data_path, index=False)
        return df

    def analyze_feature_correlations(self):
        """
        Analiza las correlaciones entre las features del conjunto de entrenamiento.
        Retorna el DataFrame de correlaciones.
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
        Este método puede eliminar registros de la base de datos o archivos que tengan más de 'days' días.
        """
        # Ejemplo: simulación de limpieza
        print(f"Limpieza de datos anteriores a {days} días realizada.")

    def extract_signal_features(self, token, dex_client, scoring_system):
        """
        Extrae features de una señal específica.
        Estas features se usarán en la predicción ML.
        
        Args:
            token (str): Identificador del token.
            dex_client: Cliente para obtener datos de mercado (si aplica).
            scoring_system: Instancia del sistema de scoring para obtener scores.
        
        Returns:
            dict: Conjunto de features relevantes, por ejemplo:
                  'tx_rate', 'whale_flag', 'is_meme', y 'score'.
        """
        # Ejemplo simulado: en producción, se extraerán datos reales de la señal
        features = {
            "tx_rate": 5.0,          # Simulación: 5 transacciones por segundo
            "whale_flag": 0,         # 0 si no se detecta actividad de ballenas
            "is_meme": 1,            # 1 si se detecta como memecoin
            "score": scoring_system.get_score("dummy_wallet")
        }
        print(f"Features extraídas para token {token}: {features}")
        return features

