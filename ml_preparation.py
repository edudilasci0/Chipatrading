import os
import pandas as pd
from config import Config
import db

class MLDataPreparation:
    def __init__(self, training_data_path="ml_data/training_data.csv"):
        self.training_data_path = training_data_path

    def collect_signal_outcomes(self):
        """
        Recolecta los outcomes de las señales para entrenamiento del modelo.
        Returns:
            int: Número de outcomes recolectados
        """
        try:
            signals = db.get_signals_without_outcomes()
            if not signals:
                print("No hay señales pendientes para recolectar outcomes")
                return 0
            count = 0
            for signal in signals:
                signal_id = signal["id"]
                performances = db.get_signal_performance(signal_id)
                if not performances:
                    continue
                # Se considera exitosa la señal si alguna medición supera el 10%
                success = any(perf["percent_change"] > 10 for perf in performances)
                features = db.get_signal_features(signal_id)
                if not features:
                    continue
                features["success"] = 1 if success else 0
                self._save_feature_with_outcome(features)
                db.mark_signal_outcome_collected(signal_id)
                count += 1
            print(f"✅ Recolectados {count} outcomes de señales")
            return count
        except Exception as e:
            print(f"Error recolectando outcomes: {e}")
            return 0

    def _save_feature_with_outcome(self, feature_data):
        """Guarda features con outcome para entrenamiento"""
        df = pd.DataFrame([feature_data])
        if not os.path.exists(self.training_data_path):
            df.to_csv(self.training_data_path, index=False)
        else:
            df.to_csv(self.training_data_path, mode='a', header=False, index=False)
