import pandas as pd
import numpy as np
import pickle
import os
import time
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score

class SignalPredictor:
    """
    Clase para predecir el éxito de señales basándose en datos históricos.
    """
    
    def __init__(self, model_path="ml_data/models/signal_model.pkl"):
        """
        Inicializa el predictor de señales.
        
        Args:
            model_path: Ruta donde se guarda/carga el modelo.
        """
        self.model_path = model_path
        self.model = None
        self.scaler = None
        self.features = [
            'num_traders', 'num_transactions', 'total_volume_usd',
            'avg_volume_per_trader', 'buy_ratio', 'tx_velocity',
            'avg_trader_score', 'max_trader_score', 'market_cap', 
            'volume_1h', 'volume_growth_5m', 'volume_growth_1h'
        ]
        
        # Crear directorio para modelos si no existe
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
        
        # Intentar cargar modelo existente
        self.load_model()
        
        # Variables para tracking
        self.last_training = None
        self.accuracy = None
        self.sample_count = 0
    
    def load_model(self):
        """
        Carga el modelo desde el archivo si existe.
        
        Returns:
            bool: True si se cargó correctamente, False si no
        """
        try:
            if os.path.exists(self.model_path):
                with open(self.model_path, 'rb') as f:
                    data = pickle.load(f)
                    self.model = data['model']
                    self.scaler = data['scaler']
                    self.last_training = data.get('timestamp')
                    self.accuracy = data.get('accuracy')
                    self.sample_count = data.get('sample_count', 0)
                    
                print(f"✅ Modelo cargado desde {self.model_path}")
                print(f"   Exactitud: {self.accuracy:.4f}, Muestras: {self.sample_count}")
                print(f"   Última actualización: {self.last_training}")
                return True
            return False
        except Exception as e:
            print(f"⚠️ Error cargando modelo: {e}")
            return False
    
    def train_model(self, training_data_path="ml_data/training_data.csv", force=False):
        """
        Entrena un nuevo modelo con los datos históricos.
        
        Args:
            training_data_path: Ruta al CSV con datos de entrenamiento.
            force: Si es True, entrena incluso si hay pocos datos nuevos.
            
        Returns:
            bool: True si se entrenó correctamente, False si no
        """
        try:
            # Cargar datos de entrenamiento
            if not os.path.exists(training_data_path):
                print(f"⚠️ No se encontró el archivo de entrenamiento: {training_data_path}")
                return False
            
            df = pd.read_csv(training_data_path)
            
            # Verificar que tenemos suficientes datos
            if len(df) < 20:
                print(f"⚠️ Datos insuficientes para entrenar: {len(df)} registros")
                return False
                
            # Verificar si hay suficientes datos nuevos
            if not force and self.sample_count >= len(df) * 0.9:
                print(f"ℹ️ No hay suficientes datos nuevos para reentrenar (actual: {len(df)}, previo: {self.sample_count})")
                return False
            
            # Preparar features y target
            # Asegurarse de que todos los features necesarios están presentes
            missing_features = [f for f in self.features if f not in df.columns]
            if missing_features:
                print(f"⚠️ Faltan features en los datos de entrenamiento: {missing_features}")
                return False
                
            X = df[self.features]
            if 'success' not in df.columns:
                print("⚠️ No se encontró la columna 'success' en los datos de entrenamiento")
                return False
                
            y = df['success']  # Columna que indica si fue exitoso (1) o no (0)
            
            # Dividir en train y test
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y
            )
            
            # Escalar features
            self.scaler = StandardScaler()
            X_train_scaled = self.scaler.fit_transform(X_train)
            
            # Entrenar modelo
            self.model = RandomForestClassifier(
                n_estimators=100, 
                max_depth=10,
                random_state=42,
                class_weight='balanced'
            )
            self.model.fit(X_train_scaled, y_train)
            
            # Evaluar modelo
            X_test_scaled = self.scaler.transform(X_test)
            y_pred = self.model.predict(X_test_scaled)
            
            # Calcular métricas
            accuracy = accuracy_score(y_test, y_pred)
            conf_matrix = confusion_matrix(y_test, y_pred)
            
            # Guardar métricas
            self.accuracy = accuracy
            self.sample_count = len(df)
            self.last_training = datetime.now().isoformat()
            
            # Guardar modelo
            os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
            with open(self.model_path, 'wb') as f:
                pickle.dump({
                    'model': self.model, 
                    'scaler': self.scaler,
                    'accuracy': accuracy,
                    'sample_count': self.sample_count,
                    'timestamp': self.last_training
                }, f)
            
            print(f"✅ Modelo entrenado y guardado en {self.model_path}")
            print(f"📊 Precisión del modelo: {accuracy:.4f}")
            print(f"Matriz de confusión:\n{conf_matrix}")
            
            # Imprimir reporte de clasificación
            report = classification_report(y_test, y_pred)
            print(f"Reporte de clasificación:\n{report}")
            
            # Importancia de features
            feature_importance = pd.DataFrame({
                'feature': self.features,
                'importance': self.model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            print("🔍 Importancia de características:")
            for idx, row in feature_importance.iterrows():
                print(f"  • {row['feature']}: {row['importance']:.4f}")
            
            return True
            
        except Exception as e:
            print(f"🚨 Error entrenando modelo: {e}")
            return False
    
    def predict_success(self, signal_features):
        """
        Predice la probabilidad de éxito de una señal.
        
        Args:
            signal_features: Diccionario con las características de la señal.
            
        Returns:
            float: Probabilidad de éxito (0.0 a 1.0)
        """
        if not self.model or not self.scaler:
            print("⚠️ No hay modelo cargado para predicción")
            return 0.5  # Valor neutro
        
        try:
            # Extraer features en el orden correcto
            features = []
            for feature in self.features:
                if feature in signal_features:
                    features.append(signal_features[feature])
                else:
                    print(f"⚠️ Feature faltante: {feature}, usando 0 como valor por defecto")
                    features.append(0)  # Valor por defecto
            
            # Convertir a array y escalar
            X = np.array([features])
            X_scaled = self.scaler.transform(X)
            
            # Predecir probabilidad
            probabilities = self.model.predict_proba(X_scaled)
            success_probability = probabilities[0][1]  # Probabilidad de la clase 1 (éxito)
            
            return success_probability
            
        except Exception as e:
            print(f"🚨 Error en predicción: {e}")
            return 0.5  # Valor neutro
    
    def get_model_info(self):
        """
        Retorna información sobre el modelo actual.
        
        Returns:
            dict: Información del modelo
        """
        return {
            "accuracy": self.accuracy,
            "sample_count": self.sample_count,
            "last_training": self.last_training,
            "features": self.features,
            "model_exists": self.model is not None
        }
