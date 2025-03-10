import pandas as pd
import numpy as np
import pickle
import os
import time
import joblib
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, KFold, cross_val_score
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, precision_recall_fscore_support
from sklearn.utils import class_weight
import math

class SignalPredictor:
    """
    Clase optimizada para predecir el éxito de señales basándose en datos históricos.
    Se integra la posibilidad de usar nuevas features (tx_rate, whale_flag, is_meme) para mejorar
    la detección de daily runners en memecoins.
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
        
        # Conjunto de features actuales (se pueden ampliar con las nuevas features)
        self.features = [
            'num_traders', 'num_transactions', 'total_volume_usd',
            'avg_volume_per_trader', 'buy_ratio', 'tx_velocity',
            'avg_trader_score', 'max_trader_score', 'market_cap', 
            'volume_1h', 'volume_growth_5m', 'volume_growth_1h',
            # Nuevas features
            'tx_rate', 'whale_flag', 'is_meme'
        ]
        
        # Crear directorio para modelos si no existe
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
        
        # Intentar cargar el modelo existente
        self.load_model()
        
        # Variables para tracking y métricas
        self.last_training = None
        self.accuracy = None
        self.precision = None
        self.recall = None
        self.f1_score = None
        self.sample_count = 0
        self.feature_importance = None
    
    def load_model(self):
        """
        Carga el modelo desde el archivo si existe.
        
        Returns:
            bool: True si se cargó correctamente, False en caso contrario.
        """
        try:
            if os.path.exists(self.model_path):
                with open(self.model_path, 'rb') as f:
                    data = pickle.load(f)
                    self.model = data['model']
                    self.scaler = data['scaler']
                    self.last_training = data.get('timestamp')
                    self.accuracy = data.get('accuracy')
                    self.precision = data.get('precision')
                    self.recall = data.get('recall')
                    self.f1_score = data.get('f1_score')
                    self.sample_count = data.get('sample_count', 0)
                    self.feature_importance = data.get('feature_importance')
                    
                print(f"✅ Modelo cargado desde {self.model_path}")
                print(f"   Exactitud: {self.accuracy:.4f}, Precision: {self.precision:.4f}, Recall: {self.recall:.4f}")
                print(f"   Muestras: {self.sample_count}, Última actualización: {self.last_training}")
                if self.feature_importance is not None:
                    print("   Features más importantes:")
                    for i, (feature, importance) in enumerate(self.feature_importance[:5]):
                        print(f"   {i+1}. {feature}: {importance:.4f}")
                return True
            return False
        except Exception as e:
            print(f"⚠️ Error cargando modelo: {e}")
            return False
    
    def train_model(self, training_data_path="ml_data/training_data.csv", force=False):
        """
        Entrena un nuevo modelo con los datos históricos, implementando validación cruzada
        y manejo de desbalance de clases. Se incorporan nuevas features si están disponibles.
        
        Args:
            training_data_path: Ruta al CSV con datos de entrenamiento.
            force: Si es True, entrena incluso si hay pocos datos nuevos.
            
        Returns:
            bool: True si se entrenó correctamente, False si no.
        """
        try:
            if not os.path.exists(training_data_path):
                print(f"⚠️ No se encontró el archivo de entrenamiento: {training_data_path}")
                return False
            
            df = pd.read_csv(training_data_path)
            
            if len(df) < 20:
                print(f"⚠️ Datos insuficientes para entrenar: {len(df)} registros")
                return False
                
            if not force and self.sample_count >= len(df) * 0.9:
                print(f"ℹ️ No hay suficientes datos nuevos para reentrenar (actual: {len(df)}, previo: {self.sample_count})")
                return False
            
            # Aquí se espera que el CSV contenga una columna 'success' (1 o 0)
            if 'success' not in df.columns:
                print("⚠️ No se encontró la columna 'success' en los datos de entrenamiento")
                return False
            
            # Si se han definido nuevas features y no están presentes, derivarlas o asignar valores por defecto
            if 'tx_rate' not in df.columns and 'num_transactions' in df.columns and 'window_seconds' in df.columns:
                df['tx_rate'] = df['num_transactions'] / df['window_seconds']
            if 'whale_flag' not in df.columns:
                # Asumir que si no hay dato, se asigna 0 (sin actividad de ballenas)
                df['whale_flag'] = 0
            if 'is_meme' not in df.columns:
                # Se puede derivar en función de market_cap y volumen de crecimiento
                df['is_meme'] = np.where((df['market_cap'] < 5000000) & (df['volume_growth_5m'] > 0.3), 1, 0)
            
            available_features = [f for f in self.features if f in df.columns]
            missing_features = [f for f in self.features if f not in df.columns]
            if missing_features:
                print(f"⚠️ Faltan features: {missing_features}. Se usarán solo los disponibles: {available_features}")
            
            X = df[available_features]
            y = df['success']
            
            # Calcular pesos de clase
            from sklearn.utils import class_weight
            class_weights = class_weight.compute_class_weight('balanced', classes=np.unique(y), y=y)
            class_weight_dict = {i: class_weights[i] for i in range(len(class_weights))}
            print(f"ℹ️ Pesos de clase: {class_weight_dict}")
            
            from sklearn.model_selection import train_test_split
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
            
            from sklearn.preprocessing import StandardScaler
            self.scaler = StandardScaler()
            X_train_scaled = self.scaler.fit_transform(X_train)
            
            # Validación cruzada
            from sklearn.model_selection import KFold, cross_val_score
            kf = KFold(n_splits=5, shuffle=True, random_state=42)
            from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
            
            rf_model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, class_weight=class_weight_dict)
            gb_model = GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42)
            
            rf_scores = cross_val_score(rf_model, X_train_scaled, y_train, cv=kf, scoring='f1')
            gb_scores = cross_val_score(gb_model, X_train_scaled, y_train, cv=kf, scoring='f1')
            print(f"🔄 RandomForest F1: {rf_scores.mean():.4f} ± {rf_scores.std():.4f}")
            print(f"🔄 GradientBoosting F1: {gb_scores.mean():.4f} ± {gb_scores.std():.4f}")
            
            if rf_scores.mean() >= gb_scores.mean():
                print("✅ Seleccionando RandomForest")
                self.model = rf_model
                self.model.fit(X_train_scaled, y_train)
            else:
                print("✅ Seleccionando GradientBoosting")
                self.model = gb_model
                self.model.fit(X_train_scaled, y_train)
            
            X_test_scaled = self.scaler.transform(X_test)
            y_pred = self.model.predict(X_test_scaled)
            from sklearn.metrics import accuracy_score, precision_recall_fscore_support, confusion_matrix
            accuracy = accuracy_score(y_test, y_pred)
            precision, recall, f1, _ = precision_recall_fscore_support(y_test, y_pred, average='binary')
            conf_matrix = confusion_matrix(y_test, y_pred)
            
            self.accuracy = accuracy
            self.precision = precision
            self.recall = recall
            self.f1_score = f1
            self.sample_count = len(df)
            self.last_training = datetime.now().isoformat()
            
            if hasattr(self.model, 'feature_importances_'):
                feature_importance = list(zip(available_features, self.model.feature_importances_))
                feature_importance.sort(key=lambda x: x[1], reverse=True)
                self.feature_importance = feature_importance
            
            os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
            with open(self.model_path, 'wb') as f:
                pickle.dump({
                    'model': self.model, 
                    'scaler': self.scaler,
                    'accuracy': accuracy,
                    'precision': precision,
                    'recall': recall,
                    'f1_score': f1,
                    'sample_count': self.sample_count,
                    'timestamp': self.last_training,
                    'feature_importance': self.feature_importance
                }, f)
            
            print(f"✅ Modelo entrenado y guardado en {self.model_path}")
            print("📊 Métricas del modelo:")
            print(f"   Accuracy: {accuracy:.4f}")
            print(f"   Precision: {precision:.4f}")
            print(f"   Recall: {recall:.4f}")
            print(f"   F1-Score: {f1:.4f}")
            print(f"Matriz de confusión:\n{conf_matrix}")
            report = classification_report(y_test, y_pred)
            print(f"Reporte de clasificación:\n{report}")
            if self.feature_importance:
                print("🔍 Importancia de características:")
                for i, (feature, importance) in enumerate(self.feature_importance[:5]):
                    print(f"   {i+1}. {feature}: {importance:.4f}")
            return True
        except Exception as e:
            print(f"🚨 Error entrenando modelo: {e}")
            import traceback
            traceback.print_exc()
            return False

    def predict_success(self, signal_features):
        """
        Predice la probabilidad de éxito de una señal utilizando el modelo entrenado.
        Combina la predicción del modelo con el score (si está incluido en las features).
        
        Args:
            signal_features (dict): Conjunto de features de la señal.
        
        Returns:
            float: Probabilidad de éxito (entre 0 y 1).
        """
        if self.model is None or self.scaler is None:
            print("⚠️ Modelo o scaler no disponibles para predicción")
            return 0.5
        
        # Crear DataFrame a partir de las features
        features_df = pd.DataFrame([signal_features])
        
        # Verificar y derivar nuevas features si faltan
        if 'tx_rate' not in features_df.columns and 'num_transactions' in features_df.columns and 'window_seconds' in features_df.columns:
            features_df['tx_rate'] = features_df['num_transactions'] / features_df['window_seconds']
        if 'whale_flag' not in features_df.columns and 'volume_1h' in features_df.columns:
            # Valor base 0, se puede ajustar según algún umbral
            features_df['whale_flag'] = 0
        if 'is_meme' not in features_df.columns and 'market_cap' in features_df.columns and 'volume_growth_5m' in features_df.columns:
            features_df['is_meme'] = np.where((features_df['market_cap'] < 5000000) & (features_df['volume_growth_5m'] > 0.3), 1, 0)
        
        available_features = [f for f in self.features if f in features_df.columns]
        X = features_df[available_features].copy()
        X.fillna(0, inplace=True)
        
        X_scaled = self.scaler.transform(X)
        probabilities = self.model.predict_proba(X_scaled)
        success_probability = probabilities[0][1]
        
        return success_probability

    def get_model_info(self):
        return {
            "accuracy": self.accuracy,
            "precision": self.precision,
            "recall": self.recall,
            "f1_score": self.f1_score,
            "sample_count": self.sample_count,
            "last_training": self.last_training,
            "features": self.features,
            "model_exists": self.model is not None,
            "top_features": self.feature_importance[:5] if self.feature_importance else None
        }

    def feature_analysis(self):
        if self.model is None or self.feature_importance is None:
            return {"error": "No hay modelo entrenado disponible"}
        
        feature_categories = {
            "trader_quality": ["avg_trader_score", "max_trader_score", "min_trader_score", "high_quality_ratio", "elite_trader_count"],
            "volume_metrics": ["volume_1h", "normalized_volume", "volume_growth_5m", "volume_growth_1h", "total_volume_usd"],
            "market_metrics": ["market_cap", "normalized_mcap"],
            "transaction_patterns": ["num_traders", "num_transactions", "buy_ratio", "tx_velocity", "tx_per_trader", "avg_volume_per_trader"]
        }
        
        category_importance = {}
        for category, feats in feature_categories.items():
            total_importance = 0
            count = 0
            for feature, importance in self.feature_importance:
                if feature in feats:
                    total_importance += importance
                    count += 1
            if count > 0:
                category_importance[category] = {
                    "total_importance": total_importance,
                    "average_importance": total_importance / count,
                    "available_features": count,
                    "total_features": len(feats)
                }
        
        sorted_categories = sorted(category_importance.items(), key=lambda x: x[1]["total_importance"], reverse=True)
        insights = []
        for category, stats in sorted_categories:
            if category == "trader_quality" and stats["total_importance"] > 0.3:
                insights.append("La calidad de los traders es un factor muy importante para el éxito de la señal.")
            elif category == "volume_metrics" and stats["total_importance"] > 0.3:
                insights.append("El volumen y su crecimiento son altamente predictivos del rendimiento.")
            elif category == "transaction_patterns" and stats["total_importance"] > 0.3:
                insights.append("Los patrones de transacción son buenos indicadores.")
        
        top_features = self.feature_importance[:5]
        
        return {
            "category_importance": sorted_categories,
            "top_features": top_features,
            "insights": insights
        }
