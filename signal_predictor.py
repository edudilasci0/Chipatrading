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

class SignalPredictor:
    """
    Clase optimizada para predecir el éxito de señales basándose en datos históricos.
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
        
        # Features ampliadas con mejores predictores
        self.features = [
            # Features principales
            'num_traders', 'num_transactions', 'total_volume_usd',
            'avg_volume_per_trader', 'buy_ratio', 'tx_velocity',
            'avg_trader_score', 'max_trader_score', 'market_cap', 
            'volume_1h', 'volume_growth_5m', 'volume_growth_1h',
            # Nuevos features
            'high_quality_ratio', 'elite_trader_count', 'normalized_volume',
            'normalized_mcap', 'min_trader_score', 'tx_per_trader'
        ]
        
        # Crear directorio para modelos si no existe
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
        
        # Intentar cargar modelo existente
        self.load_model()
        
        # Variables para tracking
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
                    self.precision = data.get('precision')
                    self.recall = data.get('recall')
                    self.f1_score = data.get('f1_score')
                    self.sample_count = data.get('sample_count', 0)
                    self.feature_importance = data.get('feature_importance')
                    
                print(f"✅ Modelo cargado desde {self.model_path}")
                print(f"   Exactitud: {self.accuracy:.4f}, Precision: {self.precision:.4f}, Recall: {self.recall:.4f}")
                print(f"   Muestras: {self.sample_count}, Última actualización: {self.last_training}")
                
                # Mostrar features más importantes
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
        Entrena un nuevo modelo con los datos históricos, implementando
        validación cruzada y manejo de desbalance de clases.
        
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
            
            # Analizar balance de clases
            successful_signals = df[df['success'] == 1].shape[0]
            unsuccessful_signals = df[df['success'] == 0].shape[0]
            
            print(f"📊 Balance de datos: {successful_signals} éxitos, {unsuccessful_signals} fracasos")
            print(f"   Ratio de éxito: {successful_signals / max(1, len(df)):.2%}")
            
            # Preparar features y target
            # Asegurarse de que todos los features necesarios están presentes
            available_features = [f for f in self.features if f in df.columns]
            missing_features = [f for f in self.features if f not in df.columns]
            
            if missing_features:
                print(f"⚠️ Algunos features no están disponibles: {missing_features}")
                print(f"   Se usarán solo los features disponibles: {len(available_features)}")
            
            X = df[available_features]
            
            if 'success' not in df.columns:
                print("⚠️ No se encontró la columna 'success' en los datos de entrenamiento")
                return False
                
            y = df['success']  # Columna que indica si fue exitoso (1) o no (0)
            
            # Calcular pesos de clase para compensar desbalance
            class_weights = class_weight.compute_class_weight(
                'balanced', classes=np.unique(y), y=y
            )
            class_weight_dict = {i: class_weights[i] for i in range(len(class_weights))}
            print(f"ℹ️ Pesos de clase para balanceo: {class_weight_dict}")
            
            # Dividir en train y test
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y
            )
            
            # Escalar features
            self.scaler = StandardScaler()
            X_train_scaled = self.scaler.fit_transform(X_train)
            
            # Entrenar con validación cruzada para verificar robustez
            kf = KFold(n_splits=5, shuffle=True, random_state=42)
            
            # Probar dos tipos de modelos
            rf_model = RandomForestClassifier(
                n_estimators=100, 
                max_depth=10,
                random_state=42,
                class_weight=class_weight_dict
            )
            
            gb_model = GradientBoostingClassifier(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=5,
                random_state=42
            )
            
            # Evaluar ambos modelos con validación cruzada
            rf_scores = cross_val_score(rf_model, X_train_scaled, y_train, cv=kf, scoring='f1')
            gb_scores = cross_val_score(gb_model, X_train_scaled, y_train, cv=kf, scoring='f1')
            
            print(f"🔄 Validación cruzada RandomForest: {rf_scores.mean():.4f} ± {rf_scores.std():.4f}")
            print(f"🔄 Validación cruzada GradientBoosting: {gb_scores.mean():.4f} ± {gb_scores.std():.4f}")
            
            # Elegir el mejor modelo
            if rf_scores.mean() >= gb_scores.mean():
                print("✅ Seleccionando modelo RandomForest")
                self.model = rf_model
                self.model.fit(X_train_scaled, y_train)
            else:
                print("✅ Seleccionando modelo GradientBoosting")
                self.model = gb_model
                self.model.fit(X_train_scaled, y_train)
            
            # Evaluar modelo
            X_test_scaled = self.scaler.transform(X_test)
            y_pred = self.model.predict(X_test_scaled)
            
            # Calcular métricas detalladas
            accuracy = accuracy_score(y_test, y_pred)
            precision, recall, f1, _ = precision_recall_fscore_support(
                y_test, y_pred, average='binary'
            )
            conf_matrix = confusion_matrix(y_test, y_pred)
            
            # Guardar métricas
            self.accuracy = accuracy
            self.precision = precision
            self.recall = recall
            self.f1_score = f1
            self.sample_count = len(df)
            self.last_training = datetime.now().isoformat()
            
            # Calcular importancia de features
            if hasattr(self.model, 'feature_importances_'):
                feature_importance = list(zip(available_features, self.model.feature_importances_))
                feature_importance.sort(key=lambda x: x[1], reverse=True)
                self.feature_importance = feature_importance
            
            # Guardar modelo
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
            print(f"📊 Métricas del modelo:")
            print(f"   Precisión: {accuracy:.4f}")
            print(f"   Precision: {precision:.4f}")
            print(f"   Recall: {recall:.4f}")
            print(f"   F1-Score: {f1:.4f}")
            print(f"Matriz de confusión:\n{conf_matrix}")
            
            # Imprimir reporte de clasificación
            report = classification_report(y_test, y_pred)
            print(f"Reporte de clasificación:\n{report}")
            
            # Importancia de features
            if self.feature_importance:
                print("🔍 Importancia de características:")
                for idx, (feature, importance) in enumerate(self.feature_importance):
                    print(f"  • {feature}: {importance:.4f}")
            
            return True
            
        except Exception as e:
            print(f"🚨 Error entrenando modelo: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def predict_success(self, signal_features):
        """
        Predice la probabilidad de éxito de una señal.
        
        Args:
            signal_features: Diccionario con las características de la señal.
            
        Returns:
            float: Probabilidad de éxito (0.0 a 1.0)
            dict: Factores que influyeron en la predicción (feature importance)
        """
        if not self.model or not self.scaler:
            print("⚠️ No hay modelo cargado para predicción")
            return 0.5, {}  # Valor neutro y diccionario vacío
        
        try:
            # Crear dataframe con los features disponibles
            features_df = pd.DataFrame([signal_features])
            
            # Verificar qué features están disponibles
            available_features = [f for f in self.features if f in features_df.columns]
            
            # Si faltan features importantes, intentar derivarlos
            if 'normalized_volume' not in features_df.columns and 'volume_1h' in features_df.columns:
                features_df['normalized_volume'] = features_df['volume_1h'] / 50000
                
            if 'normalized_mcap' not in features_df.columns and 'market_cap' in features_df.columns:
                features_df['normalized_mcap'] = features_df['market_cap'] / 10000000
                
            if 'high_quality_ratio' not in features_df.columns and 'avg_trader_score' in features_df.columns:
                # Estimar basado en score promedio
                features_df['high_quality_ratio'] = features_df['avg_trader_score'] / 10.0
            
            # Seleccionar solo los features que el modelo conoce
            X = features_df[available_features].copy()
            
            # Rellenar valores faltantes con 0
            X.fillna(0, inplace=True)
            
            # Escalar features
            X_scaled = self.scaler.transform(X)
            
            # Predecir probabilidad
            probabilities = self.model.predict_proba(X_scaled)
            success_probability = probabilities[0][1]  # Probabilidad de la clase 1 (éxito)
            
            # Calcular contribución de cada feature a la predicción
            feature_contributions = {}
            
            if hasattr(self.model, 'feature_importances_') and self.feature_importance:
                # Identificar qué features contribuyeron más a esta predicción específica
                for feature, importance in self.feature_importance:
                    if feature in X.columns:
                        # Normalizar valor del feature entre 0 y 1
                        feature_val = X[feature].iloc[0]
                        feature_contributions[feature] = {
                            'value': feature_val,
                            'importance': importance
                        }
            
            return success_probability
            
        except Exception as e:
            print(f"🚨 Error en predicción: {e}")
            import traceback
            traceback.print_exc()
            return 0.5, {}  # Valor neutro en caso de error
    
    def get_model_info(self):
        """
        Retorna información sobre el modelo actual.
        
        Returns:
            dict: Información del modelo
        """
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
        """
        Analiza la importancia de los features y proporciona insights.
        
        Returns:
            dict: Análisis de los features
        """
        if not self.model or not self.feature_importance:
            return {"error": "No hay modelo entrenado disponible"}
        
        # Agrupar features por categorías
        feature_categories = {
            "trader_quality": ["avg_trader_score", "max_trader_score", "min_trader_score", 
                              "high_quality_ratio", "elite_trader_count"],
            "volume_metrics": ["volume_1h", "normalized_volume", "volume_growth_5m", 
                              "volume_growth_1h", "total_volume_usd"],
            "market_metrics": ["market_cap", "normalized_mcap"],
            "transaction_patterns": ["num_traders", "num_transactions", "buy_ratio", 
                                    "tx_velocity", "tx_per_trader", "avg_volume_per_trader"]
        }
        
        # Calcular importancia por categoría
        category_importance = {}
        for category, features in feature_categories.items():
            category_total = 0
            available_features = 0
            for feature, importance in self.feature_importance:
                if feature in features:
                    category_total += importance
                    available_features += 1
            
            if available_features > 0:
                category_importance[category] = {
                    "total_importance": category_total,
                    "average_importance": category_total / available_features,
                    "available_features": available_features,
                    "total_features": len(features)
                }
        
        # Ordenar categorías por importancia
        sorted_categories = sorted(
            category_importance.items(),
            key=lambda x: x[1]["total_importance"],
            reverse=True
        )
        
        # Generar insights basados en el análisis
        insights = []
        for category, stats in sorted_categories:
            if category == "trader_quality" and stats["total_importance"] > 0.3:
                insights.append("La calidad de los traders es un factor muy importante para el éxito de la señal")
            elif category == "volume_metrics" and stats["total_importance"] > 0.3:
                insights.append("El volumen y su crecimiento son altamente predictivos del rendimiento")
            elif category == "transaction_patterns" and stats["total_importance"] > 0.3:
                insights.append("Los patrones de transacción (velocidad, ratio compra/venta) son buenos indicadores")
        
        # Top 5 features individuales
        top_features = self.feature_importance[:5]
        
        return {
            "category_importance": sorted_categories,
            "top_features": top_features,
            "insights": insights
        }
