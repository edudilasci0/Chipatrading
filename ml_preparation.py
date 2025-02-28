import pandas as pd
import numpy as np
import json
import os
import db
import time
from datetime import datetime, timedelta
from config import Config

class MLDataPreparation:
    """
    Clase para preparar datos para modelos de machine learning.
    Permite extraer features relevantes de las señales y transacciones
    para entrenar modelos predictivos.
    """
    
    def __init__(self):
        """
        Inicializa la clase de preparación de datos para ML.
        """
        self.features_cache = {}  # {token: features_dict}
        self.outcomes_cache = {}  # {token: outcomes_dict}
        self.last_save = time.time()
        self.auto_save_interval = 3600  # Guardar cada hora
        
        # Crear directorio para datos ML si no existe
        os.makedirs("ml_data", exist_ok=True)
    
    def extract_signal_features(self, token, dex_client, scoring_system):
        """
        Extrae características (features) de una señal para su uso en modelos ML.
        
        Características:
        - Número de traders involucrados
        - Score promedio de traders
        - Volumen total de transacciones
        - Volumen promedio por trader
        - % de compras vs ventas
        - Velocidad de acumulación (transacciones/minuto)
        - Market cap
        - Volumen 1h
        - Crecimiento de volumen en 5m, 1h
        
        Args:
            token: Dirección del token
            dex_client: Instancia de DexScreenerClient
            scoring_system: Instancia de ScoringSystem
            
        Returns:
            dict: Diccionario con features o None si no hay suficientes datos
        """
        # Obtener transacciones para el token
        transactions = db.get_token_transactions(token, hours=24)
        if not transactions or len(transactions) < 3:
            return None
            
        # Calcular features básicas
        num_transactions = len(transactions)
        unique_wallets = set(tx["wallet"] for tx in transactions)
        num_traders = len(unique_wallets)
        
        # Calcular volumen total y promedio
        total_volume = sum(tx["amount_usd"] for tx in transactions)
        avg_volume_per_trader = total_volume / num_traders if num_traders > 0 else 0
        
        # Calcular % de compras vs ventas
        buys = [tx for tx in transactions if tx["type"] == "BUY"]
        sells = [tx for tx in transactions if tx["type"] == "SELL"]
        buy_ratio = len(buys) / num_transactions if num_transactions > 0 else 0
        
        # Calcular scores de traders
        trader_scores = []
        for wallet in unique_wallets:
            score = scoring_system.get_score(wallet)
            trader_scores.append(score)
        
        avg_trader_score = sum(trader_scores) / len(trader_scores) if trader_scores else 0
        max_trader_score = max(trader_scores) if trader_scores else 0
        
        # Calcular velocidad de transacciones
        timestamps = [datetime.fromisoformat(tx["created_at"]) for tx in transactions]
        if len(timestamps) >= 2:
            time_range = max(timestamps) - min(timestamps)
            time_range_minutes = time_range.total_seconds() / 60
            tx_velocity = num_transactions / time_range_minutes if time_range_minutes > 0 else 0
        else:
            tx_velocity = 0
            
        # Obtener datos de mercado
        dex_client.update_volume_history(token)
        vol_1h, market_cap, price = dex_client.fetch_token_data(token)
        vol_growth = dex_client.get_volume_growth(token)
        
        # Crear diccionario de features
        features = {
            "token": token,
            "num_traders": num_traders,
            "num_transactions": num_transactions,
            "total_volume_usd": total_volume,
            "avg_volume_per_trader": avg_volume_per_trader,
            "buy_ratio": buy_ratio,
            "tx_velocity": tx_velocity,
            "avg_trader_score": avg_trader_score,
            "max_trader_score": max_trader_score,
            "market_cap": market_cap,
            "volume_1h": vol_1h,
            "volume_growth_5m": vol_growth.get("growth_5m", 0),
            "volume_growth_1h": vol_growth.get("growth_1h", 0),
            "initial_price": price,
            "timestamp": datetime.now().isoformat()
        }
        
        # Guardar en cache
        self.features_cache[token] = features
        
        # Auto-guardar periódicamente
        if time.time() - self.last_save > self.auto_save_interval:
            self.save_features_to_csv()
            self.last_save = time.time()
        
        return features
    
    def save_features_to_csv(self, filename="ml_data/features.csv"):
        """
        Guarda todas las características en un archivo CSV para entrenar modelos.
        
        Args:
            filename: Ruta donde guardar el archivo CSV
            
        Returns:
            bool: True si se guardó correctamente, False si no
        """
        if not self.features_cache:
            print("⚠️ No hay características para guardar")
            return False
            
        try:
            df = pd.DataFrame(list(self.features_cache.values()))
            df.to_csv(filename, index=False)
            print(f"✅ Se guardaron {len(df)} registros de características en {filename}")
            return True
        except Exception as e:
            print(f"🚨 Error al guardar características: {e}")
            return False
    
    def add_outcome_data(self, token, price_increase_24h=None, volume_increase_24h=None, success=None):
        """
        Añade datos de resultado (outcome) para entrenar modelos supervisados.
        
        Estos datos se recopilan 24h después de la señal para saber si fue exitosa.
        
        Args:
            token: Dirección del token
            price_increase_24h: % de incremento de precio en 24h, o None
            volume_increase_24h: % de incremento de volumen en 24h, o None
            success: 1 si fue exitosa, 0 si no, o None para calcularlo automáticamente
            
        Returns:
            bool: True si se añadió correctamente, False si no
        """
        if token not in self.features_cache:
            print(f"⚠️ No se encontraron características para el token {token}")
            return False
            
        # Si 'success' no se proporciona, calcularlo basado en price_increase_24h
        if success is None and price_increase_24h is not None:
            success = 1 if price_increase_24h > 50 else 0
            
        # Guardar outcomes
        self.outcomes_cache[token] = {
            "token": token,
            "price_increase_24h": price_increase_24h,
            "volume_increase_24h": volume_increase_24h,
            "success": success,
            "timestamp": datetime.now().isoformat()
        }
        
        # Intentar guardar inmediatamente el outcome
        self.save_outcomes_to_csv()
        
        return True
    
    def save_outcomes_to_csv(self, filename="ml_data/outcomes.csv"):
        """
        Guarda los outcomes en un archivo CSV.
        
        Args:
            filename: Ruta donde guardar el archivo CSV
            
        Returns:
            bool: True si se guardó correctamente, False si no
        """
        if not self.outcomes_cache:
            print("⚠️ No hay outcomes para guardar")
            return False
            
        try:
            df = pd.DataFrame(list(self.outcomes_cache.values()))
            df.to_csv(filename, index=False)
            print(f"✅ Se guardaron {len(df)} registros de outcomes en {filename}")
            return True
        except Exception as e:
            print(f"🚨 Error al guardar outcomes: {e}")
            return False
    
    def prepare_training_data(self, features_file="ml_data/features.csv", outcomes_file="ml_data/outcomes.csv"):
        """
        Combina datos de características con resultados para crear dataset de entrenamiento.
        
        Args:
            features_file: Ruta al archivo CSV con características
            outcomes_file: Ruta al archivo CSV con outcomes
            
        Returns:
            DataFrame o None: DataFrame con datos de entrenamiento o None si hay error
        """
        try:
            # Verificar que existan ambos archivos
            if not os.path.exists(features_file) or not os.path.exists(outcomes_file):
                print("⚠️ No se encontraron archivos de features u outcomes")
                return None
                
            # Cargar datos
            features_df = pd.read_csv(features_file)
            outcomes_df = pd.read_csv(outcomes_file)
            
            # Verificar que haya datos
            if len(features_df) == 0 or len(outcomes_df) == 0:
                print("⚠️ No hay suficientes datos para crear dataset de entrenamiento")
                return None
            
            # Unir por token
            training_df = pd.merge(features_df, outcomes_df, on="token", how="inner")
            
            # Eliminar columnas innecesarias
            if "timestamp_x" in training_df.columns and "timestamp_y" in training_df.columns:
                training_df = training_df.drop(["timestamp_x", "timestamp_y"], axis=1)
            
            # Guardar dataset completo
            training_df.to_csv("ml_data/training_data.csv", index=False)
            
            print(f"✅ Dataset de entrenamiento creado con {len(training_df)} registros")
            return training_df
            
        except Exception as e:
            print(f"🚨 Error al preparar datos de entrenamiento: {e}")
            return None
            
    def collect_signal_outcomes(self, dex_client):
        """
        Recolecta outcomes para señales emitidas que aún no tienen resultados.
        Esta función se debe ejecutar periódicamente.
        
        Args:
            dex_client: Instancia de DexScreenerClient
            
        Returns:
            int: Número de outcomes recolectados
        """
        # Obtener señales sin outcomes
        signals = db.get_signals_without_outcomes(hours=48)
        count = 0
        
        for signal in signals:
            token = signal["token"]
            signal_time = signal["created_at"]
            signal_id = signal["id"]
            
            # Calcular cuánto tiempo ha pasado
            now = datetime.now()
            signal_datetime = datetime.fromisoformat(signal_time)
            hours_passed = (now - signal_datetime).total_seconds() / 3600
            
            # Solo procesar señales con más de 24 horas
            if hours_passed >= 24:
                # Obtener precio inicial y actual
                initial_price = signal.get("initial_price")
                current_price = dex_client.get_token_price(token)
                
                if initial_price and current_price and initial_price > 0:
                    # Calcular incremento de precio
                    price_increase = ((current_price - initial_price) / initial_price) * 100
                    
                    # Obtener volumen actual y calcular incremento
                    vol_1h, _, _ = dex_client.fetch_token_data(token)
                    # Aquí necesitaríamos el volumen inicial para hacer el cálculo real
                    volume_increase = 0  # Placeholder
                    
                    # Determinar si fue exitosa
                    success = 1 if price_increase > 50 else 0
                    
                    # Añadir outcome
                    self.add_outcome_data(
                        token=token,
                        price_increase_24h=price_increase,
                        volume_increase_24h=volume_increase,
                        success=success
                    )
                    
                    # Marcar como procesada
                    db.mark_signal_outcome_collected(signal_id)
                    count += 1
        
        if count > 0:
            print(f"✅ Se recolectaron outcomes para {count} señales")
            # Intentar preparar dataset de entrenamiento si hay nuevos datos
            self.prepare_training_data()
            
        return count
