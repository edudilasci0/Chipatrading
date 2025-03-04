import pandas as pd
import numpy as np
import json
import os
import db
import time
import logging
from datetime import datetime, timedelta
from config import Config

# Configurar logging
logger = logging.getLogger("ml_preparation")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class MLDataPreparation:
    """
    Clase optimizada para preparar datos para modelos de machine learning.
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
        os.makedirs("ml_data/history", exist_ok=True)  # Para backups históricos
    
    async def extract_signal_features(self, token, dex_client, scoring_system):
        """
        Extrae características (features) de una señal para su uso en modelos ML.
        Versión optimizada con features adicionales y normalización.
        
        Features:
        - Número de traders involucrados
        - Score promedio de traders
        - Volumen total de transacciones
        - Volumen promedio por trader
        - % de compras vs ventas
        - Velocidad de acumulación (transacciones/minuto)
        - Distribución de calidad de traders
        - Market cap y volúmenes normalizados
        - Varianza de scores de traders
        
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
            logger.warning(f"Datos insuficientes para token {token}: {len(transactions) if transactions else 0} transacciones")
            return None
            
        try:
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
            
            # Calcular velocidad de transacciones
            timestamps = [datetime.fromisoformat(tx["created_at"]) for tx in transactions]
            if len(timestamps) >= 2:
                time_range = max(timestamps) - min(timestamps)
                time_range_minutes = time_range.total_seconds() / 60
                tx_velocity = num_transactions / time_range_minutes if time_range_minutes > 0 else 0
            else:
                tx_velocity = 0
            
            # Calcular scores de traders y distribución
            trader_scores = []
            high_quality_count = 0
            elite_count = 0
            
            for wallet in unique_wallets:
                score = scoring_system.get_score(wallet)
                trader_scores.append(score)
                if score >= 7.0:  # Trader de alta calidad
                    high_quality_count += 1
                if score >= 9.0:  # Trader de élite
                    elite_count += 1
            
            # Calcular estadísticas de scores
            avg_trader_score = sum(trader_scores) / len(trader_scores) if trader_scores else 0
            max_trader_score = max(trader_scores) if trader_scores else 0
            min_trader_score = min(trader_scores) if trader_scores else 0
            
            # NUEVO: Calcular varianza de scores (indica diversidad o homogeneidad)
            score_variance = np.var(trader_scores) if trader_scores else 0
            
            # NUEVO: Ratio de calidad
            high_quality_ratio = high_quality_count / num_traders if num_traders > 0 else 0
            
            # NUEVO: Transacciones por trader (intensidad)
            tx_per_trader = num_transactions / num_traders if num_traders > 0 else 0
            
            # Obtener datos de mercado
            await dex_client.update_volume_history(token)
            vol_1h, market_cap, price = await dex_client.fetch_token_data(token)
            vol_growth = dex_client.get_volume_growth(token)
            
            # NUEVO: Features normalizados
            normalized_volume = min(vol_1h / 50000, 1.0)  # Normalizado a 50K
            normalized_mcap = min(market_cap / 10000000, 1.0)  # Normalizado a 10M
            
            # Crear diccionario de features
            features = {
                "token": token,
                "num_traders": num_traders,
                "num_transactions": num_transactions,
                "total_volume_usd": total_volume,
                "avg_volume_per_trader": avg_volume_per_trader,
                "buy_ratio": buy_ratio,
                "tx_velocity": tx_velocity,
                "tx_per_trader": tx_per_trader,
                "avg_trader_score": avg_trader_score,
                "max_trader_score": max_trader_score,
                "min_trader_score": min_trader_score,
                "score_variance": score_variance,
                "high_quality_count": high_quality_count,
                "elite_trader_count": elite_count,
                "high_quality_ratio": high_quality_ratio,
                "market_cap": market_cap,
                "normalized_mcap": normalized_mcap,
                "volume_1h": vol_1h,
                "normalized_volume": normalized_volume,
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
            
        except Exception as e:
            logger.error(f"Error al extraer features para {token}: {e}", exc_info=True)
            return None
    
    def save_features_to_csv(self, filename="ml_data/features.csv"):
        """
        Guarda todas las características en un archivo CSV para entrenar modelos.
        
        Args:
            filename: Ruta donde guardar el archivo CSV
            
        Returns:
            bool: True si se guardó correctamente, False si no
        """
        if not self.features_cache:
            logger.warning("No hay características para guardar")
            return False
            
        try:
            df = pd.DataFrame(list(self.features_cache.values()))
            df.to_csv(filename, index=False)
            
            # NUEVO: Guardar también un backup con timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"ml_data/history/features_{timestamp}.csv"
            df.to_csv(backup_file, index=False)
            
            logger.info(f"✅ Se guardaron {len(df)} registros de características en {filename}")
            logger.info(f"✅ Backup guardado en {backup_file}")
            return True
        except Exception as e:
            logger.error(f"🚨 Error al guardar características: {e}", exc_info=True)
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
            logger.warning(f"No se encontraron características para el token {token}")
            return False
            
        # Si 'success' no se proporciona, calcularlo basado en price_increase_24h
        if success is None and price_increase_24h is not None:
            success = 1 if price_increase_24h > 50 else 0
            
        # NUEVO: Categorización más detallada del resultado
        outcome_category = "unknown"
        if price_increase_24h is not None:
            if price_increase_24h > 100:
                outcome_category = "explosive_growth"
            elif price_increase_24h > 50:
                outcome_category = "strong_growth"
            elif price_increase_24h > 20:
                outcome_category = "moderate_growth"
            elif price_increase_24h > 0:
                outcome_category = "slight_growth"
            elif price_increase_24h > -20:
                outcome_category = "slight_decline"
            else:
                outcome_category = "strong_decline"
        
        # Guardar outcomes
        self.outcomes_cache[token] = {
            "token": token,
            "price_increase_24h": price_increase_24h,
            "volume_increase_24h": volume_increase_24h,
            "success": success,
            "outcome_category": outcome_category,
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
            logger.warning("No hay outcomes para guardar")
            return False
            
        try:
            df = pd.DataFrame(list(self.outcomes_cache.values()))
            df.to_csv(filename, index=False)
            
            # NUEVO: Guardar también un backup con timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"ml_data/history/outcomes_{timestamp}.csv"
            df.to_csv(backup_file, index=False)
            
            logger.info(f"✅ Se guardaron {len(df)} registros de outcomes en {filename}")
            logger.info(f"✅ Backup guardado en {backup_file}")
            return True
        except Exception as e:
            logger.error(f"🚨 Error al guardar outcomes: {e}", exc_info=True)
            return False
    
    def prepare_training_data(self, features_file="ml_data/features.csv", outcomes_file="ml_data/outcomes.csv"):
        """
        Combina datos de características con resultados para crear dataset de entrenamiento.
        Versión optimizada con manejo de datos faltantes y balanceo.
        
        Args:
            features_file: Ruta al archivo CSV con características
            outcomes_file: Ruta al archivo CSV con outcomes
            
        Returns:
            DataFrame o None: DataFrame con datos de entrenamiento o None si hay error
        """
        try:
            # Verificar que existan ambos archivos
            if not os.path.exists(features_file) or not os.path.exists(outcomes_file):
                logger.warning("No se encontraron archivos de features u outcomes")
                return None
                
            # Cargar datos
            features_df = pd.read_csv(features_file)
            outcomes_df = pd.read_csv(outcomes_file)
            
            # Verificar que haya datos
            if len(features_df) == 0 or len(outcomes_df) == 0:
                logger.warning("No hay suficientes datos para crear dataset de entrenamiento")
                return None
            
            # Limpiar y preparar datos
            # NUEVO: Manejo de valores faltantes
            features_df = features_df.fillna(0)
            outcomes_df = outcomes_df.fillna(0)
            
            # Unir por token
            training_df = pd.merge(features_df, outcomes_df, on="token", how="inner")
            
            # NUEVO: Verificar si hay suficientes ejemplos positivos
            success_count = training_df[training_df['success'] == 1].shape[0]
            total_count = training_df.shape[0]
            
            logger.info(f"Dataset combinado: {total_count} ejemplos, {success_count} éxitos ({success_count/total_count:.1%})")
            
            if success_count < 5 and total_count > 20:
                logger.warning(f"Pocos ejemplos positivos ({success_count}). Considera ajustar el umbral de éxito.")
            
            # Eliminar columnas innecesarias
            if "timestamp_x" in training_df.columns and "timestamp_y" in training_df.columns:
                training_df = training_df.drop(["timestamp_x", "timestamp_y"], axis=1)
            
            # NUEVO: Crear versión enriquecida con variables adicionales
            try:
                # Crear features derivados
                training_df['price_to_volume_ratio'] = training_df['initial_price'] / training_df['volume_1h'].replace(0, 0.001)
                training_df['quality_to_quantity_ratio'] = training_df['avg_trader_score'] / training_df['num_traders'].replace(0, 0.001)
                
                # Normalizar valores extremos - aplicar log a valores muy grandes
                for col in ['market_cap', 'volume_1h', 'total_volume_usd']:
                    if col in training_df.columns:
                        training_df[f'{col}_log'] = np.log1p(training_df[col])
                
                logger.info(f"✅ Features derivados creados exitosamente")
            except Exception as e:
                logger.warning(f"No se pudieron crear features derivados: {e}")
            
            # Guardar dataset completo
            training_file = "ml_data/training_data.csv"
            training_df.to_csv(training_file, index=False)
            
            # NUEVO: Guardar también un backup con timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"ml_data/history/training_data_{timestamp}.csv"
            training_df.to_csv(backup_file, index=False)
            
            logger.info(f"✅ Dataset de entrenamiento creado con {len(training_df)} registros")
            logger.info(f"✅ Guardado en {training_file} y {backup_file}")
            
            return training_df
            
        except Exception as e:
            logger.error(f"🚨 Error al preparar datos de entrenamiento: {e}", exc_info=True)
            return None
            
    async def collect_signal_outcomes(self, dex_client):
        """
        Recolecta outcomes para señales emitidas que aún no tienen resultados.
        Esta función se debe ejecutar periódicamente.
        Versión optimizada con más métricas y análisis detallado.
        
        Args:
            dex_client: Instancia de DexScreenerClient
            
        Returns:
            int: Número de outcomes recolectados
        """
        # Obtener señales sin outcomes
        signals = db.get_signals_without_outcomes(hours=48)
        count = 0
        
        # Contadores para estadísticas
        outcome_categories = {
            "explosive_growth": 0,  # >100%
            "strong_growth": 0,     # 50-100%
            "moderate_growth": 0,   # 20-50%
            "slight_growth": 0,     # 0-20%
            "slight_decline": 0,    # 0 to -20%
            "strong_decline": 0     # <-20%
        }
        
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
                try:
                    # Obtener precio inicial y actual
                    initial_price = signal.get("initial_price")
                    current_price = await dex_client.get_token_price(token)
                    
                    if initial_price and current_price and initial_price > 0:
                        # Calcular incremento de precio
                        price_increase = ((current_price - initial_price) / initial_price) * 100
                        
                        # Obtener volumen actual y calcular incremento
                        vol_1h, _, _ = await dex_client.fetch_token_data(token)
                        # Intentar obtener volumen inicial desde features
                        initial_vol = None
                        if token in self.features_cache:
                            initial_vol = self.features_cache[token].get("volume_1h")
                        
                        volume_increase = 0
                        if initial_vol and initial_vol > 0:
                            volume_increase = ((vol_1h - initial_vol) / initial_vol) * 100
                        
                        # Determinar si fue exitosa
                        success = 1 if price_increase > 50 else 0
                        
                        # Determinar categoría de outcome
                        outcome_category = "unknown"
                        if price_increase > 100:
                            outcome_category = "explosive_growth"
                        elif price_increase > 50:
                            outcome_category = "strong_growth"
                        elif price_increase > 20:
                            outcome_category = "moderate_growth"
                        elif price_increase > 0:
                            outcome_category = "slight_growth"
                        elif price_increase > -20:
                            outcome_category = "slight_decline"
                        else:
                            outcome_category = "strong_decline"
                        
                        # Actualizar contador de categorías
                        if outcome_category in outcome_categories:
                            outcome_categories[outcome_category] += 1
                        
                        # Añadir outcome
                        self.add_outcome_data(
                            token=token,
                            price_increase_24h=price_increase,
                            volume_increase_24h=volume_increase,
                            success=success
                        )
                        
                        # NUEVO: Log detallado para análisis
                        logger.info(f"📊 Outcome para {token}: {price_increase:.1f}% precio, "
                                   f"{volume_increase:.1f}% volumen, categoría: {outcome_category}")
                        
                        # Marcar como procesada
                        db.mark_signal_outcome_collected(signal_id)
                        count += 1
                except Exception as e:
                    logger.error(f"Error procesando outcome para {token}: {e}")
        
        if count > 0:
            logger.info(f"✅ Se recolectaron outcomes para {count} señales")
            
            # Resumen estadístico
            logger.info("📈 Distribución de resultados:")
            for category, cat_count in outcome_categories.items():
                if cat_count > 0:
                    percent = (cat_count / count) * 100
                    logger.info(f"  • {category}: {cat_count} ({percent:.1f}%)")
            
            # Intentar preparar dataset de entrenamiento si hay nuevos datos
            self.prepare_training_data()
            
        return count
    
    def analyze_feature_correlations(self):
        """
        NUEVO: Analiza correlaciones entre features y outcomes para insight.
        
        Returns:
            DataFrame o None: Correlaciones ordenadas por relevancia
        """
        try:
            training_file = "ml_data/training_data.csv"
            if not os.path.exists(training_file):
                logger.warning(f"No se encuentra el archivo de training: {training_file}")
                return None
                
            # Cargar datos
            df = pd.read_csv(training_file)
            
            # Verificar que tenemos la columna target
            if 'success' not in df.columns:
                logger.warning("No se encontró la columna 'success' en los datos")
                return None
                
            # Calcular correlaciones con el outcome
            numeric_cols = df.select_dtypes(include=['number']).columns
            correlations = {}
            
            for col in numeric_cols:
                if col != 'success' and col != 'token':
                    correlation = df[col].corr(df['success'])
                    if not pd.isna(correlation):
                        correlations[col] = correlation
            
            # Convertir a DataFrame y ordenar
            corr_df = pd.DataFrame({
                'feature': list(correlations.keys()),
                'correlation': list(correlations.values())
            })
            
            corr_df = corr_df.sort_values('correlation', ascending=False)
            
            # Generar insights
            logger.info("🔍 Top features correlacionados con éxito:")
            for i, row in corr_df.head(5).iterrows():
                logger.info(f"  • {row['feature']}: {row['correlation']:.4f}")
                
            logger.info("🔍 Features menos correlacionados con éxito:")
            for i, row in corr_df.tail(5).iterrows():
                logger.info(f"  • {row['feature']}: {row['correlation']:.4f}")
                
            return corr_df
            
        except Exception as e:
            logger.error(f"Error analizando correlaciones: {e}", exc_info=True)
            return None
    
    def clean_old_data(self, days=90):
        """
        NUEVO: Limpia datos antiguos para mantener los datasets limpios.
        
        Args:
            days: Número de días para mantener datos
            
        Returns:
            int: Número de registros eliminados
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            removed = 0
            
            # Limpiar features cache
            tokens_to_remove = []
            for token, data in self.features_cache.items():
                if 'timestamp' in data:
                    data_date = datetime.fromisoformat(data['timestamp'])
                    if data_date < cutoff_date:
                        tokens_to_remove.append(token)
            
            for token in tokens_to_remove:
                del self.features_cache[token]
                removed += 1
            
            # Limpiar outcomes cache
            tokens_to_remove = []
            for token, data in self.outcomes_cache.items():
                if 'timestamp' in data:
                    data_date = datetime.fromisoformat(data['timestamp'])
                    if data_date < cutoff_date:
                        tokens_to_remove.append(token)
            
            for token in tokens_to_remove:
                del self.outcomes_cache[token]
                removed += 1
            
            # Guardar versiones limpias
            if removed > 0:
                self.save_features_to_csv()
                self.save_outcomes_to_csv()
                logger.info(f"🧹 Se eliminaron {removed} registros antiguos (>{days} días)")
            
            return removed
            
        except Exception as e:
            logger.error(f"Error limpiando datos antiguos: {e}")
            return 0

