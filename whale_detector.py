# whale_detector.py
import logging
import time
from datetime import datetime, timedelta
import asyncio
from config import Config
import db

logger = logging.getLogger("whale_detector")

class WhaleDetector:
    """
    Módulo para detectar y analizar la actividad de ballenas (whales) en tokens de Solana.
    - Detecta grandes transacciones relativas al market cap
    - Identifica carteras conocidas de ballenas
    - Analiza el impacto de transacciones grandes en el precio
    """
    
    def __init__(self, helius_client=None, token_data_service=None):
        self.helius_client = helius_client
        self.token_data_service = token_data_service
        self.known_whales = set()
        self.whale_transactions = {}  # {token: [transactions]}
        self.market_cap_cache = {}    # {token: {"market_cap": value, "timestamp": time}}
        self.cache_ttl = int(Config.get("MARKET_CAP_CACHE_TTL", 300))  # 5 minutos por defecto
        self.whale_score_threshold = float(Config.get("WHALE_SCORE_THRESHOLD", 8.5))
        self.whale_transaction_history = {}  # {token: {"last_24h": [], "last_hour": []}}
        self._load_known_whales()
        
    def _load_known_whales(self):
        """Carga la lista de ballenas conocidas desde la base de datos o configuración"""
        try:
            # Intentar cargar desde BD primero
            whale_addresses = db.execute_cached_query(
                "SELECT wallet FROM wallet_scores WHERE score > %s", 
                (self.whale_score_threshold,),
                max_age=3600
            )
            
            if whale_addresses:
                self.known_whales = set(row['wallet'] for row in whale_addresses)
                logger.info(f"Cargadas {len(self.known_whales)} ballenas conocidas desde la BD")
            else:
                # Fallback a la lista hardcodeada de wallets en traders_data.json
                from wallet_tracker import WalletTracker
                wallet_tracker = WalletTracker()
                self.known_whales = set(wallet_tracker.get_wallets())
                logger.info(f"Cargadas {len(self.known_whales)} wallets desde traders_data.json como potenciales ballenas")
        except Exception as e:
            logger.error(f"Error cargando ballenas conocidas: {e}")
            self.known_whales = set()
    
    def is_known_whale(self, wallet_address):
        """Verifica si una dirección pertenece a una ballena conocida"""
        return wallet_address in self.known_whales
    
    async def get_market_cap(self, token):
        """Obtiene el market cap de un token con caché para evitar consultas repetidas"""
        now = time.time()
        
        # Verificar caché
        if token in self.market_cap_cache:
            cache_entry = self.market_cap_cache[token]
            if now - cache_entry["timestamp"] < self.cache_ttl:
                return cache_entry["market_cap"]
        
        # Consultar market cap
        market_cap = 0
        if self.token_data_service:
            try:
                if hasattr(self.token_data_service, 'get_token_data_async'):
                    token_data = await self.token_data_service.get_token_data_async(token)
                    if token_data and 'market_cap' in token_data:
                        market_cap = token_data['market_cap']
                elif hasattr(self.token_data_service, 'get_token_data'):
                    token_data = self.token_data_service.get_token_data(token)
                    if token_data and 'market_cap' in token_data:
                        market_cap = token_data['market_cap']
            except Exception as e:
                logger.error(f"Error obteniendo market cap para {token}: {e}")
        
        # Actualizar caché
        self.market_cap_cache[token] = {
            "market_cap": market_cap,
            "timestamp": now
        }
        
        return market_cap
    
    def detect_large_transactions(self, token, recent_transactions, market_cap=None):
        """
        Detecta transacciones de ballenas basadas en su tamaño relativo al market cap.
        
        Args:
            token: Dirección del token
            recent_transactions: Lista de transacciones recientes
            market_cap: Market cap del token (opcional, se consultará si no se proporciona)
            
        Returns:
            dict: Información sobre actividad de ballenas incluyendo:
                - has_whale_activity (bool): Si se detectó actividad de ballenas
                - whale_transactions (list): Lista de transacciones de ballenas
                - impact_score (float): Puntuación de impacto (0-1)
                - whale_volume_percent (float): Porcentaje del volumen de las ballenas
        """
        if not recent_transactions:
            return {
                "has_whale_activity": False,
                "whale_transactions": [],
                "impact_score": 0,
                "whale_volume_percent": 0
            }
        
        # Inicializar resultado
        result = {
            "has_whale_activity": False,
            "whale_transactions": [],
            "impact_score": 0,
            "whale_volume_percent": 0,
            "known_whales_count": 0
        }
        
        # Calcular volumen total de las transacciones
        total_volume = sum(tx.get("amount_usd", 0) for tx in recent_transactions)
        
        # Si el market cap no está disponible, usar volumen como aproximación
        threshold_base = market_cap if market_cap and market_cap > 0 else total_volume * 10
        
        # Umbral dinámico basado en el market cap/volumen
        if threshold_base < 100000:  # Microcap (< $100k)
            threshold_percentage = 0.03  # 3% para microcaps
        elif threshold_base < 1000000:  # Small cap ($100k-$1M)
            threshold_percentage = 0.02  # 2% para small caps
        elif threshold_base < 10000000:  # Mid cap ($1M-$10M)
            threshold_percentage = 0.01  # 1% para mid caps
        else:  # Large cap (>$10M)
            threshold_percentage = 0.005  # 0.5% para large caps
        
        threshold_usd = threshold_base * threshold_percentage
        whale_threshold = max(threshold_usd, float(Config.get("MIN_WHALE_TRANSACTION_USD", 5000)))
        
        # Detectar transacciones de ballenas
        whale_volume = 0
        known_whales_set = set()
        
        for tx in recent_transactions:
            amount_usd = tx.get("amount_usd", 0)
            wallet = tx.get("wallet")
            
            is_whale_tx = amount_usd >= whale_threshold
            is_from_known_whale = wallet and self.is_known_whale(wallet)
            
            if is_whale_tx or is_from_known_whale:
                result["whale_transactions"].append(tx)
                whale_volume += amount_usd
                
                if is_from_known_whale and wallet:
                    known_whales_set.add(wallet)
        
        # Actualizar estadísticas
        result["has_whale_activity"] = len(result["whale_transactions"]) > 0
        result["known_whales_count"] = len(known_whales_set)
        
        # Calcular métricas de impacto
        if total_volume > 0:
            result["whale_volume_percent"] = whale_volume / total_volume
            
            # Calcular score de impacto (0-1)
            whale_count_factor = min(len(result["whale_transactions"]) / 5, 1)
            volume_factor = min(result["whale_volume_percent"] * 2, 1)
            known_whales_factor = min(len(known_whales_set) / 3, 1)
            
            # Combinar factores para el score final
            result["impact_score"] = (
                whale_count_factor * 0.3 + 
                volume_factor * 0.4 + 
                known_whales_factor * 0.3
            )
        
        # Guardar historial
        if token not in self.whale_transaction_history:
            self.whale_transaction_history[token] = {"last_24h": [], "last_hour": []}
            
        # Actualizar historial
        now = time.time()
        for tx in result["whale_transactions"]:
            tx_with_time = tx.copy()
            tx_with_time["detected_at"] = now
            
            self.whale_transaction_history[token]["last_24h"].append(tx_with_time)
            self.whale_transaction_history[token]["last_hour"].append(tx_with_time)
        
        # Limpiar transacciones antiguas
        hour_ago = now - 3600
        day_ago = now - 86400
        
        self.whale_transaction_history[token]["last_hour"] = [
            tx for tx in self.whale_transaction_history[token]["last_hour"]
            if tx.get("detected_at", 0) >= hour_ago
        ]
        
        self.whale_transaction_history[token]["last_24h"] = [
            tx for tx in self.whale_transaction_history[token]["last_24h"]
            if tx.get("detected_at", 0) >= day_ago
        ]
        
        return result
    
    async def analyze_transaction_impact(self, token, transaction, market_data=None):
        """
        Analiza el impacto potencial de una transacción grande en el token.
        
        Args:
            token: Dirección del token
            transaction: Datos de la transacción
            market_data: Datos de mercado (opcional)
            
        Returns:
            dict: Análisis de impacto
        """
        impact_analysis = {
            "significance": 0,  # 0-1 score de significancia
            "price_impact_estimate": 0,  # Estimación del impacto en precio %
            "is_significant": False
        }
        
        try:
            # Obtener market_data si no se proporcionó
            if not market_data and self.token_data_service:
                if hasattr(self.token_data_service, 'get_token_data_async'):
                    market_data = await self.token_data_service.get_token_data_async(token)
                elif hasattr(self.token_data_service, 'get_token_data'):
                    market_data = self.token_data_service.get_token_data(token)
            
            if not market_data:
                return impact_analysis
            
            amount_usd = transaction.get("amount_usd", 0)
            tx_type = transaction.get("type", "").upper()
            
            # Obtener datos clave del mercado
            market_cap = market_data.get("market_cap", 0)
            volume_1h = market_data.get("volume", 0)
            
            if market_cap <= 0 or volume_1h <= 0:
                return impact_analysis
            
            # Calcular impacto relativo al volumen de 1 hora
            volume_impact = amount_usd / max(volume_1h, 1)
            
            # Calcular impacto relativo al market cap
            market_impact = amount_usd / max(market_cap, 1)
            
            # Usar liquidez como factor de impacto estimado en precio
            # Mayor liquidez = menor impacto
            liquidity_factor = 1
            if "liquidity" in market_data and market_data["liquidity"] > 0:
                # Normalizar la liquidez (mayor liquidez -> menor impacto)
                liquidity_factor = min(1, 100000 / max(market_data["liquidity"], 1000))
            
            # Calcular impacto estimado en precio basado en la dirección de la transacción
            price_impact_direction = 1 if tx_type == "BUY" else -1 if tx_type == "SELL" else 0
            
            # Modelo simplificado de impacto en precio:
            # - Mayor volumen relativo -> mayor impacto
            # - Menor liquidez -> mayor impacto
            base_price_impact = volume_impact * liquidity_factor * 100  # en porcentaje
            
            # Limitar el impacto máximo estimado (típicamente incluso grandes órdenes tienen impacto limitado)
            price_impact = min(base_price_impact, 15) * price_impact_direction
            
            # Calcular significancia general
            significance = (
                min(volume_impact * 5, 1) * 0.5 +  # Impacto en volumen (50%)
                min(market_impact * 200, 1) * 0.3 + # Impacto en market cap (30%)
                (1 if self.is_known_whale(transaction.get("wallet", "")) else 0) * 0.2  # Bonus si es ballena conocida (20%)
            )
            
            impact_analysis = {
                "significance": significance,
                "price_impact_estimate": price_impact,
                "is_significant": significance > 0.5,
                "volume_impact": volume_impact,
                "market_cap_impact": market_impact
            }
            
        except Exception as e:
            logger.error(f"Error en analyze_transaction_impact para {token}: {e}")
        
        return impact_analysis
    
    def get_whale_activity_report(self, token, timeframe="1h"):
        """
        Genera un informe resumido de la actividad de ballenas para un token.
        
        Args:
            token: Dirección del token
            timeframe: Intervalo de tiempo ('1h' o '24h')
            
        Returns:
            dict: Informe de actividad de ballenas
        """
        if token not in self.whale_transaction_history:
            return {
                "whale_transactions_count": 0,
                "total_whale_volume": 0,
                "unique_whales": 0,
                "has_significant_activity": False
            }
        
        key = "last_hour" if timeframe == "1h" else "last_24h"
        transactions = self.whale_transaction_history[token][key]
        
        total_volume = sum(tx.get("amount_usd", 0) for tx in transactions)
        unique_wallets = set(tx.get("wallet") for tx in transactions if tx.get("wallet"))
        
        return {
            "whale_transactions_count": len(transactions),
            "total_whale_volume": total_volume,
            "unique_whales": len(unique_wallets),
            "has_significant_activity": len(transactions) >= 2 and total_volume > 10000
        }
    
    def cleanup_old_data(self):
        """Limpia datos antiguos para liberar memoria"""
        now = time.time()
        day_ago = now - 86400
        
        tokens_to_remove = []
        for token, history in self.whale_transaction_history.items():
            # Mantener solo tokens con actividad reciente
            if all(tx.get("detected_at", 0) < day_ago for tx in history["last_24h"]):
                tokens_to_remove.append(token)
        
        for token in tokens_to_remove:
            del self.whale_transaction_history[token]
        
        # También limpiar caché de market cap antigua
        mc_tokens_to_remove = []
        for token, cache_entry in self.market_cap_cache.items():
            if now - cache_entry["timestamp"] > self.cache_ttl * 2:
                mc_tokens_to_remove.append(token)
                
        for token in mc_tokens_to_remove:
            del self.market_cap_cache[token]
