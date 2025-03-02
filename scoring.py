import time
import db
from config import Config

class ScoringSystem:
    """
    Sistema de scoring para evaluar la calidad de los traders
    y calcular la confianza de las señales.
    """

    def __init__(self):
        """
        Inicializa el sistema de scoring.
        """
        # Cache local para evitar consultas repetidas a la BD
        self.local_cache = {}
        # Tiempo de la última limpieza de cache
        self.last_cache_cleanup = time.time()
        # Contador de transacciones por wallet
        self.wallet_tx_count = {}
        # Cache de boosters
        self.boosters = {}  # {wallet: {'active': bool, 'multiplier': float, 'expires': timestamp}}
        # Score de boosters por tipo de token
        self.token_type_scores = {}  # {token_type: multiplier}
        
        # Inicializar boosters de tipos de token
        self._init_token_type_boosters()
        
    def _init_token_type_boosters(self):
        """
        Inicializa boosters por tipo de token.
        """
        # Tokens conocidos con historiales de éxito tienen mayor boost
        self.token_type_scores = {
            "meme": 1.2,  # Tokens meme suelen tener más pump inicial
            "defi": 1.1,  # DeFi tokens
            "nft": 1.15,  # NFT relacionados
            "gaming": 1.1,  # Gaming tokens
            "ai": 1.2,     # AI tokens (tendencia actual)
            "new": 1.25    # Tokens nuevos (<24h)
        }

    def get_score(self, wallet):
        """
        Obtiene el score de un wallet, usando cache si está disponible.
        
        Args:
            wallet: Dirección de la wallet
            
        Returns:
            float: Score actual de la wallet
        """
        if wallet not in self.local_cache:
            self.local_cache[wallet] = db.get_wallet_score(wallet)
        
        # Aplicar booster si existe y está activo
        base_score = self.local_cache[wallet]
        if wallet in self.boosters and self.boosters[wallet]['active']:
            if time.time() < self.boosters[wallet]['expires']:
                return base_score * self.boosters[wallet]['multiplier']
            else:
                # Booster expirado
                self.boosters[wallet]['active'] = False
        
        return base_score

    def update_score_on_trade(self, wallet, tx_data):
        """
        Actualiza el score de un wallet basado en una transacción.
        
        Args:
            wallet: Dirección de la wallet
            tx_data: Diccionario con datos de la transacción
        """
        tx_type = tx_data.get("type", "")
        amount_usd = tx_data.get("amount_usd", 0)
        current_score = self.get_score(wallet)
        
        # Incrementar contador de transacciones
        if wallet not in self.wallet_tx_count:
            self.wallet_tx_count[wallet] = 0
        self.wallet_tx_count[wallet] += 1
        
        # Ajustar score según el tipo de transacción
        if tx_type == "BUY":
            # Incrementos mayores para transacciones grandes
            if amount_usd > 5000:
                score_increment = Config.BUY_SCORE_INCREASE * 2
            else:
                score_increment = Config.BUY_SCORE_INCREASE
                
            new_score = current_score + score_increment
            
        elif tx_type == "SELL":
            # Incrementos mayores para transacciones grandes
            if amount_usd > 5000:
                score_increment = Config.SELL_SCORE_INCREASE * 2
            else:
                score_increment = Config.SELL_SCORE_INCREASE
                
            new_score = current_score + score_increment
        else:
            return  # No cambiar score para otros tipos

        # Limitar entre MIN_SCORE y MAX_SCORE
        new_score = max(Config.MIN_SCORE, min(Config.MAX_SCORE, new_score))
        
        # Actualizar cache y BD
        self.local_cache[wallet] = new_score
        db.update_wallet_score(wallet, new_score)
        
        # Cada 50 transacciones por wallet, añadir un pequeño booster
        if self.wallet_tx_count[wallet] % 50 == 0:
            self.add_score_booster(wallet, 1.2, 86400)  # 20% boost por 24 horas
            print(f"🔥 ¡Booster de actividad para {wallet}! +20% por 24h")
        
        print(f"📊 Score de {wallet} actualizado: {current_score:.1f} → {new_score:.1f}")
        
        # Limpiar cache periódicamente
        if time.time() - self.last_cache_cleanup > 3600:  # Cada hora
            self.cleanup_cache()

    def compute_confidence(self, wallet_scores, volume_1h, market_cap, recent_volume_growth=0, token_type=None):
        """
        Calcula un nivel de confianza basado en múltiples factores:
        1. Calidad y diversidad de traders
        2. Volumen y market cap
        3. Crecimiento de volumen reciente
        4. Tipo de token
        
        Args:
            wallet_scores: Lista de scores de wallets
            volume_1h: Volumen en la última hora en USD
            market_cap: Market cap en USD
            recent_volume_growth: Crecimiento de volumen en porcentaje (ej: 0.05 = 5%)
            token_type: Tipo de token para aplicar boosters
            
        Returns:
            float: Nivel de confianza entre 0.0 y 1.0
        """
        if not wallet_scores:
            return 0.0

        # 1. Factor de calidad y diversidad de traders (0.0 - 1.0)
        avg_score = sum(wallet_scores) / len(wallet_scores)
        score_factor = avg_score / Config.MAX_SCORE
        
        # Factor de diversidad - valorar más carteras diversas
        unique_wallets = len(wallet_scores)
        wallet_diversity = min(unique_wallets / 10, 1.0)  # Máximo con 10 wallets
        
        # Calidad de wallets - dar más peso a carteras con traders de alta calidad
        high_quality_traders = sum(1 for score in wallet_scores if score > 8.0)
        quality_factor = min(high_quality_traders / 3, 1.0)  # Máximo con 3 traders de calidad
        
        # Combinar factores de wallet
        wallet_factor = score_factor * 0.5 + wallet_diversity * 0.3 + quality_factor * 0.2
        
        # 2. Factor de mercado (volumen y market cap)
        # Normalizar volumen, con máximo en Config.VOL_NORMALIZATION_FACTOR USD
        vol_factor = min(volume_1h / Config.VOL_NORMALIZATION_FACTOR, 1.0)
        
        # Penalizar market caps muy pequeños o muy grandes
        if market_cap < Config.MIN_MARKETCAP:
            mc_factor = 0.3  # Penalización para mcap muy bajo
        elif market_cap > Config.MAX_MARKETCAP:
            mc_factor = 0.5  # Penalización moderada para tokens grandes
        else:
            # Curva que favorece market caps medios
            normalized_mc = (market_cap - Config.MIN_MARKETCAP) / (Config.MAX_MARKETCAP - Config.MIN_MARKETCAP)
            mc_factor = 1.0 - 2.0 * (normalized_mc - 0.5) ** 2
        
        # Factor de crecimiento reciente
        growth_factor = min(recent_volume_growth / 0.2, 1.0)  # Máximo con 20% crecimiento
        
        # Combinar factores de mercado
        market_factor = vol_factor * 0.4 + mc_factor * 0.4 + growth_factor * 0.2
        
        # 3. Calcular confianza final - dar más peso a wallets
        weighted_score = wallet_factor * 0.6 + market_factor * 0.4
        
        # 4. Aplicar booster de tipo de token si existe
        if token_type and token_type.lower() in self.token_type_scores:
            weighted_score *= self.token_type_scores[token_type.lower()]
            print(f"🏷️ Booster aplicado para tipo {token_type}: x{self.token_type_scores[token_type.lower()]}")
        
        # Redondear a 3 decimales
        return round(min(weighted_score, 1.0), 3)

    def cleanup_cache(self, max_size=1000):
        """
        Limpia la cache si crece demasiado.
        
        Args:
            max_size: Tamaño máximo permitido de la caché
        """
        if len(self.local_cache) > max_size:
            # Conservar solo las 500 wallets más recientes
            self.local_cache = {}
            print(f"🧹 Cache de scores limpiada (superó {max_size} entradas)")
        
        # Limpiar boosters expirados
        now = time.time()
        expired_wallets = [wallet for wallet, data in self.boosters.items() 
                          if now > data['expires']]
        
        for wallet in expired_wallets:
            del self.boosters[wallet]
        
        if expired_wallets:
            print(f"🧹 Limpiados {len(expired_wallets)} boosters expirados")
        
        self.last_cache_cleanup = time.time()

    def add_score_booster(self, wallet, multiplier, duration_seconds):
        """
        Añade un multiplicador temporal al score de una wallet.
        
        Args:
            wallet: Dirección de la wallet
            multiplier: Multiplicador a aplicar (ej: 1.5 para +50%)
            duration_seconds: Duración del booster en segundos
        """
        if wallet not in self.boosters:
            self.boosters[wallet] = {
                'active': True,
                'multiplier': multiplier,
                'expires': time.time() + duration_seconds
            }
        else:
            # Si ya existe, extender duración y mantener el mayor multiplicador
            self.boosters[wallet]['active'] = True
            self.boosters[wallet]['multiplier'] = max(self.boosters[wallet]['multiplier'], multiplier)
            self.boosters[wallet]['expires'] = time.time() + duration_seconds
