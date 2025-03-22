# db.py - Archivo completo para el bot de trading en Solana
import os
import time
import psycopg2
import psycopg2.pool
import psycopg2.extras
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta
from config import Config
import threading
import random
import json

logger = logging.getLogger("database")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Pool de conexiones
pool = None
pool_lock = threading.Lock()

# Cache para consultas frecuentes
query_cache = {}
query_cache_timestamp = {}
query_cache_hits = 0
query_cache_misses = 0

def init_db_pool(min_conn=1, max_conn=10):
    global pool
    with pool_lock:
        if pool is None:
            db_url = Config.DATABASE_PATH
            if not db_url:
                raise ValueError("DATABASE_PATH no está configurado")
            pool = psycopg2.pool.SimpleConnectionPool(min_conn, max_conn, db_url)
            logger.info(f"✅ Pool de conexiones a base de datos inicializado (min={min_conn}, max={max_conn})")

@contextmanager
def get_connection():
    global pool
    if pool is None:
        init_db_pool()
    conn = None
    try:
        conn = pool.getconn()
        yield conn
    except psycopg2.OperationalError as e:
        logger.error(f"Error de conexión: {e}. Intentando reconectar...")
        if conn:
            try:
                pool.putconn(conn, close=True)
            except:
                pass
        init_db_pool()
        conn = pool.getconn()
        yield conn
    finally:
        if conn is not None:
            pool.putconn(conn)

def retry_db_operation(max_attempts=3, delay=1, backoff_factor=2):
    def decorator(func):
        def wrapper(*args, **kwargs):
            global query_cache, query_cache_timestamp, query_cache_hits, query_cache_misses
            last_error = None
            current_delay = delay
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    last_error = e
                    if attempt == max_attempts - 1:
                        raise
                    jitter = random.uniform(0.8, 1.2)
                    wait_time = current_delay * jitter
                    logger.warning(f"⚠️ Error de BD: {e}. Reintentando ({attempt+1}/{max_attempts}) en {wait_time:.2f}s...")
                    time.sleep(wait_time)
                    current_delay *= backoff_factor
                except Exception as e:
                    logger.error(f"Error no manejado: {e}")
                    raise
            raise last_error
        return wrapper
    return decorator

@retry_db_operation()
def init_db():
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            # Tabla para versiones de schema/migraciones
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMP DEFAULT NOW(),
                    description TEXT
                )
            """)
            cur.execute("SELECT MAX(version) FROM schema_version")
            result = cur.fetchone()
            current_version = result[0] if result and result[0] else 0
            logger.info(f"Versión actual del schema: {current_version}")
            
            # Migración #1: Tablas iniciales
            if current_version < 1:
                logger.info("Aplicando migración #1: Tablas iniciales")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS wallet_scores (
                        wallet TEXT PRIMARY KEY,
                        score NUMERIC,
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS transactions (
                        id SERIAL PRIMARY KEY,
                        wallet TEXT,
                        token TEXT,
                        tx_type TEXT,
                        amount_usd NUMERIC,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS signals (
                        id SERIAL PRIMARY KEY,
                        token TEXT,
                        trader_count INTEGER,
                        confidence NUMERIC,
                        initial_price NUMERIC,
                        created_at TIMESTAMP DEFAULT NOW(),
                        outcome_collected BOOLEAN DEFAULT FALSE
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS signal_performance (
                        id SERIAL PRIMARY KEY,
                        token TEXT,
                        signal_id INTEGER REFERENCES signals(id),
                        timeframe TEXT CHECK (timeframe IN ('3m', '5m', '10m', '30m', '1h', '2h', '4h', '24h')),
                        percent_change NUMERIC,
                        confidence NUMERIC,
                        traders_count INTEGER,
                        extra_data JSONB,
                        timestamp TIMESTAMP DEFAULT NOW(),
                        UNIQUE(token, timeframe)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_settings (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS failed_tokens (
                        token TEXT PRIMARY KEY,
                        reason TEXT,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    INSERT INTO schema_version (version, description)
                    VALUES (1, 'Tablas iniciales')
                """)
                current_version = 1
                logger.info("Migración #1 aplicada correctamente")
            
            # Migración #2: Mejoras y nuevas tablas
            if current_version < 2:
                logger.info("Aplicando migración #2: Mejoras y nuevas tablas")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS wallet_profits (
                        id SERIAL PRIMARY KEY,
                        wallet TEXT,
                        token TEXT,
                        buy_price NUMERIC,
                        sell_price NUMERIC,
                        profit_percent NUMERIC,
                        hold_time_hours NUMERIC,
                        buy_timestamp TIMESTAMP,
                        sell_timestamp TIMESTAMP DEFAULT NOW(),
                        UNIQUE(wallet, token, buy_timestamp)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS signal_features (
                        id SERIAL PRIMARY KEY,
                        signal_id INTEGER REFERENCES signals(id),
                        token TEXT,
                        feature_json JSONB,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS token_metadata (
                        token TEXT PRIMARY KEY,
                        token_type TEXT,
                        volatility NUMERIC,
                        max_price NUMERIC,
                        max_volume NUMERIC,
                        first_seen TIMESTAMP DEFAULT NOW(),
                        last_updated TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    INSERT INTO schema_version (version, description)
                    VALUES (2, 'Mejoras y nuevas tablas')
                """)
                current_version = 2
                logger.info("Migración #2 aplicada correctamente")
            
            # Migración #3: Nuevas tablas para análisis avanzado
            if current_version < 3:
                logger.info("Aplicando migración #3: Tablas para análisis avanzado")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS token_liquidity (
                        id SERIAL PRIMARY KEY,
                        token TEXT,
                        total_liquidity_usd NUMERIC,
                        volume_24h NUMERIC,
                        slippage_1k NUMERIC,
                        slippage_10k NUMERIC,
                        dex_sources TEXT[],
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS whale_activity (
                        id SERIAL PRIMARY KEY,
                        token TEXT,
                        transaction_hash TEXT,
                        wallet TEXT,
                        amount_usd NUMERIC,
                        tx_type TEXT,
                        impact_score NUMERIC,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS holder_growth (
                        id SERIAL PRIMARY KEY,
                        token TEXT,
                        holder_count INTEGER,
                        growth_rate_1h NUMERIC,
                        growth_rate_24h NUMERIC,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trader_profiles (
                        wallet TEXT PRIMARY KEY,
                        profile_data JSONB,
                        quality_score NUMERIC,
                        specialty TEXT,
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trader_patterns (
                        id SERIAL PRIMARY KEY,
                        token TEXT,
                        wallets TEXT[],
                        coordination_score NUMERIC,
                        pattern_type TEXT,
                        detected_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS token_analysis (
                        id SERIAL PRIMARY KEY,
                        token TEXT,
                        volume_trend TEXT,
                        price_trend TEXT,
                        volatility NUMERIC,
                        rsi NUMERIC,
                        pattern_quality NUMERIC,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trending_tokens (
                        id SERIAL PRIMARY KEY,
                        token TEXT,
                        platforms TEXT[],
                        discovery_potential NUMERIC,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    ALTER TABLE signal_performance
                    ADD COLUMN IF NOT EXISTS extra_data JSONB
                """)
                cur.execute("""
                    INSERT INTO schema_version (version, description)
                    VALUES (3, 'Tablas para análisis avanzado')
                """)
                current_version = 3
                logger.info("Migración #3 aplicada correctamente")
            
            # Crear índices para mejorar el rendimiento
            try:
                cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_token ON transactions(token)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(wallet)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_token ON signals(token)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_wallet_token ON transactions(wallet, token)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_profits_wallet ON wallet_profits(wallet)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_features_token ON signal_features(token)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_features_signal_id ON signal_features(signal_id)")
                if current_version >= 3:
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_token_liquidity_token ON token_liquidity(token)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_whale_activity_token ON whale_activity(token)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_whale_activity_wallet ON whale_activity(wallet)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_holder_growth_token ON holder_growth(token)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_trader_patterns_token ON trader_patterns(token)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_token_analysis_token ON token_analysis(token)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_trending_tokens_token ON trending_tokens(token)")
                logger.info("✅ Índices creados correctamente")
            except Exception as e:
                logger.error(f"⚠️ Error al crear índices: {e}")
                conn.rollback()
            
            # Insertar configuraciones iniciales si no existen
            default_settings = [
                ("min_transaction_usd", str(Config.get("MIN_TRANSACTION_USD", 200))),
                ("min_traders_for_signal", str(Config.get("MIN_TRADERS_FOR_SIGNAL", 2))),
                ("signal_window_seconds", "540"),
                ("min_confidence_threshold", str(Config.get("MIN_CONFIDENCE_THRESHOLD", 0.3))),
                ("rugcheck_min_score", "50"),
                ("min_volume_usd", str(Config.get("MIN_VOLUME_USD", 2000))),
                ("signal_throttling", str(Config.get("SIGNAL_THROTTLING", 10))),
                ("adapt_confidence_threshold", "true"),
                ("high_quality_trader_score", "7.0"),
                ("whale_transaction_threshold", str(Config.get("WHALE_TRANSACTION_THRESHOLD", 10000))),
                ("liquidity_healthy_threshold", str(Config.get("LIQUIDITY_HEALTHY_THRESHOLD", 20000))),
                ("slippage_warning_threshold", str(Config.get("SLIPPAGE_WARNING_THRESHOLD", 10))),
                ("trader_quality_weight", str(Config.get("TRADER_QUALITY_WEIGHT", 0.35))),
                ("whale_activity_weight", str(Config.get("WHALE_ACTIVITY_WEIGHT", 0.20))),
                ("holder_growth_weight", str(Config.get("HOLDER_GROWTH_WEIGHT", 0.15))),
                ("liquidity_health_weight", str(Config.get("LIQUIDITY_HEALTH_WEIGHT", 0.15))),
                ("technical_factors_weight", str(Config.get("TECHNICAL_FACTORS_WEIGHT", 0.15)))
            ]
            for key, value in default_settings:
                cur.execute("""
                    INSERT INTO bot_settings (key, value)
                    VALUES (%s, %s)
                    ON CONFLICT (key) DO NOTHING
                """, (key, value))
            
            conn.commit()
            logger.info("✅ Base de datos inicializada correctamente")
            return True
    except Exception as e:
        logger.error(f"🚨 Error crítico al inicializar base de datos: {e}", exc_info=True)
        return False

def clear_query_cache():
    global query_cache, query_cache_timestamp, query_cache_hits, query_cache_misses
    query_cache = {}
    query_cache_timestamp = {}
    query_cache_hits = 0
    query_cache_misses = 0
    logger.info("Cache de consultas limpiada")

def get_cache_stats():
    global query_cache_hits, query_cache_misses
    total = query_cache_hits + query_cache_misses
    hit_ratio = query_cache_hits / total if total > 0 else 0
    return {
        "cache_size": len(query_cache),
        "cache_hits": query_cache_hits,
        "cache_misses": query_cache_misses,
        "hit_ratio": hit_ratio
    }

@retry_db_operation()
def execute_cached_query(query, params=None, max_age=60, write_query=False):
    global query_cache, query_cache_timestamp, query_cache_hits, query_cache_misses
    if write_query:
        with get_connection() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query, params or ())
            conn.commit()
            return []
    cache_key = f"{query}:{str(params)}"
    now = time.time()
    if cache_key in query_cache and cache_key in query_cache_timestamp:
        cache_age = now - query_cache_timestamp[cache_key]
        if cache_age < max_age:
            query_cache_hits += 1
            return query_cache[cache_key]
    query_cache_misses += 1
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, params or ())
        results = [dict(row) for row in cur.fetchall()]
        query_cache[cache_key] = results
        query_cache_timestamp[cache_key] = now
        return results

# Funciones adicionales para análisis avanzado

@retry_db_operation()
def save_token_liquidity(token, liquidity_data):
    query = """
    INSERT INTO token_liquidity 
        (token, total_liquidity_usd, volume_24h, slippage_1k, slippage_10k, dex_sources)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    sources = liquidity_data.get("sources", [])
    if isinstance(sources, list):
        sources_array = sources
    else:
        sources_array = [sources] if sources else []
    params = (
        token,
        liquidity_data.get("total_liquidity_usd", 0),
        liquidity_data.get("volume_24h", 0),
        liquidity_data.get("slippage_1k", 0),
        liquidity_data.get("slippage_10k", 0),
        sources_array
    )
    execute_cached_query(query, params, write_query=True)
    logger.debug(f"Datos de liquidez guardados para token {token}")

@retry_db_operation()
def save_whale_activity(token, transaction_data):
    query = """
    INSERT INTO whale_activity 
        (token, transaction_hash, wallet, amount_usd, tx_type, impact_score)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    params = (
        token,
        transaction_data.get("tx_hash", "unknown"),
        transaction_data.get("wallet", ""),
        transaction_data.get("amount_usd", 0),
        transaction_data.get("type", ""),
        transaction_data.get("impact_score", 0)
    )
    execute_cached_query(query, params, write_query=True)
    logger.debug(f"Actividad de ballena registrada para token {token}")

@retry_db_operation()
def save_holder_growth(token, holder_data):
    query = """
    INSERT INTO holder_growth 
        (token, holder_count, growth_rate_1h, growth_rate_24h)
    VALUES (%s, %s, %s, %s)
    """
    params = (
        token,
        holder_data.get("holder_count", 0),
        holder_data.get("growth_rate_1h", 0),
        holder_data.get("growth_rate_24h", 0)
    )
    execute_cached_query(query, params, write_query=True)
    logger.debug(f"Datos de holders guardados para token {token}")

@retry_db_operation()
def save_trader_profile(wallet, profile_data):
    query = """
    INSERT INTO trader_profiles 
        (wallet, profile_data, quality_score, specialty, updated_at)
    VALUES (%s, %s, %s, %s, NOW())
    ON CONFLICT (wallet) 
    DO UPDATE SET 
        profile_data = EXCLUDED.profile_data,
        quality_score = EXCLUDED.quality_score,
        specialty = EXCLUDED.specialty,
        updated_at = NOW()
    """
    if isinstance(profile_data, dict):
        profile_json = psycopg2.extras.Json(profile_data)
    else:
        profile_json = profile_data
    specialty = ""
    if "specialization" in profile_data and "token_types" in profile_data["specialization"]:
        token_types = profile_data["specialization"]["token_types"]
        if token_types:
            specialty = max(token_types.items(), key=lambda x: x[1])[0]
    params = (
        wallet,
        profile_json,
        profile_data.get("quality_score", 0.5),
        specialty
    )
    execute_cached_query(query, params, write_query=True)
    logger.debug(f"Perfil de trader actualizado para {wallet}")

@retry_db_operation()
def save_trader_pattern(token, wallets, coordination_score, pattern_type):
    query = """
    INSERT INTO trader_patterns 
        (token, wallets, coordination_score, pattern_type)
    VALUES (%s, %s, %s, %s)
    """
    params = (
        token,
        wallets,
        coordination_score,
        pattern_type
    )
    execute_cached_query(query, params, write_query=True)
    logger.debug(f"Patrón de traders guardado para token {token}")

@retry_db_operation()
def save_token_analysis(token, analysis_data):
    query = """
    INSERT INTO token_analysis 
        (token, volume_trend, price_trend, volatility, rsi, pattern_quality)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    params = (
        token,
        analysis_data.get("volume_trend", "neutral"),
        analysis_data.get("trend", "neutral"),
        analysis_data.get("volatility", 0),
        analysis_data.get("rsi", 50),
        analysis_data.get("price_action_quality", 0)
    )
    execute_cached_query(query, params, write_query=True)
    logger.debug(f"Análisis técnico guardado para token {token}")

@retry_db_operation()
def get_top_tokens_by_performance(timeframe="1h", limit=10):
    query = """
    SELECT 
        s.token,
        s.signal_id,
        sp.percent_change,
        s.confidence,
        sp.traders_count,
        sp.extra_data,
        s.created_at,
        sp.timestamp
    FROM signal_performance sp
    JOIN signals s ON sp.signal_id = s.id
    WHERE 
        sp.timeframe = %s AND
        sp.timestamp > NOW() - INTERVAL '24 HOUR'
    ORDER BY sp.percent_change DESC
    LIMIT %s
    """
    results = execute_cached_query(query, (timeframe, limit), max_age=60)
    return results

@retry_db_operation()
def get_tokens_with_high_liquidity(min_liquidity=20000, limit=20):
    query = """
    SELECT 
        token,
        total_liquidity_usd,
        volume_24h,
        dex_sources,
        created_at
    FROM token_liquidity
    WHERE 
        total_liquidity_usd >= %s AND
        created_at > NOW() - INTERVAL '1 HOUR'
    ORDER BY created_at DESC
    LIMIT %s
    """
    results = execute_cached_query(query, (min_liquidity, limit), max_age=300)
    return results

@retry_db_operation()
def get_trending_tokens(limit=10):
    query = """
    SELECT 
        token,
        platforms,
        discovery_potential,
        created_at
    FROM trending_tokens
    WHERE created_at > NOW() - INTERVAL '6 HOUR'
    ORDER BY discovery_potential DESC
    LIMIT %s
    """
    results = execute_cached_query(query, (limit,), max_age=300)
    return results

@retry_db_operation()
def get_tokens_with_whale_activity(hours=24, min_impact=0.5, limit=20):
    query = """
    SELECT 
        token,
        COUNT(*) as transaction_count,
        SUM(amount_usd) as total_volume,
        MAX(impact_score) as max_impact,
        AVG(impact_score) as avg_impact,
        MAX(created_at) as latest_activity
    FROM whale_activity
    WHERE 
        created_at > NOW() - INTERVAL '%s HOUR' AND
        impact_score >= %s
    GROUP BY token
    ORDER BY max_impact DESC, total_volume DESC
    LIMIT %s
    """
    results = execute_cached_query(query, (hours, min_impact, limit), max_age=60)
    return results

@retry_db_operation()
def get_tokens_with_coordinated_activity(min_score=0.7, limit=10):
    query = """
    SELECT 
        token,
        AVG(coordination_score) as avg_coordination,
        COUNT(*) as pattern_count,
        MAX(detected_at) as latest_detection
    FROM trader_patterns
    WHERE 
        detected_at > NOW() - INTERVAL '24 HOUR' AND
        coordination_score >= %s
    GROUP BY token
    ORDER BY avg_coordination DESC
    LIMIT %s
    """
    results = execute_cached_query(query, (min_score, limit), max_age=300)
    return results

@retry_db_operation()
def get_tokens_with_growing_holders(min_growth_rate=5, limit=20):
    query = """
    SELECT 
        token,
        holder_count,
        growth_rate_1h,
        growth_rate_24h,
        created_at
    FROM holder_growth
    WHERE 
        created_at > NOW() - INTERVAL '6 HOUR' AND
        growth_rate_1h >= %s
    ORDER BY growth_rate_1h DESC
    LIMIT %s
    """
    results = execute_cached_query(query, (min_growth_rate, limit), max_age=300)
    return results

@retry_db_operation()
def get_tokens_with_strong_technical_patterns(min_quality=0.7, limit=20):
    query = """
    SELECT 
        token,
        volume_trend,
        price_trend,
        volatility,
        rsi,
        pattern_quality,
        created_at
    FROM token_analysis
    WHERE 
        created_at > NOW() - INTERVAL '12 HOUR' AND
        pattern_quality >= %s
    ORDER BY pattern_quality DESC
    LIMIT %s
    """
    results = execute_cached_query(query, (min_quality, limit), max_age=300)
    return results

@retry_db_operation()
def get_token_integrated_analysis(token):
    result = {
        "token": token,
        "liquidity": {},
        "holder_growth": {},
        "whale_activity": [],
        "technical": {},
        "trending": {},
        "signals": [],
        "performance": {}
    }
    try:
        liquidity_data = get_token_liquidity_history(token, hours=24)
        if liquidity_data:
            latest = liquidity_data[-1]
            result["liquidity"] = {
                "total_liquidity_usd": latest.get("total_liquidity_usd", 0),
                "volume_24h": latest.get("volume_24h", 0),
                "slippage_1k": latest.get("slippage_1k", 0),
                "slippage_10k": latest.get("slippage_10k", 0),
                "dex_sources": latest.get("dex_sources", []),
                "timestamp": latest.get("created_at").isoformat() if latest.get("created_at") else None
            }
    except Exception as e:
        logger.error(f"Error obteniendo liquidez para {token}: {e}")
    try:
        holder_data = get_holder_growth_history(token, days=3)
        if holder_data:
            latest = holder_data[-1]
            result["holder_growth"] = {
                "holder_count": latest.get("holder_count", 0),
                "growth_rate_1h": latest.get("growth_rate_1h", 0),
                "growth_rate_24h": latest.get("growth_rate_24h", 0),
                "timestamp": latest.get("created_at").isoformat() if latest.get("created_at") else None
            }
    except Exception as e:
        logger.error(f"Error obteniendo datos de holders para {token}: {e}")
    try:
        whale_activity = get_whale_activity_for_token(token, hours=24)
        if whale_activity:
            result["whale_activity"] = [
                {
                    "wallet": tx.get("wallet", ""),
                    "amount_usd": tx.get("amount_usd", 0),
                    "tx_type": tx.get("tx_type", ""),
                    "impact_score": tx.get("impact_score", 0),
                    "timestamp": tx.get("created_at").isoformat() if tx.get("created_at") else None
                } for tx in whale_activity[:5]
            ]
    except Exception as e:
        logger.error(f"Error obteniendo actividad de ballenas para {token}: {e}")
    try:
        query = """
        SELECT 
            volume_trend, price_trend, volatility, rsi, pattern_quality, created_at
        FROM token_analysis
        WHERE token = %s
        ORDER BY created_at DESC
        LIMIT 1
        """
        technical_results = execute_cached_query(query, (token,), max_age=300)
        if technical_results:
            tech = technical_results[0]
            result["technical"] = {
                "volume_trend": tech.get("volume_trend", "neutral"),
                "price_trend": tech.get("price_trend", "neutral"),
                "volatility": tech.get("volatility", 0),
                "rsi": tech.get("rsi", 50),
                "pattern_quality": tech.get("pattern_quality", 0),
                "timestamp": tech.get("created_at").isoformat() if tech.get("created_at") else None
            }
    except Exception as e:
        logger.error(f"Error obteniendo análisis técnico para {token}: {e}")
    try:
        query = """
        SELECT 
            platforms, discovery_potential, created_at
        FROM trending_tokens
        WHERE token = %s
        ORDER BY created_at DESC
        LIMIT 1
        """
        trending_results = execute_cached_query(query, (token,), max_age=300)
        if trending_results:
            trend = trending_results[0]
            result["trending"] = {
                "platforms": trend.get("platforms", []),
                "discovery_potential": trend.get("discovery_potential", 0),
                "timestamp": trend.get("created_at").isoformat() if trend.get("created_at") else None
            }
    except Exception as e:
        logger.error(f"Error obteniendo datos de trending para {token}: {e}")
    try:
        query = """
        SELECT 
            id, trader_count, confidence, initial_price, created_at
        FROM signals
        WHERE token = %s AND created_at > NOW() - INTERVAL '7 DAY'
        ORDER BY created_at DESC
        LIMIT 5
        """
        signal_results = execute_cached_query(query, (token,), max_age=60)
        if signal_results:
            result["signals"] = [
                {
                    "signal_id": signal.get("id"),
                    "trader_count": signal.get("trader_count", 0),
                    "confidence": signal.get("confidence", 0),
                    "initial_price": signal.get("initial_price", 0),
                    "timestamp": signal.get("created_at").isoformat() if signal.get("created_at") else None
                } for signal in signal_results
            ]
    except Exception as e:
        logger.error(f"Error obteniendo señales para {token}: {e}")
    try:
        query = """
        SELECT 
            sp.timeframe, sp.percent_change, sp.timestamp
        FROM signal_performance sp
        JOIN signals s ON sp.signal_id = s.id
        WHERE s.token = %s
        ORDER BY sp.timestamp DESC
        LIMIT 10
        """
        performance_results = execute_cached_query(query, (token,), max_age=60)
        if performance_results:
            by_timeframe = {}
            for perf in performance_results:
                tf = perf.get("timeframe")
                if tf not in by_timeframe:
                    by_timeframe[tf] = {
                        "percent_change": perf.get("percent_change", 0),
                        "timestamp": perf.get("timestamp").isoformat() if perf.get("timestamp") else None
                    }
            result["performance"] = by_timeframe
    except Exception as e:
        logger.error(f"Error obteniendo rendimiento para {token}: {e}")
    return result

@retry_db_operation()
def get_market_trends():
    result = {
        "trending_tokens": [],
        "high_liquidity_tokens": [],
        "tokens_with_whale_activity": [],
        "tokens_with_growing_holders": [],
        "best_performing_tokens": [],
        "market_insights": []
    }
    try:
        trending = get_trending_tokens(limit=5)
        result["trending_tokens"] = [
            {
                "token": t.get("token"),
                "platforms": t.get("platforms", []),
                "discovery_potential": t.get("discovery_potential", 0)
            } for t in trending
        ]
    except Exception as e:
        logger.error(f"Error obteniendo tokens trending: {e}")
    try:
        high_liquidity = get_tokens_with_high_liquidity(min_liquidity=50000, limit=5)
        result["high_liquidity_tokens"] = [
            {
                "token": t.get("token"),
                "total_liquidity_usd": t.get("total_liquidity_usd", 0),
                "volume_24h": t.get("volume_24h", 0)
            } for t in high_liquidity
        ]
    except Exception as e:
        logger.error(f"Error obteniendo tokens con alta liquidez: {e}")
    try:
        whale_activity = get_tokens_with_whale_activity(hours=12, min_impact=0.7, limit=5)
        result["tokens_with_whale_activity"] = [
            {
                "token": t.get("token"),
                "transaction_count": t.get("transaction_count", 0),
                "total_volume": t.get("total_volume", 0),
                "max_impact": t.get("max_impact", 0)
            } for t in whale_activity
        ]
    except Exception as e:
        logger.error(f"Error obteniendo tokens con actividad de ballenas: {e}")
    try:
        growing_holders = get_tokens_with_growing_holders(min_growth_rate=10, limit=5)
        result["tokens_with_growing_holders"] = [
            {
                "token": t.get("token"),
                "holder_count": t.get("holder_count", 0),
                "growth_rate_1h": t.get("growth_rate_1h", 0)
            } for t in growing_holders
        ]
    except Exception as e:
        logger.error(f"Error obteniendo tokens con crecimiento de holders: {e}")
    try:
        best_performing = get_top_tokens_by_performance(timeframe="1h", limit=5)
        result["best_performing_tokens"] = [
            {
                "token": t.get("token"),
                "percent_change": t.get("percent_change", 0),
                "confidence": t.get("confidence", 0)
            } for t in best_performing
        ]
    except Exception as e:
        logger.error(f"Error obteniendo tokens con mejor rendimiento: {e}")
    try:
        query = """
        SELECT COUNT(*) as count FROM trending_tokens WHERE created_at > NOW() - INTERVAL '24 HOUR'
        """
        trending_count = execute_cached_query(query, max_age=300)
        
        query = """
        SELECT COUNT(*) as count FROM whale_activity WHERE created_at > NOW() - INTERVAL '24 HOUR'
        """
        whale_tx_count = execute_cached_query(query, max_age=300)
        
        query = """
        SELECT AVG(percent_change) as avg_change FROM signal_performance 
        WHERE timeframe = '1h' AND timestamp > NOW() - INTERVAL '24 HOUR'
        """
        avg_performance = execute_cached_query(query, max_age=300)
        
        market_activity = "normal"
        if trending_count and trending_count[0]["count"] > 10:
            market_activity = "alta"
        elif trending_count and trending_count[0]["count"] < 3:
            market_activity = "baja"
        
        avg_change = 0
        if avg_performance and avg_performance[0]["avg_change"] is not None:
            avg_change = avg_performance[0]["avg_change"]
        
        market_sentiment = "neutral"
        if avg_change > 10:
            market_sentiment = "muy positivo"
        elif avg_change > 5:
            market_sentiment = "positivo"
        elif avg_change < -10:
            market_sentiment = "muy negativo"
        elif avg_change < -5:
            market_sentiment = "negativo"
        
        insights = [
            f"Actividad del mercado: {market_activity}",
            f"Sentimiento del mercado: {market_sentiment}"
        ]
        
        if whale_tx_count and whale_tx_count[0]["count"] > 50:
            insights.append("Alta actividad de ballenas en las últimas 24h")
        
        result["market_insights"] = insights
    except Exception as e:
        logger.error(f"Error generando insights de mercado: {e}")
    return result
