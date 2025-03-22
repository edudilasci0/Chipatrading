# db.py - Implementación completa para el bot de trading en Solana

import os
import sys
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

# Pool de conexiones global y lock para operaciones seguras en multithreading
pool = None
pool_lock = threading.Lock()

# Caché para consultas frecuentes
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
            except Exception:
                pass
        init_db_pool()
        conn = pool.getconn()
        yield conn
    finally:
        if conn is not None:
            pool.putconn(conn)

def retry_db_operation(max_attempts=3, delay=1, backoff_factor=2):
    """
    Decorador para reintentar operaciones en la base de datos.
    """
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
            # Crear tabla para versiones del esquema/migraciones
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
                        timestamp TIMESTAMP DEFAULT NOW(),
                        extra_data JSONB,
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
            
            # Migración #3: Tablas para análisis avanzado
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
            
            # Crear índices para optimizar las consultas
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
            
            # Insertar configuraciones iniciales en bot_settings
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

# Funciones de inserción y consulta existentes

@retry_db_operation()
def save_transaction(tx_data):
    query = """
    INSERT INTO transactions (wallet, token, tx_type, amount_usd)
    VALUES (%s, %s, %s, %s)
    """
    params = (tx_data["wallet"], tx_data["token"], tx_data["type"], tx_data["amount_usd"])
    execute_cached_query(query, params, write_query=True)
    # Invalida caché de consultas relacionadas a transacciones
    for key in list(query_cache.keys()):
        if "transactions" in key:
            query_cache.pop(key, None)
            query_cache_timestamp.pop(key, None)

@retry_db_operation()
def update_wallet_score(wallet, new_score):
    query = """
    INSERT INTO wallet_scores (wallet, score)
    VALUES (%s, %s)
    ON CONFLICT (wallet)
    DO UPDATE SET score = EXCLUDED.score, updated_at = NOW()
    """
    execute_cached_query(query, (wallet, new_score), write_query=True)
    for key in list(query_cache.keys()):
        if "wallet_scores" in key and wallet in key:
            query_cache.pop(key, None)
            query_cache_timestamp.pop(key, None)

@retry_db_operation()
def get_wallet_score(wallet):
    query = "SELECT score FROM wallet_scores WHERE wallet=%s"
    results = execute_cached_query(query, (wallet,), max_age=300)
    if results:
        return float(results[0]['score'])
    else:
        return Config.get("DEFAULT_SCORE", 5.0)

@retry_db_operation()
def save_signal(token, trader_count, confidence, initial_price=None):
    query = """
    INSERT INTO signals (token, trader_count, confidence, initial_price)
    VALUES (%s, %s, %s, %s)
    RETURNING id
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, (token, trader_count, confidence, initial_price))
        signal_id = cur.fetchone()[0]
        conn.commit()
    for key in list(query_cache.keys()):
        if "signals" in key:
            query_cache.pop(key, None)
            query_cache_timestamp.pop(key, None)
    return signal_id

@retry_db_operation()
def save_signal_features(signal_id, token, features):
    query = """
    INSERT INTO signal_features (signal_id, token, feature_json)
    VALUES (%s, %s, %s)
    """
    features_json = psycopg2.extras.Json(features)
    execute_cached_query(query, (signal_id, token, features_json), write_query=True)
    logger.info(f"Features saved for signal {signal_id} and token {token}")

# Función para guardar datos de rendimiento de señales (ya existente)
@retry_db_operation()
def save_signal_performance(token, signal_id, timeframe, percent_change, confidence, traders_count, extra_data=None):
    query = """
    INSERT INTO signal_performance 
        (token, signal_id, timeframe, percent_change, confidence, traders_count, extra_data)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (token, timeframe) 
    DO UPDATE SET 
        percent_change = EXCLUDED.percent_change,
        confidence = EXCLUDED.confidence,
        traders_count = EXCLUDED.traders_count,
        extra_data = EXCLUDED.extra_data,
        timestamp = NOW()
    """
    if extra_data:
        extra_data_json = psycopg2.extras.Json(extra_data)
    else:
        extra_data_json = None
    params = (token, signal_id, timeframe, percent_change, confidence, traders_count, extra_data_json)
    execute_cached_query(query, params, write_query=True)
    return True

# NUEVAS FUNCIONES REQUERIDAS

@retry_db_operation()
def count_transactions_today():
    """
    Cuenta las transacciones registradas hoy.
    """
    query = """
    SELECT COUNT(*) as count FROM transactions
    WHERE created_at::date = CURRENT_DATE
    """
    results = execute_cached_query(query, max_age=60)
    if results:
        return results[0]['count']
    return 0

@retry_db_operation()
def count_signals_last_hour():
    """
    Cuenta las señales emitidas en la última hora.
    """
    query = """
    SELECT COUNT(*) as count FROM signals
    WHERE created_at > NOW() - INTERVAL '1 HOUR'
    """
    results = execute_cached_query(query, max_age=60)
    if results:
        return results[0]['count']
    return 0

@retry_db_operation()
def count_signals_today():
    """
    Cuenta las señales emitidas hoy.
    """
    query = """
    SELECT COUNT(*) as count FROM signals
    WHERE created_at::date = CURRENT_DATE
    """
    results = execute_cached_query(query, max_age=60)
    if results:
        return results[0]['count']
    return 0

@retry_db_operation()
def get_recent_untracked_signals(hours=24):
    """
    Obtiene señales recientes que no han sido marcadas como procesadas (untracked) en las últimas 'hours' horas.
    
    Args:
        hours (int): Número de horas a considerar.
        
    Returns:
        list: Lista de señales recientes.
    """
    query = """
    SELECT * FROM signals
    WHERE outcome_collected = FALSE
    AND created_at > NOW() - INTERVAL '%s HOUR'
    ORDER BY created_at DESC
    """
    results = execute_cached_query(query, (hours,), max_age=60)
    return results

# Otras funciones de consulta adicionales (por ejemplo, para liquidez, actividad de ballenas, etc.)
@retry_db_operation()
def get_token_liquidity_history(token, hours=24):
    query = """
    SELECT total_liquidity_usd, volume_24h, slippage_1k, slippage_10k, dex_sources, created_at
    FROM token_liquidity
    WHERE token = %s AND created_at > NOW() - INTERVAL '%s HOUR'
    ORDER BY created_at ASC
    """
    results = execute_cached_query(query, (token, hours), max_age=60)
    return results

@retry_db_operation()
def get_whale_activity_for_token(token, hours=24):
    query = """
    SELECT wallet, amount_usd, tx_type, impact_score, created_at
    FROM whale_activity
    WHERE token = %s AND created_at > NOW() - INTERVAL '%s HOUR'
    ORDER BY created_at DESC
    """
    results = execute_cached_query(query, (token, hours), max_age=60)
    return results

@retry_db_operation()
def get_holder_growth_history(token, days=7):
    query = """
    SELECT holder_count, growth_rate_1h, growth_rate_24h, created_at
    FROM holder_growth
    WHERE token = %s AND created_at > NOW() - INTERVAL '%s DAY'
    ORDER BY created_at ASC
    """
    results = execute_cached_query(query, (token, days), max_age=300)
    return results

@retry_db_operation()
def get_trader_profile_by_wallet(wallet):
    query = """
    SELECT profile_data, quality_score, specialty, updated_at
    FROM trader_profiles
    WHERE wallet = %s
    """
    results = execute_cached_query(query, (wallet,), max_age=3600)
    if results:
        return results[0]
    return None

@retry_db_operation()
def get_high_quality_traders(min_score=0.7, limit=100):
    query = """
    SELECT wallet, quality_score, specialty
    FROM trader_profiles
    WHERE quality_score >= %s
    ORDER BY quality_score DESC
    LIMIT %s
    """
    results = execute_cached_query(query, (min_score, limit), max_age=3600)
    return results

@retry_db_operation()
def get_signal_performance_with_extras(signal_id):
    query = """
    SELECT timeframe, percent_change, confidence, traders_count, extra_data, timestamp
    FROM signal_performance
    WHERE signal_id = %s
    ORDER BY timestamp ASC
    """
    results = execute_cached_query(query, (signal_id,), max_age=60)
    return [
        {
            "timeframe": row['timeframe'],
            "percent_change": row['percent_change'],
            "confidence": row['confidence'],
            "traders_count": row['traders_count'],
            "timestamp": row['timestamp'].isoformat() if row['timestamp'] else None,
            "extra_data": row.get('extra_data', {})
        } for row in results
    ]
