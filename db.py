#!/usr/bin/env python3
# db.py - Módulo de acceso a la base de datos para el bot de trading en Solana

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

# Pool de conexiones y variables para la caché de consultas
pool = None
pool_lock = threading.Lock()

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

@retry_db_operation()
def init_db():
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS schema_version (
                        version INTEGER PRIMARY KEY,
                        applied_at TIMESTAMP DEFAULT NOW(),
                        description TEXT
                    )
                """)
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Error creando tabla schema_version: {e}")
                return False

            try:
                cur.execute("SELECT MAX(version) FROM schema_version")
                result = cur.fetchone()
                current_version = result[0] if result and result[0] else 0
                logger.info(f"Versión actual del schema: {current_version}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Error verificando versión del schema: {e}")
                return False

            if current_version < 1:
                try:
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
                    conn.commit()
                    logger.info("Migración #1 aplicada correctamente")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error en migración #1: {e}")
                    return False

            if current_version < 2:
                try:
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
                    conn.commit()
                    logger.info("Migración #2 aplicada correctamente")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error en migración #2: {e}")
                    return False

            if current_version < 3:
                try:
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
                    conn.commit()
                    logger.info("Migración #3 aplicada correctamente")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error en migración #3: {e}")
                    return False

            try:
                cur.execute("SELECT market_cap FROM signals LIMIT 1")
            except Exception as e:
                conn.rollback()
                logger.info("Añadiendo campo market_cap a la tabla signals")
                try:
                    cur.execute("ALTER TABLE signals ADD COLUMN IF NOT EXISTS market_cap NUMERIC DEFAULT 0")
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error añadiendo campo market_cap: {e}")
                    return False
            
            try:
                cur.execute("SELECT volume FROM signals LIMIT 1")
            except Exception as e:
                conn.rollback()
                logger.info("Añadiendo campo volume a la tabla signals")
                try:
                    cur.execute("ALTER TABLE signals ADD COLUMN IF NOT EXISTS volume NUMERIC DEFAULT 0")
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error añadiendo campo volume: {e}")
                    return False

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
                
                conn.commit()
                logger.info("✅ Índices creados correctamente")
            except Exception as e:
                conn.rollback()
                logger.warning(f"⚠️ Error al crear índices: {e}")
            
            try:
                default_settings = [
                    ("min_transaction_usd", Config.MIN_TRANSACTION_USD),
                    ("min_traders_for_signal", Config.MIN_TRADERS_FOR_SIGNAL),
                    ("signal_window_seconds", Config.SIGNAL_WINDOW_SECONDS),
                    ("min_confidence_threshold", Config.MIN_CONFIDENCE_THRESHOLD),
                    ("rugcheck_min_score", "50"),
                    ("min_volume_usd", Config.VOLUME_THRESHOLD),
                    ("signal_throttling", "10"),
                    ("adapt_confidence_threshold", "true"),
                    ("high_quality_trader_score", Config.HIGH_QUALITY_TRADER_SCORE),
                    ("whale_transaction_threshold", Config.WHALE_TRANSACTION_THRESHOLD),
                    ("liquidity_healthy_threshold", Config.LIQUIDITY_HEALTHY_THRESHOLD),
                    ("slippage_warning_threshold", Config.SLIPPAGE_WARNING_THRESHOLD),
                    ("trader_quality_weight", Config.TRADER_QUALITY_WEIGHT),
                    ("whale_activity_weight", Config.WHALE_ACTIVITY_WEIGHT),
                    ("holder_growth_weight", Config.HOLDER_GROWTH_WEIGHT),
                    ("liquidity_health_weight", Config.LIQUIDITY_HEALTH_WEIGHT),
                    ("technical_factors_weight", Config.TECHNICAL_FACTORS_WEIGHT)
                ]
                
                for key, value in default_settings:
                    try:
                        cur.execute("""
                            INSERT INTO bot_settings (key, value)
                            VALUES (%s, %s)
                            ON CONFLICT (key) DO NOTHING
                        """, (key, str(value)))
                    except Exception as e:
                        logger.warning(f"Error al configurar setting {key}: {e}")
                conn.commit()
                logger.info("✅ Base de datos inicializada correctamente")
                return True
            except Exception as e:
                conn.rollback()
                logger.error(f"Error al configurar settings: {e}")
                return False
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
def save_transaction(tx_data):
    query = """
    INSERT INTO transactions (wallet, token, tx_type, amount_usd)
    VALUES (%s, %s, %s, %s)
    """
    params = (tx_data["wallet"], tx_data["token"], tx_data["type"], tx_data["amount_usd"])
    try:
        execute_cached_query(query, params, write_query=True)
        return True
    except Exception as e:
        logger.error(f"Error guardando transacción: {e}")
        return False

@retry_db_operation()
def update_setting(key, value):
    """
    Actualiza o crea un setting en la tabla bot_settings.
    
    Args:
        key: Nombre del setting a actualizar.
        value: Valor a establecer.
        
    Returns:
        bool: True si la operación fue exitosa.
    """
    query = """
    INSERT INTO bot_settings (key, value, updated_at)
    VALUES (%s, %s, NOW())
    ON CONFLICT (key) DO UPDATE 
    SET value = %s, updated_at = NOW()
    """
    try:
        execute_cached_query(query, (key, value, value), write_query=True)
        cache_key = f"SELECT * FROM bot_settings WHERE key = '{key}'"
        if cache_key in query_cache:
            del query_cache[cache_key]
        logger.info(f"Setting actualizado: {key} = {value}")
        return True
    except Exception as e:
        logger.error(f"Error al actualizar setting {key}: {e}")
        return False

@retry_db_operation()
def save_signal(token, trader_count, confidence, initial_price, market_cap=0, volume=0):
    """
    Guarda una nueva señal en la base de datos.
    
    Args:
        token: Dirección del token.
        trader_count: Número de traders involucrados.
        confidence: Nivel de confianza (0-1).
        initial_price: Precio inicial del token.
        market_cap: Market cap del token (opcional).
        volume: Volumen del token (opcional).
        
    Returns:
        int: ID de la señal creada o None en caso de error.
    """
    query = """
    INSERT INTO signals (token, trader_count, confidence, initial_price, market_cap, volume, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, NOW())
    RETURNING id
    """
    params = (token, trader_count, confidence, initial_price, market_cap, volume)
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            signal_id = cur.fetchone()[0]
            conn.commit()
            logger.info(f"Señal guardada para {token} con ID {signal_id}")
            return signal_id
    except Exception as e:
        logger.error(f"Error guardando señal para {token}: {e}")
        return None

@retry_db_operation()
def update_signal_status(signal_id, status, reason=None):
    """
    Actualiza el estado de una señal en la base de datos.
    
    Args:
        signal_id: ID de la señal.
        status: Nuevo estado ('active', 'dead', etc.).
        reason: Razón del cambio de estado (opcional).
        
    Returns:
        bool: True si la actualización fue exitosa.
    """
    query = """
    UPDATE signals 
    SET status = %s, status_reason = %s, updated_at = NOW()
    WHERE id = %s
    """
    try:
        execute_cached_query(query, (status, reason, signal_id), write_query=True)
        logger.info(f"Estado de señal {signal_id} actualizado a '{status}'")
        return True
    except Exception as e:
        logger.error(f"Error actualizando estado de señal {signal_id}: {e}")
        return False

@retry_db_operation()
def get_recent_untracked_signals(hours=24):
    """
    Obtiene señales recientes que aún no han sido rastreadas por el performance tracker.
    
    Args:
        hours: Número de horas atrás para buscar señales.
        
    Returns:
        list: Lista de señales pendientes para procesamiento.
    """
    try:
        query = """
        SELECT s.id as signal_id, s.token, s.confidence, s.initial_price, 
               s.trader_count as traders_count, s.market_cap, s.volume as total_volume,
               s.created_at
        FROM signals s
        LEFT JOIN signal_performance sp ON s.id = sp.signal_id
        WHERE s.created_at > NOW() - INTERVAL '%s HOURS'
          AND sp.id IS NULL
        ORDER BY s.created_at DESC
        """
        results = execute_cached_query(query, (hours,), max_age=60)
        for result in results:
            if "created_at" in result and result["created_at"]:
                result["timestamp"] = result["created_at"].timestamp()
        logger.info(f"Encontradas {len(results)} señales pendientes para seguimiento")
        return results
    except Exception as e:
        logger.error(f"Error obteniendo señales pendientes: {e}")
        return []

@retry_db_operation()
def count_signals_today():
    """
    Cuenta el número de señales generadas hoy.
    
    Returns:
        int: Número de señales generadas hoy.
    """
    query = """
    SELECT COUNT(*) as count
    FROM signals
    WHERE created_at >= CURRENT_DATE
    """
    result = execute_cached_query(query)
    if result and result[0]["count"] is not None:
        return result[0]["count"]
    return 0

@retry_db_operation()
def count_transactions_today():
    """
    Cuenta el número de transacciones procesadas hoy.
    
    Returns:
        int: Número de transacciones procesadas hoy.
    """
    query = """
    SELECT COUNT(*) as count
    FROM transactions
    WHERE created_at >= CURRENT_DATE
    """
    result = execute_cached_query(query)
    if result and result[0]["count"] is not None:
        return result[0]["count"]
    return 0

@retry_db_operation()
def get_wallet_recent_transactions(wallet, hours=24):
    query = """
    SELECT wallet, token, tx_type, amount_usd, created_at
    FROM transactions
    WHERE wallet = %s
    AND created_at > NOW() - INTERVAL '%s HOURS'
    ORDER BY created_at DESC
    """
    results = execute_cached_query(query, (wallet, hours), max_age=60)
    return results

# NUEVA FUNCIÓN: update_wallet_score
@retry_db_operation()
def update_wallet_score(wallet, score):
    """
    Actualiza el score de un wallet en la base de datos.
    
    Args:
        wallet: Dirección del wallet
        score: Nuevo score (0-10)
        
    Returns:
        bool: True si se actualizó correctamente
    """
    query = """
    INSERT INTO wallet_scores (wallet, score, updated_at)
    VALUES (%s, %s, NOW())
    ON CONFLICT (wallet) DO UPDATE 
    SET score = %s, updated_at = NOW()
    """
    try:
        execute_cached_query(query, (wallet, score, score), write_query=True)
        logger.info(f"Score actualizado en BD para {wallet}: {score}")
        # Limpiar caché
        cache_key = f"SELECT * FROM wallet_scores WHERE wallet = '{wallet}'"
        if cache_key in query_cache:
            del query_cache[cache_key]
        return True
    except Exception as e:
        logger.error(f"Error actualizando score para {wallet} en BD: {e}")
        return False
