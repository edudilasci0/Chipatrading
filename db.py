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

# Variables globales para el pool y cache de consultas
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
            # Crear la tabla schema_version si no existe
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

            # Verificar versión actual del schema
            try:
                cur.execute("SELECT MAX(version) FROM schema_version")
                result = cur.fetchone()
                current_version = result[0] if result and result[0] else 0
                logger.info(f"Versión actual del schema: {current_version}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Error verificando versión del schema: {e}")
                return False

            # Migración #1: Tablas iniciales
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

            # Migración #2: Mejoras y nuevas tablas
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

            # Migración #3: Análisis avanzado
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

            # Verificar si la tabla signals tiene los campos market_cap y volume
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
            
            # Crear índices para optimizar consultas
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
            
            # Configurar valores predeterminados en bot_settings
            try:
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
                    try:
                        cur.execute("""
                            INSERT INTO bot_settings (key, value)
                            VALUES (%s, %s)
                            ON CONFLICT (key) DO NOTHING
                        """, (key, value))
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
def get_token_transactions(token, hours=1):
    query = """
    SELECT wallet, token, tx_type, amount_usd, created_at
    FROM transactions
    WHERE token = %s
    AND created_at > NOW() - INTERVAL '%s HOURS'
    ORDER BY created_at DESC
    """
    results = execute_cached_query(query, (token, hours), max_age=60)
    transactions = []
    for row in results:
        tx = {
            "wallet": row["wallet"],
            "token": row["token"],
            "type": row["tx_type"],
            "amount_usd": float(row["amount_usd"]),
            "timestamp": row["created_at"].timestamp() if hasattr(row["created_at"], "timestamp") else time.time()
        }
        transactions.append(tx)
    return transactions

@retry_db_operation()
def get_recent_untracked_signals(hours=24):
    query = """
    SELECT id, token, trader_count, confidence, initial_price, created_at
    FROM signals
    WHERE outcome_collected = FALSE
    AND created_at > NOW() - INTERVAL '%s HOURS'
    ORDER BY created_at DESC
    """
    results = execute_cached_query(query, (hours,), max_age=60)
    return results

@retry_db_operation()
def count_signals_today():
    query = """
    SELECT COUNT(*) as count FROM signals
    WHERE created_at::date = CURRENT_DATE
    """
    results = execute_cached_query(query, max_age=60)
    if results:
        return results[0]['count']
    return 0

@retry_db_operation()
def count_transactions_today():
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
    query = """
    SELECT COUNT(*) as count FROM signals
    WHERE created_at > NOW() - INTERVAL '1 HOUR'
    """
    results = execute_cached_query(query, max_age=60)
    if results:
        return results[0]['count']
    return 0

@retry_db_operation()
def mark_signal_outcome_collected(signal_id):
    query = """
    UPDATE signals 
    SET outcome_collected = TRUE
    WHERE id = %s
    """
    execute_cached_query(query, (signal_id,), write_query=True)
    return True

@retry_db_operation()
def get_signal_performance(signal_id):
    query = """
    SELECT timeframe, percent_change, confidence, traders_count, timestamp
    FROM signal_performance
    WHERE signal_id = %s
    ORDER BY timestamp ASC
    """
    results = execute_cached_query(query, (signal_id,), max_age=60)
    return results

@retry_db_operation()
def get_signal_features(signal_id):
    query = """
    SELECT feature_json
    FROM signal_features
    WHERE signal_id = %s
    LIMIT 1
    """
    results = execute_cached_query(query, (signal_id,), max_age=300)
    if results and results[0]["feature_json"]:
        return results[0]["feature_json"]
    return None

@retry_db_operation()
def get_signals_performance_stats():
    query = """
    SELECT 
        timeframe,
        COUNT(*) as total_signals,
        AVG(percent_change) as avg_percent_change,
        SUM(CASE WHEN percent_change > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate,
        created_at
    FROM signal_performance
    WHERE created_at > NOW() - INTERVAL '7 DAYS'
    GROUP BY timeframe, created_at
    ORDER BY 
        CASE timeframe
            WHEN '3m' THEN 1
            WHEN '5m' THEN 2
            WHEN '10m' THEN 3
            WHEN '30m' THEN 4
            WHEN '1h' THEN 5
            WHEN '2h' THEN 6
            WHEN '4h' THEN 7
            WHEN '24h' THEN 8
            ELSE 9
        END
    """
    results = execute_cached_query(query, max_age=300)
    formatted_results = []
    for row in results:
        formatted_results.append({
            "timeframe": row["timeframe"],
            "total_signals": row["total_signals"],
            "avg_percent_change": round(row["avg_percent_change"], 2) if row["avg_percent_change"] is not None else 0,
            "success_rate": round(row["success_rate"], 2) if row["success_rate"] is not None else 0,
            "created_at": row["created_at"].isoformat() if row["created_at"] else None
        })
    return formatted_results

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
