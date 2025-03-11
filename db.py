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

# Configurar logging
logger = logging.getLogger("database")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Pool de conexiones global
pool = None
pool_lock = threading.Lock()  # Para operaciones seguras en multithreading

# Cache para consultas frecuentes
query_cache = {}
query_cache_timestamp = {}
query_cache_hits = 0
query_cache_misses = 0

def init_db_pool(min_conn=1, max_conn=10):
    """
    Inicializa el pool de conexiones a la base de datos.
    """
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
    """
    Obtiene una conexión del pool y la devuelve cuando termina.
    Implementa reconexión automática en caso de error.
    """
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
    """
    Decorador para reintentar operaciones de BD en caso de error.
    Implementa backoff exponencial con jitter.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            import random
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
    """
    Crea las tablas necesarias si no existen, aplica migraciones y crea índices.
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            
            # Crear tabla para versiones de schema/migrations
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMP DEFAULT NOW(),
                    description TEXT
                )
            """)
            
            # Verificar versión actual del schema
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
                # Registrar migración #1
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
                logger.info("✅ Índices creados correctamente")
            except Exception as e:
                logger.error(f"⚠️ Error al crear índices: {e}")
                conn.rollback()
                fallback_indices = [
                    ("idx_transactions_token", "CREATE INDEX IF NOT EXISTS idx_transactions_token ON transactions(token)"),
                    ("idx_transactions_wallet", "CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(wallet)"),
                    ("idx_transactions_created_at", "CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)"),
                    ("idx_signals_created_at", "CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)"),
                    ("idx_signals_token", "CREATE INDEX IF NOT EXISTS idx_signals_token ON signals(token)"),
                    ("idx_transactions_wallet_token", "CREATE INDEX IF NOT EXISTS idx_transactions_wallet_token ON transactions(wallet, token)"),
                    ("idx_wallet_profits_wallet", "CREATE INDEX IF NOT EXISTS idx_wallet_profits_wallet ON wallet_profits(wallet)"),
                    ("idx_signal_features_token", "CREATE INDEX IF NOT EXISTS idx_signal_features_token ON signal_features(token)"),
                    ("idx_signal_features_signal_id", "CREATE INDEX IF NOT EXISTS idx_signal_features_signal_id ON signal_features(signal_id)")
                ]
                for idx_name, idx_def in fallback_indices:
                    try:
                        cur.execute(idx_def)
                        conn.commit()
                        logger.info(f"✅ Índice {idx_name} creado")
                    except Exception as e2:
                        logger.error(f"⚠️ Error al crear {idx_name}: {e2}")
                        conn.rollback()
            
            # Insertar configuraciones iniciales (solo si no existen)
            default_settings = [
                ("min_transaction_usd", str(Config.MIN_TRANSACTION_USD)),
                ("min_traders_for_signal", str(Config.MIN_TRADERS_FOR_SIGNAL)),
                ("signal_window_seconds", "540"),
                ("min_confidence_threshold", str(Config.MIN_CONFIDENCE_THRESHOLD)),
                ("rugcheck_min_score", "50"),
                ("min_volume_usd", str(Config.MIN_VOLUME_USD)),
                ("signal_throttling", "10"),
                ("adapt_confidence_threshold", "true"),
                ("high_quality_trader_score", "7.0")
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
    """
    Limpia la caché de consultas.
    """
    global query_cache, query_cache_timestamp, query_cache_hits, query_cache_misses
    query_cache = {}
    query_cache_timestamp = {}
    query_cache_hits = 0
    query_cache_misses = 0
    logger.info("Cache de consultas limpiada")

def get_cache_stats():
    """
    Obtiene estadísticas de la caché de consultas.
    """
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
    """
    Ejecuta una consulta con caché para lecturas frecuentes.
    """
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
def save_transaction(tx_data):
    """
    Guarda una transacción en la tabla 'transactions'.
    """
    query = """
    INSERT INTO transactions (wallet, token, tx_type, amount_usd)
    VALUES (%s, %s, %s, %s)
    """
    params = (tx_data["wallet"], tx_data["token"], tx_data["type"], tx_data["amount_usd"])
    execute_cached_query(query, params, write_query=True)
    for key in list(query_cache.keys()):
        if "transactions" in key:
            query_cache.pop(key, None)
            query_cache_timestamp.pop(key, None)

@retry_db_operation()
def update_wallet_score(wallet, new_score):
    """
    Actualiza el score en 'wallet_scores'.
    """
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
    """
    Retorna el score de la wallet; si no existe, retorna el score por defecto.
    """
    query = "SELECT score FROM wallet_scores WHERE wallet=%s"
    results = execute_cached_query(query, (wallet,), max_age=300)
    if results:
        return float(results[0]['score'])
    else:
        return Config.DEFAULT_SCORE

@retry_db_operation()
def save_signal(token, trader_count, confidence, initial_price=None):
    """
    Guarda una señal emitida y retorna su ID.
    """
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
    """
    Guarda las características completas de una señal para análisis ML.
    """
    query = """
    INSERT INTO signal_features (signal_id, token, feature_json)
    VALUES (%s, %s, %s)
    """
    features_json = psycopg2.extras.Json(features)
    execute_cached_query(query, (signal_id, token, features_json), write_query=True)
    logger.info(f"Features guardadas para señal {signal_id} del token {token}")

@retry_db_operation()
def save_wallet_profit(wallet, token, buy_price, sell_price, profit_percent, hold_time_hours, buy_timestamp):
    """
    Registra un profit realizado por un wallet para análisis.
    """
    query = """
    INSERT INTO wallet_profits 
        (wallet, token, buy_price, sell_price, profit_percent, hold_time_hours, buy_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (wallet, token, buy_timestamp) DO NOTHING
    """
    params = (wallet, token, buy_price, sell_price, profit_percent, hold_time_hours, datetime.fromtimestamp(buy_timestamp))
    execute_cached_query(query, params, write_query=True)

@retry_db_operation()
def count_signals_today():
    """
    Cuenta cuántas señales se han emitido hoy.
    """
    query = """
    SELECT COUNT(*) FROM signals
    WHERE created_at::date = CURRENT_DATE
    """
    results = execute_cached_query(query, max_age=60)
    return results[0]['count'] if results else 0

@retry_db_operation()
def count_signals_last_hour():
    """
    Cuenta las señales emitidas en la última hora.
    """
    query = """
    SELECT COUNT(*) FROM signals
    WHERE created_at > NOW() - INTERVAL '1 HOUR'
    """
    results = execute_cached_query(query, max_age=60)
    return results[0]['count'] if results else 0

@retry_db_operation()
def count_transactions_today():
    """
    Cuenta las transacciones guardadas hoy.
    """
    query = """
    SELECT COUNT(*) FROM transactions
    WHERE created_at::date = CURRENT_DATE
    """
    results = execute_cached_query(query, max_age=60)
    return results[0]['count'] if results else 0

@retry_db_operation()
def get_token_transactions(token, hours=24):
    """
    Obtiene transacciones para un token en las últimas X horas.
    """
    query = """
    SELECT wallet, tx_type, amount_usd, created_at
    FROM transactions
    WHERE token = %s AND created_at > NOW() - INTERVAL '%s HOUR'
    ORDER BY created_at DESC
    """
    results = execute_cached_query(query, (token, hours), max_age=30)
    return [
        {
            "wallet": row['wallet'],
            "type": row['tx_type'],
            "amount_usd": float(row['amount_usd']),
            "created_at": row['created_at'].isoformat()
        } for row in results
    ]

@retry_db_operation()
def get_wallet_recent_transactions(wallet, hours=24):
    """
    Obtiene transacciones recientes de una wallet.
    """
    query = """
    SELECT token, tx_type, amount_usd, created_at
    FROM transactions
    WHERE wallet = %s AND created_at > NOW() - INTERVAL '%s HOUR'
    ORDER BY created_at DESC
    """
    results = execute_cached_query(query, (wallet, hours), max_age=30)
    return [
        {
            "token": row['token'],
            "type": row['tx_type'],
            "amount_usd": float(row['amount_usd']),
            "created_at": row['created_at'].isoformat()
        } for row in results
    ]

@retry_db_operation()
def get_wallet_profit_stats(wallet, days=30):
    """
    Obtiene estadísticas de profit para una wallet en los últimos 'days' días.
    """
    query = """
    SELECT 
        COUNT(*) as trade_count,
        AVG(profit_percent) as avg_profit,
        MAX(profit_percent) as max_profit,
        SUM(CASE WHEN profit_percent > 0 THEN 1 ELSE 0 END) as win_count,
        SUM(CASE WHEN profit_percent <= 0 THEN 1 ELSE 0 END) as loss_count,
        AVG(hold_time_hours) as avg_hold_time
    FROM wallet_profits
    WHERE wallet = %s AND sell_timestamp > NOW() - INTERVAL '%s DAY'
    """
    results = execute_cached_query(query, (wallet, days), max_age=300)
    if not results or results[0]['trade_count'] == 0:
        return None
    result = results[0]
    total_trades = result['trade_count']
    win_count = result['win_count'] or 0
    return {
        'trade_count': total_trades,
        'avg_profit': float(result['avg_profit'] or 0),
        'max_profit': float(result['max_profit'] or 0),
        'win_rate': float(win_count / total_trades) if total_trades > 0 else 0,
        'avg_hold_time': float(result['avg_hold_time'] or 0)
    }

# --- Funciones adicionales solicitadas ---

@retry_db_operation()
def get_signals_performance_stats():
    """
    Obtiene estadísticas de rendimiento de señales por timeframe.
    
    Returns:
        list: Lista de diccionarios con estadísticas por timeframe
    """
    query = """
    SELECT 
        timeframe,
        COUNT(*) as total_signals,
        ROUND(AVG(percent_change), 2) as avg_percent_change,
        ROUND(SUM(CASE WHEN percent_change > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as success_rate
    FROM signal_performance
    WHERE timestamp > NOW() - INTERVAL '30 DAY'
    GROUP BY timeframe
    ORDER BY 
        CASE 
            WHEN timeframe = '3m' THEN 1
            WHEN timeframe = '5m' THEN 2
            WHEN timeframe = '10m' THEN 3
            WHEN timeframe = '30m' THEN 4
            WHEN timeframe = '1h' THEN 5
            WHEN timeframe = '2h' THEN 6
            WHEN timeframe = '4h' THEN 7
            WHEN timeframe = '24h' THEN 8
            ELSE 9
        END
    """
    results = execute_cached_query(query, max_age=300)
    return [
        {
            "timeframe": row['timeframe'],
            "total_signals": row['total_signals'],
            "avg_percent_change": row['avg_percent_change'],
            "success_rate": row['success_rate']
        } for row in results
    ]

@retry_db_operation()
def get_signal_performance(signal_id):
    """
    Obtiene el rendimiento de una señal específica.
    
    Args:
        signal_id: ID de la señal
        
    Returns:
        list: Lista de performances de la señal por timeframe
    """
    query = """
    SELECT 
        timeframe, 
        percent_change,
        timestamp
    FROM signal_performance
    WHERE signal_id = %s
    ORDER BY timestamp ASC
    """
    results = execute_cached_query(query, (signal_id,), max_age=60)
    return [
        {
            "timeframe": row['timeframe'],
            "percent_change": row['percent_change'],
            "timestamp": row['timestamp'].isoformat() if row['timestamp'] else None
        } for row in results
    ]
    
@retry_db_operation()
def get_signal_features(signal_id):
    """
    Obtiene las features guardadas para una señal.
    """
    query = """
    SELECT feature_json
    FROM signal_features
    WHERE signal_id = %s
    LIMIT 1
    """
    results = execute_cached_query(query, (signal_id,), max_age=60)
    if results and results[0]['feature_json']:
        return results[0]['feature_json']
    return None

@retry_db_operation()
def update_setting(key, value):
    """
    Actualiza o crea un valor de configuración en la BD.
    """
    query = """
    INSERT INTO bot_settings (key, value)
    VALUES (%s, %s)
    ON CONFLICT (key) 
    DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
    """
    execute_cached_query(query, (key, value), write_query=True)
    for cache_key in list(query_cache.keys()):
        if "bot_settings" in cache_key:
            query_cache.pop(cache_key, None)
            query_cache_timestamp.pop(cache_key, None)
    return True

@retry_db_operation()
def save_failed_token(token, reason):
    """
    Guarda un token fallido en detección de señales.
    """
    query = """
    INSERT INTO failed_tokens (token, reason)
    VALUES (%s, %s)
    ON CONFLICT (token) 
    DO UPDATE SET reason = EXCLUDED.reason, created_at = NOW()
    """
    execute_cached_query(query, (token, reason), write_query=True)
    return True
