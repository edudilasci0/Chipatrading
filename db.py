import os
import time
import psycopg2
import psycopg2.pool
import psycopg2.extras
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta
import threading
import random
import json

from config import Config

# Configuración de logging
logger = logging.getLogger("database")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Variables globales para el pool de conexiones y cache de consultas
pool = None
pool_lock = threading.Lock()
query_cache = {}
query_cache_timestamp = {}
query_cache_hits = 0
query_cache_misses = 0

def init_db_pool(min_conn=1, max_conn=10):
    """Inicializa el pool de conexiones a la base de datos."""
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
    """Proporciona un contexto para obtener y liberar una conexión del pool."""
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
    Decorador para reintentar operaciones de base de datos en caso de error.
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
    """
    Inicializa la base de datos: crea tablas, aplica migraciones y crea índices.
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
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
                        created_at TIMESTAMP DEFAULT NOW(),
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
            
            # Crear índices
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
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_trending_tokens_token ON trending_tokens(token)")
                logger.info("✅ Índices creados correctamente")
            except Exception as e:
                logger.error(f"⚠️ Error al crear índices: {e}")
                conn.rollback()
            
            # Configurar valores predeterminados en bot_settings
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
    """Limpia la caché de consultas."""
    global query_cache, query_cache_timestamp, query_cache_hits, query_cache_misses
    query_cache = {}
    query_cache_timestamp = {}
    query_cache_hits = 0
    query_cache_misses = 0
    logger.info("Cache de consultas limpiada")

def get_cache_stats():
    """Retorna estadísticas de la caché de consultas."""
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
    Ejecuta una consulta a la base de datos con cacheo.
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

# ================= FUNCIONES NUEVAS =================

@retry_db_operation()
def get_recent_untracked_signals(hours=24):
    """
    Obtiene señales recientes que no han sido marcadas como procesadas (untracked)
    en las últimas 'hours' horas.
    
    Args:
        hours (int): Número de horas a considerar.
        
    Returns:
        list: Lista de señales recientes.
    """
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
def mark_signal_outcome_collected(signal_id):
    """
    Marca una señal como procesada para análisis de outcome.
    
    Args:
        signal_id: ID de la señal a marcar.
        
    Returns:
        bool: True si se actualizó correctamente.
    """
    query = """
    UPDATE signals 
    SET outcome_collected = TRUE
    WHERE id = %s
    """
    execute_cached_query(query, (signal_id,), write_query=True)
    return True

@retry_db_operation()
def get_signals_without_outcomes():
    """
    Obtiene las señales que aún no tienen outcome recolectado.
    
    Returns:
        list: Lista de señales sin outcome.
    """
    query = """
    SELECT id, token, trader_count, confidence, initial_price, created_at
    FROM signals
    WHERE outcome_collected = FALSE
      AND created_at < NOW() - INTERVAL '4 HOURS'
    ORDER BY created_at
    LIMIT 20
    """
    results = execute_cached_query(query, max_age=60)
    return results

@retry_db_operation()
def get_signal_performance(signal_id):
    """
    Obtiene el rendimiento de una señal específica.
    
    Args:
        signal_id: ID de la señal.
        
    Returns:
        list: Lista de rendimientos en diferentes timeframes.
    """
    query = """
    SELECT timeframe, percent_change, confidence, traders_count, created_at
    FROM signal_performance
    WHERE signal_id = %s
    ORDER BY created_at ASC
    """
    results = execute_cached_query(query, (signal_id,), max_age=60)
    return results

@retry_db_operation()
def get_signal_features(signal_id):
    """
    Obtiene las features de una señal específica.
    
    Args:
        signal_id: ID de la señal.
        
    Returns:
        dict: Features de la señal o None si no existe.
    """
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
    """
    Obtiene estadísticas de rendimiento de señales.
    
    Returns:
        list: Estadísticas de rendimiento por timeframe.
    """
    query = """
    SELECT 
        timeframe,
        COUNT(*) as total_signals,
        AVG(percent_change) as avg_percent_change,
        SUM(CASE WHEN percent_change > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate
    FROM signal_performance
    WHERE created_at > NOW() - INTERVAL '7 DAYS'
    GROUP BY timeframe
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
            "avg_percent_change": round(row["avg_percent_change"], 2),
            "success_rate": round(row["success_rate"], 2)
        })
    return formatted_results

@retry_db_operation()
def update_setting(key, value):
    """
    Actualiza un valor de configuración en la base de datos.
    
    Args:
        key: Clave de configuración.
        value: Valor a establecer.
        
    Returns:
        bool: True si se actualizó correctamente.
    """
    query = """
    INSERT INTO bot_settings (key, value)
    VALUES (%s, %s)
    ON CONFLICT (key)
    DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
    """
    execute_cached_query(query, (key, str(value)), write_query=True)
    if hasattr(Config, '_dynamic_config'):
        Config._dynamic_config[key] = value
    return True

@retry_db_operation()
def get_wallet_profit_stats(wallet, days=30):
    """
    Obtiene estadísticas de profit para un wallet.
    
    Args:
        wallet: Dirección del wallet.
        days: Número de días a considerar (default: 30).
        
    Returns:
        dict: Estadísticas de profit o None si no hay datos.
    """
    query = """
    SELECT 
        COUNT(*) as trade_count,
        AVG(profit_percent) as avg_profit,
        MAX(profit_percent) as max_profit,
        SUM(CASE WHEN profit_percent > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
        AVG(hold_time_hours) as avg_hold_time
    FROM wallet_profits
    WHERE wallet = %s
      AND sell_timestamp > NOW() - INTERVAL '%s DAYS'
    """
    results = execute_cached_query(query, (wallet, days), max_age=300)
    if results and results[0]["trade_count"] > 0:
        return {
            "trade_count": results[0]["trade_count"],
            "avg_profit": results[0]["avg_profit"],
            "max_profit": results[0]["max_profit"],
            "win_rate": results[0]["win_rate"],
            "avg_hold_time": results[0]["avg_hold_time"]
        }
    return None

@retry_db_operation()
def get_wallet_recent_transactions(wallet, hours=24):
    """
    Obtiene transacciones recientes para un wallet.
    
    Args:
        wallet: Dirección del wallet.
        hours: Número de horas a considerar (default: 24).
        
    Returns:
        list: Transacciones recientes.
    """
    query = """
    SELECT wallet, token, tx_type, amount_usd, created_at
    FROM transactions
    WHERE wallet = %s
      AND created_at > NOW() - INTERVAL '%s HOURS'
    ORDER BY created_at DESC
    """
    results = execute_cached_query(query, (wallet, hours), max_age=60)
    return results

@retry_db_operation()
def get_token_transactions(token, hours=1):
    """
    Obtiene transacciones recientes para un token.
    
    Args:
        token: Dirección del token.
        hours: Número de horas a considerar (default: 1).
        
    Returns:
        list: Transacciones recientes.
    """
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
def get_trader_name_from_wallet(wallet):
    """
    Obtiene el nombre humano asociado a un wallet.
    
    Args:
        wallet: Dirección del wallet.
        
    Returns:
        str: Nombre del trader o la wallet si no se encuentra.
    """
    known_wallets = {
        "DfMxre4cKmvogbLrPigxmibVTTQDuzjdXojWzjCXXhzj": "Euros",
        "AJ6MGExeK7FXmeKkKPmALjcdXVStXYokYNv9uVfDRtvo": "Tim",
        "73LnJ7G9ffBDjEBGgJDdgvLUhD5APLonKrNiHsKDCw5B": "Waddles",
        "99i9uVA7Q56bY22ajKKUfTZTgTeP5yCtVGsrG9J4pDYQ": "Zrool",
    }
    if wallet in known_wallets:
        return known_wallets[wallet]
    
    query = """
    SELECT name FROM traders WHERE wallet = %s LIMIT 1
    """
    try:
        results = execute_cached_query(query, (wallet,), max_age=3600)
        if results and "name" in results[0]:
            return results[0]["name"]
    except Exception:
        pass
    return wallet

@retry_db_operation()
def get_tokens_with_high_liquidity(min_liquidity=20000, limit=10):
    """
    Obtiene tokens con alta liquidez.
    
    Args:
        min_liquidity: Liquidez mínima en USD.
        limit: Número máximo de tokens a retornar.
        
    Returns:
        list: Tokens con alta liquidez.
    """
    query = """
    SELECT token, total_liquidity_usd
    FROM token_liquidity
    WHERE total_liquidity_usd >= %s
      AND created_at > NOW() - INTERVAL '1 DAY'
    ORDER BY total_liquidity_usd DESC
    LIMIT %s
    """
    results = execute_cached_query(query, (min_liquidity, limit), max_age=300)
    return results

# ================= FUNCIONES NUEVAS PARA SCORING =================

@retry_db_operation()
def get_wallet_score(wallet):
    """
    Obtiene el score actual de un wallet desde la base de datos.
    Si no existe, devuelve el score por defecto.
    
    Args:
        wallet: Dirección del wallet.
        
    Returns:
        float: Score del wallet (0-10).
    """
    query = """
    SELECT score FROM wallet_scores WHERE wallet = %s
    """
    results = execute_cached_query(query, (wallet,), max_age=300)
    if results and results[0]["score"] is not None:
        return float(results[0]["score"])
    default_score = float(Config.get("DEFAULT_SCORE", 5.0))
    return default_score

@retry_db_operation()
def update_wallet_score(wallet, score):
    """
    Actualiza el score de un wallet en la base de datos.
    
    Args:
        wallet: Dirección del wallet.
        score: Nuevo score (0-10).
        
    Returns:
        bool: True si la operación fue exitosa.
    """
    query = """
    INSERT INTO wallet_scores (wallet, score, updated_at)
    VALUES (%s, %s, NOW())
    ON CONFLICT (wallet)
    DO UPDATE SET score = %s, updated_at = NOW()
    """
    try:
        execute_cached_query(query, (wallet, score, score), write_query=True)
        return True
    except Exception as e:
        logger.error(f"Error actualizando score del wallet: {e}")
        return False

if __name__ == "__main__":
    # Pruebas básicas (opcional)
    print("Prueba de db.py")
    print("Signals hoy:", count_signals_today())
    print("Transacciones hoy:", count_transactions_today())
