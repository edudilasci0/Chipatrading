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

# NUEVO: Cache para consultas frecuentes
query_cache = {}
query_cache_timestamp = {}
query_cache_hits = 0
query_cache_misses = 0

def init_db_pool(min_conn=1, max_conn=10):
    """
    Inicializa el pool de conexiones a la base de datos.
    
    Args:
        min_conn: Mínimo número de conexiones en el pool
        max_conn: Máximo número de conexiones en el pool
    """
    global pool
    with pool_lock:
        if pool is None:
            db_url = Config.DATABASE_PATH
            if not db_url:
                raise ValueError("DATABASE_PATH no está configurado")
                
            # Crear un pool con conexiones especificadas
            pool = psycopg2.pool.SimpleConnectionPool(min_conn, max_conn, db_url)
            logger.info(f"✅ Pool de conexiones a base de datos inicializado (min={min_conn}, max={max_conn})")

@contextmanager
def get_connection():
    """
    Obtiene una conexión del pool y la devuelve cuando termina.
    Versión mejorada con reconexión automática.
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
        # Cerrar la conexión en mal estado
        if conn:
            try:
                pool.putconn(conn, close=True)
            except:
                pass
            
        # Reinicializar el pool
        init_db_pool()
        # Intentar obtener una nueva conexión
        conn = pool.getconn()
        yield conn
    finally:
        if conn is not None:
            pool.putconn(conn)

def retry_db_operation(max_attempts=3, delay=1, backoff_factor=2):
    """
    Decorador mejorado para reintentar operaciones de BD en caso de error.
    Implementa backoff exponencial con jitter.
    
    Args:
        max_attempts: Número máximo de intentos
        delay: Retraso inicial en segundos
        backoff_factor: Factor para aumentar el retraso entre intentos
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
                    
                    # Backoff exponencial con jitter aleatorio
                    jitter = random.uniform(0.8, 1.2)
                    wait_time = current_delay * jitter
                    
                    logger.warning(f"⚠️ Error de BD: {e}. Reintentando ({attempt+1}/{max_attempts}) en {wait_time:.2f}s...")
                    time.sleep(wait_time)
                    
                    # Aumentar el delay para el próximo intento
                    current_delay *= backoff_factor
                except Exception as e:
                    # Para otros errores, solo registrar y reenviar
                    logger.error(f"Error no manejado: {e}")
                    raise
                    
            raise last_error  # No debería llegar aquí, pero por si acaso
        return wrapper
    return decorator

@retry_db_operation()
def init_db():
    """
    Crea las tablas necesarias si no existen y los índices.
    Versión optimizada con manejo mejorado de errores y logging.
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            
            # NUEVO: Verificar versión de schema/migrations
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMP DEFAULT NOW(),
                    description TEXT
                )
            """)
            
            # Verificar versión actual
            cur.execute("SELECT MAX(version) FROM schema_version")
            result = cur.fetchone()
            current_version = result[0] if result and result[0] else 0
            
            logger.info(f"Versión actual del schema: {current_version}")
            
            # Aplicar migrations incremental si es necesario
            if current_version < 1:
                logger.info("Aplicando migración #1: Tablas iniciales")
                
                # Tabla de puntuaciones de wallets
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS wallet_scores (
                        wallet TEXT PRIMARY KEY,
                        score NUMERIC,
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """)

                # Tabla de transacciones
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

                # Tabla de señales
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
                
                # Tabla de rendimiento de señales
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
                
                # Tabla de configuración del bot
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_settings (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                
                # Tabla de tokens fallidos
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS failed_tokens (
                        token TEXT PRIMARY KEY,
                        reason TEXT,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                
                # Registrar migración
                cur.execute("""
                    INSERT INTO schema_version (version, description)
                    VALUES (1, 'Tablas iniciales')
                """)
                
                current_version = 1
                logger.info("Migración #1 aplicada correctamente")
            
            # Migración #2: Mejoras
            if current_version < 2:
                logger.info("Aplicando migración #2: Mejoras y nuevas tablas")
                
                # NUEVO: Tabla de historial de profits por wallet
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
                
                # NUEVO: Tabla para mejorar análisis ML
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS signal_features (
                        id SERIAL PRIMARY KEY,
                        signal_id INTEGER REFERENCES signals(id),
                        token TEXT,
                        feature_json JSONB,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                
                # NUEVO: Tabla para calificación de tokens
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
                
                # Registrar migración
                cur.execute("""
                    INSERT INTO schema_version (version, description)
                    VALUES (2, 'Mejoras y nuevas tablas')
                """)
                
                current_version = 2
                logger.info("Migración #2 aplicada correctamente")
            
            # Crear índices para mejorar rendimiento
            try:
                # Índices existentes
                cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_token ON transactions(token)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(wallet)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_token ON signals(token)")
                
                # NUEVO: Índices para mejoras de rendimiento
                cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_wallet_token ON transactions(wallet, token)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_profits_wallet ON wallet_profits(wallet)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_features_token ON signal_features(token)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_signal_features_signal_id ON signal_features(signal_id)")
                
                logger.info("✅ Índices creados correctamente")
            except Exception as e:
                logger.error(f"⚠️ Error al crear índices: {e}")
                conn.rollback()
                
                # Intentar crear los índices uno por uno (fallback)
                for idx_name, idx_def in [
                    ("idx_transactions_token", "CREATE INDEX IF NOT EXISTS idx_transactions_token ON transactions(token)"),
                    ("idx_transactions_wallet", "CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(wallet)"),
                    ("idx_transactions_created_at", "CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)"),
                    ("idx_signals_created_at", "CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)"),
                    ("idx_signals_token", "CREATE INDEX IF NOT EXISTS idx_signals_token ON signals(token)"),
                    ("idx_transactions_wallet_token", "CREATE INDEX IF NOT EXISTS idx_transactions_wallet_token ON transactions(wallet, token)"),
                    ("idx_wallet_profits_wallet", "CREATE INDEX IF NOT EXISTS idx_wallet_profits_wallet ON wallet_profits(wallet)"),
                    ("idx_signal_features_token", "CREATE INDEX IF NOT EXISTS idx_signal_features_token ON signal_features(token)"),
                    ("idx_signal_features_signal_id", "CREATE INDEX IF NOT EXISTS idx_signal_features_signal_id ON signal_features(signal_id)")
                ]:
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
                ("signal_window_seconds", str(540)),  # 9 minutos como solicitaste
                ("min_confidence_threshold", str(Config.MIN_CONFIDENCE_THRESHOLD)),
                ("rugcheck_min_score", "50"),
                ("min_volume_usd", str(Config.MIN_VOLUME_USD)),
                ("signal_throttling", "10"),  # Nueva configuración: máximo de señales por hora
                ("adapt_confidence_threshold", "true"),  # Nueva configuración: ajuste automático
                ("high_quality_trader_score", "7.0")  # Umbral para traders de alta calidad
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
    NUEVO: Limpia la caché de consultas.
    """
    global query_cache, query_cache_timestamp, query_cache_hits, query_cache_misses
    query_cache = {}
    query_cache_timestamp = {}
    query_cache_hits = 0
    query_cache_misses = 0
    logger.info("Cache de consultas limpiada")

def get_cache_stats():
    """
    NUEVO: Obtiene estadísticas de la caché de consultas.
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
    NUEVO: Ejecuta una consulta con caché para consultas de lectura frecuentes.
    
    Args:
        query: Consulta SQL a ejecutar
        params: Parámetros para la consulta
        max_age: Tiempo máximo en segundos para la validez de la caché
        write_query: Si es True, es una consulta de escritura y no se cachea
        
    Returns:
        list: Resultados de la consulta
    """
    global query_cache, query_cache_timestamp, query_cache_hits, query_cache_misses
    
    # Las consultas de escritura no se cachean
    if write_query:
        with get_connection() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query, params or ())
            conn.commit()
            return []
    
    # Generar clave de caché
    cache_key = f"{query}:{str(params)}"
    
    # Verificar caché
    now = time.time()
    if cache_key in query_cache and cache_key in query_cache_timestamp:
        cache_age = now - query_cache_timestamp[cache_key]
        if cache_age < max_age:
            query_cache_hits += 1
            return query_cache[cache_key]
    
    # Caché no válida o no existe
    query_cache_misses += 1
    
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, params or ())
        results = [dict(row) for row in cur.fetchall()]
        
        # Guardar en caché
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
    params = (
        tx_data["wallet"],
        tx_data["token"],
        tx_data["type"],
        tx_data["amount_usd"]
    )
    
    execute_cached_query(query, params, write_query=True)
    
    # NUEVO: Invalidar caché relacionada
    # (Enfoque simplificado - en producción sería más específico)
    for key in list(query_cache.keys()):
        if "transactions" in key:
            query_cache.pop(key, None)
            query_cache_timestamp.pop(key, None)

@retry_db_operation()
def update_wallet_score(wallet, new_score):
    """
    Actualiza el score de la wallet en la tabla 'wallet_scores'.
    """
    query = """
    INSERT INTO wallet_scores (wallet, score)
    VALUES (%s, %s)
    ON CONFLICT (wallet)
    DO UPDATE SET score = EXCLUDED.score, updated_at = NOW()
    """
    execute_cached_query(query, (wallet, new_score), write_query=True)
    
    # Invalidar caché relacionada
    for key in list(query_cache.keys()):
        if "wallet_scores" in key and wallet in key:
            query_cache.pop(key, None)
            query_cache_timestamp.pop(key, None)

@retry_db_operation()
def get_wallet_score(wallet):
    """
    Retorna el score de la wallet. Si no existe, retorna el score por defecto.
    Versión optimizada con caché.
    """
    query = "SELECT score FROM wallet_scores WHERE wallet=%s"
    results = execute_cached_query(query, (wallet,), max_age=300)  # Cache por 5 minutos
    
    if results:
        return float(results[0]['score'])
    else:
        return Config.DEFAULT_SCORE

@retry_db_operation()
def save_signal(token, trader_count, confidence, initial_price=None):
    """
    Guarda un registro de una señal emitida y retorna su ID.
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
        
    # Invalidar caché relacionada
    for key in list(query_cache.keys()):
        if "signals" in key:
            query_cache.pop(key, None)
            query_cache_timestamp.pop(key, None)
    
    return signal_id

@retry_db_operation()
def save_signal_features(signal_id, token, features):
    """
    NUEVO: Guarda las características completas de una señal para análisis ML.
    
    Args:
        signal_id: ID de la señal
        token: Dirección del token
        features: Diccionario con características
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
    NUEVO: Registra un profit realizado por un wallet para análisis
    
    Args:
        wallet: Dirección del wallet
        token: Dirección del token
        buy_price: Precio de compra en USD
        sell_price: Precio de venta en USD
        profit_percent: Porcentaje de ganancia
        hold_time_hours: Tiempo de hold en horas
        buy_timestamp: Timestamp de la compra
    """
    query = """
    INSERT INTO wallet_profits 
        (wallet, token, buy_price, sell_price, profit_percent, hold_time_hours, buy_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (wallet, token, buy_timestamp) DO NOTHING
    """
    
    params = (wallet, token, buy_price, sell_price, profit_percent, 
              hold_time_hours, datetime.fromtimestamp(buy_timestamp))
    
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
    results = execute_cached_query(query, max_age=60)  # Cache por 1 minuto
    
    return results[0]['count'] if results else 0

@retry_db_operation()
def count_signals_last_hour():
    """
    Cuenta cuántas señales se han emitido en la última hora.
    """
    query = """
    SELECT COUNT(*) FROM signals
    WHERE created_at > NOW() - INTERVAL '1 HOUR'
    """
    results = execute_cached_query(query, max_age=60)  # Cache por 1 minuto
    
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
    results = execute_cached_query(query, max_age=60)  # Cache por 1 minuto
    
    return results[0]['count'] if results else 0

@retry_db_operation()
def get_token_transactions(token, hours=24):
    """
    Obtiene todas las transacciones para un token específico
    en las últimas X horas.
    """
    query = """
    SELECT wallet, tx_type, amount_usd, created_at
    FROM transactions
    WHERE token = %s AND created_at > NOW() - INTERVAL '%s HOUR'
    ORDER BY created_at DESC
    """
    
    # Este tipo de consulta cambia frecuentemente, caché más corta
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
    Obtiene todas las transacciones recientes de una wallet.
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
    NUEVO: Obtiene estadísticas de profit para un wallet
    
    Args:
        wallet: Dirección del wallet
        days: Número de días a considerar
        
    Returns:
        dict: Estadísticas de profit o None si no hay datos
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

@retry_db_operation()
def save_signal_performance(token, signal_id, timeframe, percent_change, confidence, traders_count):
    """
    Guarda el rendimiento de una señal en un timeframe específico.
    """
    allowed_timeframes = ['3m', '5m', '10m', '30m', '1h', '2h', '4h', '24h']
    if timeframe not in allowed_timeframes:
        logger.warning(f"⚠️ Timeframe no válido: {timeframe}")
        return False
    
    query = """
    INSERT INTO signal_performance (
        token, signal_id, timeframe, percent_change, confidence, traders_count
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (token, timeframe)
    DO UPDATE SET 
        percent_change = EXCLUDED.percent_change,
        confidence = EXCLUDED.confidence,
        traders_count = EXCLUDED.traders_count,
        timestamp = NOW()
    """
    
    execute_cached_query(query, (
        token, signal_id, timeframe, percent_change, confidence, traders_count
    ), write_query=True)
    
    logger.info(f"✅ Rendimiento guardado para {token} en {timeframe}: {percent_change:.2f}%")
    return True

@retry_db_operation()
def get_signals_without_outcomes(hours=48):
    """
    Obtiene señales sin resultados registrados de las últimas X horas.
    """
    query = """
    SELECT id, token, trader_count, confidence, initial_price, created_at
    FROM signals
    WHERE created_at > NOW() - INTERVAL '%s HOUR'
    AND outcome_collected = FALSE
    """
    
    results = execute_cached_query(query, (hours,), max_age=60)
    
    return [
        {
            "id": row['id'],
            "token": row['token'],
            "trader_count": row['trader_count'],
            "confidence": row['confidence'],
            "initial_price": row['initial_price'],
            "created_at": row['created_at'].isoformat()
        } for row in results
    ]

@retry_db_operation()
def mark_signal_outcome_collected(signal_id):
    """
    Marca una señal como procesada para outcomes ML.
    """
    query = """
    UPDATE signals
    SET outcome_collected = TRUE
    WHERE id = %s
    """
    
    execute_cached_query(query, (signal_id,), write_query=True)

@retry_db_operation()
def get_signal_by_token(token):
    """
    Obtiene la última señal emitida para un token específico.
    """
    query = """
    SELECT id, trader_count, confidence, initial_price, created_at
    FROM signals
    WHERE token = %s
    ORDER BY created_at DESC
    LIMIT 1
    """
    
    results = execute_cached_query(query, (token,), max_age=60)
    
    if results:
        row = results[0]
        return {
            "id": row['id'],
            "trader_count": row['trader_count'],
            "confidence": row['confidence'],
            "initial_price": row['initial_price'],
            "created_at": row['created_at'].isoformat()
        }
    return None

@retry_db_operation()
def update_token_metadata(token, token_type=None, volatility=None, max_price=None, max_volume=None):
    """
    NUEVO: Actualiza o crea metadatos para un token
    
    Args:
        token: Dirección del token
        token_type: Tipo de token (meme, defi, etc)
        volatility: Valor de volatilidad calculado
        max_price: Precio máximo histórico
        max_volume: Volumen máximo histórico
    """
    query = """
    INSERT INTO token_metadata (token, token_type, volatility, max_price, max_volume)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (token) DO UPDATE SET
        token_type = COALESCE(EXCLUDED.token_type, token_metadata.token_type),
        volatility = COALESCE(EXCLUDED.volatility, token_metadata.volatility),
        max_price = GREATEST(EXCLUDED.max_price, token_metadata.max_price),
        max_volume = GREATEST(EXCLUDED.max_volume, token_metadata.max_volume),
        last_updated = NOW()
    """
    
    execute_cached_query(query, (token, token_type, volatility, max_price, max_volume), write_query=True)

@retry_db_operation()
def get_token_metadata(token):
    """
    NUEVO: Obtiene metadatos de un token
    
    Args:
        token: Dirección del token
        
    Returns:
        dict: Metadatos del token o None si no existe
    """
    query = """
    SELECT token_type, volatility, max_price, max_volume, first_seen, last_updated
    FROM token_metadata
    WHERE token = %s
    """
    
    results = execute_cached_query(query, (token,), max_age=300)
    
    if results:
        row = results[0]
        return {
            "token_type": row['token_type'],
            "volatility": float(row['volatility']) if row['volatility'] else None,
            "max_price": float(row['max_price']) if row['max_price'] else None,
            "max_volume": float(row['max_volume']) if row['max_volume'] else None,
            "first_seen": row['first_seen'].isoformat(),
            "last_updated": row['last_updated'].isoformat(),
            "age_days": (datetime.now() - row['first_seen']).days
        }
    return None

@retry_db_operation()
def get_all_settings():
    """
    Obtiene todas las configuraciones de la base de datos.
    """
    query = "SELECT key, value FROM bot_settings"
    results = execute_cached_query(query, max_age=300)  # Cache por 5 minutos
    
    return {row['key']: row['value'] for row in results}

@retry_db_operation()
def update_setting(key, value):
    """
    Actualiza o crea un valor de configuración.
    """
    query = """
    INSERT INTO bot_settings (key, value)
    VALUES (%s, %s)
    ON CONFLICT (key) 
    DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
    """
    
    execute_cached_query(query, (key, value), write_query=True)
    
    # Invalidar caché
    for cache_key in list(query_cache.keys()):
        if "bot_settings" in cache_key:
            query_cache.pop(cache_key, None)
            query_cache_timestamp.pop(cache_key, None)
    
    return True

@retry_db_operation()
def save_failed_token(token, reason):
    """
    Guarda un token que falló en la detección de señales.
    """
    query = """
    INSERT INTO failed_tokens (token, reason)
    VALUES (%s, %s)
    ON CONFLICT (token) 
    DO UPDATE SET reason = EXCLUDED.reason, created_at = NOW()
    """
    
    execute_cached_query(query, (token, reason), write_query=True)
    return True

@retry_db_operation()
def get_failed_token(token):
    """
    Verifica si un token está en la lista de fallos recientes.
    """
    query = """
    SELECT reason, created_at
    FROM failed_tokens
    WHERE token = %s AND created_at > NOW() - INTERVAL '24 HOUR'
    """
    
    results = execute_cached_query(query, (token,), max_age=60)
    
    if results:
        row = results[0]
        return {
            "reason": row['reason'],
            "timestamp": row['created_at'].isoformat()
        }
    return None

@retry_db_operation()
def get_signals_performance_stats():
    """
    Obtiene estadísticas de rendimiento de las señales.
    """
    query = """
    SELECT 
        timeframe,
        COUNT(*) as total_signals,
        ROUND(AVG(percent_change), 2) as avg_percent_change,
        ROUND(AVG(CASE WHEN percent_change > 0 THEN 1 ELSE 0 END) * 100, 2) as success_rate
    FROM signal_performance
    WHERE timestamp > NOW() - INTERVAL '30 DAY'
    GROUP BY timeframe
    ORDER BY timeframe
    """
    
    results = execute_cached_query(query, max_age=300)  # Cache por 5 minutos
    
    return [
        {
            "timeframe": row['timeframe'],
            "total_signals": row['total_signals'],
            "avg_percent_change": row['avg_percent_change'],
            "success_rate": row['success_rate']
        } for row in results
    ]

@retry_db_operation()
def cleanup_old_data(days=90):
    """
    NUEVO: Elimina datos antiguos para mantener la base de datos eficiente
    
    Args:
        days: Número de días a mantener
        
    Returns:
        dict: Número de registros eliminados por tabla
    """
    cutoff_date = datetime.now() - timedelta(days=days)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d")
    
    tables = [
        "transactions",
        "signal_performance",
        "failed_tokens"
    ]
    
    deleted_counts = {}
    
    with get_connection() as conn:
        for table in tables:
            try:
                cur = conn.cursor()
                query = f"DELETE FROM {table} WHERE created_at < %s"
                cur.execute(query, (cutoff_str,))
                deleted_counts[table] = cur.rowcount
                conn.commit()
                logger.info(f"Eliminados {cur.rowcount} registros antiguos de {table}")
            except Exception as e:
                logger.error(f"Error limpiando tabla {table}: {e}")
                conn.rollback()
    
    return deleted_counts

def get_db_stats():
    """
    NUEVO: Obtiene estadísticas generales de la base de datos
    
    Returns:
        dict: Estadísticas de la BD
    """
    stats = {}
    
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            
            # Contar registros por tabla
            tables = ["transactions", "signals", "wallet_scores", "signal_performance", 
                     "failed_tokens", "wallet_profits", "token_metadata"]
            
            for table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    stats[f"{table}_count"] = count
                except:
                    stats[f"{table}_count"] = "Error"
            
            # Tamaño de la base de datos
            try:
                query = """
                SELECT pg_size_pretty(pg_database_size(current_database())) as db_size,
                       pg_database_size(current_database()) as db_size_bytes
                """
                cur.execute(query)
                result = cur.fetchone()
                stats["db_size_pretty"] = result[0]
                stats["db_size_bytes"] = result[1]
            except:
                stats["db_size"] = "No disponible"
            
            # Transacciones por día últimos 7 días
            try:
                query = """
                SELECT DATE(created_at) as date, COUNT(*) as count
                FROM transactions
                WHERE created_at > NOW() - INTERVAL '7 DAY'
                GROUP BY DATE(created_at)
                ORDER BY date DESC
                """
                cur.execute(query)
                stats["transactions_by_day"] = [
                    {"date": row[0].strftime("%Y-%m-%d"), "count": row[1]} 
                    for row in cur.fetchall()
                ]
            except:
                stats["transactions_by_day"] = "Error"
    
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas de BD: {e}")
        stats["error"] = str(e)
    
    # Añadir stats de caché
    stats["cache"] = get_cache_stats()
    
    return stats
