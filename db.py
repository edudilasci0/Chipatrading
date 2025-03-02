import os
import time
import psycopg2
import psycopg2.pool
from contextlib import contextmanager
from datetime import datetime, timedelta
from config import Config

# Pool de conexiones global
pool = None

def init_db_pool():
    """
    Inicializa el pool de conexiones a la base de datos.
    """
    global pool
    if pool is None:
        db_url = Config.DATABASE_PATH
        if not db_url:
            raise ValueError("DATABASE_PATH no está configurado")
            
        # Crear un pool con mínimo 1 y máximo 10 conexiones
        pool = psycopg2.pool.SimpleConnectionPool(1, 10, db_url)
        print("✅ Pool de conexiones a base de datos inicializado")

@contextmanager
def get_connection():
    """
    Obtiene una conexión del pool y la devuelve cuando termina.
    """
    global pool
    if pool is None:
        init_db_pool()
        
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)

def retry_db_operation(max_attempts=3, delay=1):
    """
    Decorador para reintentar operaciones de BD en caso de error.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    last_error = e
                    if attempt == max_attempts - 1:
                        raise
                    print(f"⚠️ Error de BD: {e}. Reintentando ({attempt+1}/{max_attempts})...")
                    time.sleep(delay)
            raise last_error  # No debería llegar aquí, pero por si acaso
        return wrapper
    return decorator

def init_db():
    """
    Crea las tablas necesarias si no existen y los índices.
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            
            # Recrear la tabla transactions desde cero (enfoque directo)
            try:
                # Comprobar si la tabla existe y borrarla
                cur.execute("DROP TABLE IF EXISTS transactions CASCADE")
                print("✅ Tabla transactions eliminada para recreación")
            except Exception as e:
                print(f"⚠️ Error al eliminar tabla transactions: {e}")
                conn.rollback()
            
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
            
            # Tabla de rendimiento de señales - MODIFICADA
            cur.execute("""
                CREATE TABLE IF NOT EXISTS signal_performance (
                    id SERIAL PRIMARY KEY,
                    token TEXT,
                    signal_id INTEGER REFERENCES signals(id),
                    timeframe TEXT CHECK (timeframe IN ('10m', '30m', '1h', '2h', '4h', '24h')),
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
            
            # Insertar configuraciones iniciales
            default_settings = [
                ("min_transaction_usd", str(Config.MIN_TRANSACTION_USD)),
                ("min_traders_for_signal", str(Config.MIN_TRADERS_FOR_SIGNAL)),
                ("signal_window_seconds", str(Config.SIGNAL_WINDOW_SECONDS)),
                ("min_confidence_threshold", str(Config.MIN_CONFIDENCE_THRESHOLD)),
                ("rugcheck_min_score", "50"),
                ("min_volume_usd", str(Config.MIN_VOLUME_USD))
            ]
            
            for key, value in default_settings:
                cur.execute("""
                    INSERT INTO bot_settings (key, value)
                    VALUES (%s, %s)
                    ON CONFLICT (key) DO NOTHING
                """, (key, value))
            
            try:
                # Crear índices para mejorar rendimiento
                cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_token ON transactions(token)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(wallet)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_token ON signals(token)")
                print("✅ Índices creados correctamente")
            except Exception as e:
                print(f"⚠️ Error al crear índices: {e}")
                conn.rollback()
                
                # Intentar crear los índices uno por uno
                try:
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_token ON transactions(token)")
                    print("✅ Índice idx_transactions_token creado")
                except Exception as e1:
                    print(f"⚠️ Error al crear idx_transactions_token: {e1}")
                
                try:
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(wallet)")
                    print("✅ Índice idx_transactions_wallet creado")
                except Exception as e2:
                    print(f"⚠️ Error al crear idx_transactions_wallet: {e2}")
                
                # Seguir creando el resto de índices individualmente
                # ...

            conn.commit()
            print("✅ Base de datos inicializada correctamente")
            return True
            
    except Exception as e:
        print(f"🚨 Error crítico al inicializar base de datos: {e}")
        return False

@retry_db_operation()
def save_transaction(tx_data):
    """
    Guarda una transacción en la tabla 'transactions'.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        INSERT INTO transactions (wallet, token, tx_type, amount_usd)
        VALUES (%s, %s, %s, %s)
        """
        cur.execute(sql, (
            tx_data["wallet"],
            tx_data["token"],
            tx_data["type"],
            tx_data["amount_usd"]
        ))
        conn.commit()

@retry_db_operation()
def update_wallet_score(wallet, new_score):
    """
    Actualiza el score de la wallet en la tabla 'wallet_scores'.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        INSERT INTO wallet_scores (wallet, score)
        VALUES (%s, %s)
        ON CONFLICT (wallet)
        DO UPDATE SET score = EXCLUDED.score, updated_at = NOW()
        """
        cur.execute(sql, (wallet, new_score))
        conn.commit()

@retry_db_operation()
def get_wallet_score(wallet):
    """
    Retorna el score de la wallet. Si no existe, retorna el score por defecto.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        sql = "SELECT score FROM wallet_scores WHERE wallet=%s"
        cur.execute(sql, (wallet,))
        row = cur.fetchone()
        
    if row:
        return float(row[0])
    else:
        return Config.DEFAULT_SCORE

@retry_db_operation()
def save_signal(token, trader_count, confidence, initial_price=None):
    """
    Guarda un registro de una señal emitida y retorna su ID.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        INSERT INTO signals (token, trader_count, confidence, initial_price)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """
        cur.execute(sql, (token, trader_count, confidence, initial_price))
        signal_id = cur.fetchone()[0]
        conn.commit()
        
    return signal_id

@retry_db_operation()
def count_signals_today():
    """
    Cuenta cuántas señales se han emitido hoy.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        SELECT COUNT(*) FROM signals
        WHERE created_at::date = CURRENT_DATE
        """
        cur.execute(sql)
        row = cur.fetchone()
        
    return row[0] if row else 0

@retry_db_operation()
def count_transactions_today():
    """
    Cuenta las transacciones guardadas hoy.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        SELECT COUNT(*) FROM transactions
        WHERE created_at::date = CURRENT_DATE
        """
        cur.execute(sql)
        row = cur.fetchone()
        
    return row[0] if row else 0

@retry_db_operation()
def get_token_transactions(token, hours=24):
    """
    Obtiene todas las transacciones para un token específico
    en las últimas X horas.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        SELECT wallet, tx_type, amount_usd, created_at
        FROM transactions
        WHERE token = %s AND created_at > NOW() - INTERVAL '%s HOUR'
        ORDER BY created_at DESC
        """
        cur.execute(sql, (token, hours))
        transactions = cur.fetchall()
    
    result = []
    for tx in transactions:
        result.append({
            "wallet": tx[0],
            "type": tx[1],
            "amount_usd": float(tx[2]),
            "created_at": tx[3].isoformat()
        })
    return result

@retry_db_operation()
def save_signal_performance(token, signal_id, timeframe, percent_change, confidence, traders_count):
    """
    Guarda el rendimiento de una señal en un timeframe específico.
    
    Args:
        token: Dirección del token
        signal_id: ID de la señal (opcional)
        timeframe: Intervalo de tiempo ('10m', '30m', '1h', '2h', '4h', '24h')
        percent_change: Porcentaje de cambio
        confidence: Nivel de confianza
        traders_count: Número de traders
    """
    allowed_timeframes = ['10m', '30m', '1h', '2h', '4h', '24h']
    if timeframe not in allowed_timeframes:
        print(f"⚠️ Timeframe no válido: {timeframe}")
        return False
    
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        INSERT INTO signal_performance (
            token, 
            signal_id, 
            timeframe, 
            percent_change, 
            confidence, 
            traders_count
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (token, timeframe)
        DO UPDATE SET 
            percent_change = EXCLUDED.percent_change,
            confidence = EXCLUDED.confidence,
            traders_count = EXCLUDED.traders_count,
            timestamp = NOW()
        """
        cur.execute(sql, (
            token, 
            signal_id, 
            timeframe, 
            percent_change, 
            confidence, 
            traders_count
        ))
        conn.commit()
    
    print(f"✅ Rendimiento guardado para {token} en {timeframe}: {percent_change:.2f}%")
    return True

# Resto de métodos permanecen igual

@retry_db_operation()
def get_signals_without_outcomes(hours=48):
    """
    Obtiene señales sin resultados registrados de las últimas X horas.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        SELECT id, token, trader_count, confidence, initial_price, created_at
        FROM signals
        WHERE created_at > NOW() - INTERVAL '%s HOUR'
        AND outcome_collected = FALSE
        """
        cur.execute(sql, (hours,))
        signals = cur.fetchall()
    
    result = []
    for signal in signals:
        result.append({
            "id": signal[0],
            "token": signal[1],
            "trader_count": signal[2],
            "confidence": signal[3],
            "initial_price": signal[4],
            "created_at": signal[5].isoformat()
        })
    return result

@retry_db_operation()
def mark_signal_outcome_collected(signal_id):
    """
    Marca una señal como procesada para outcomes ML.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        UPDATE signals
        SET outcome_collected = TRUE
        WHERE id = %s
        """
        cur.execute(sql, (signal_id,))
        conn.commit()

@retry_db_operation()
def get_all_settings():
    """
    Obtiene todas las configuraciones de la base de datos.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        sql = "SELECT key, value FROM bot_settings"
        cur.execute(sql)
        settings = cur.fetchall()
    
    result = {}
    for key, value in settings:
        result[key] = value
    return result

@retry_db_operation()
def update_setting(key, value):
    """
    Actualiza o crea un valor de configuración.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        INSERT INTO bot_settings (key, value)
        VALUES (%s, %s)
        ON CONFLICT (key) 
        DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """
        cur.execute(sql, (key, value))
        conn.commit()
    return True
