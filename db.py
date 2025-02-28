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

def fix_database_schema():
    """
    Corrige la estructura de la base de datos para asegurar compatibilidad.
    Se ejecuta antes de init_db para manejar migraciones y correcciones.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            # 1. Verificar tabla transactions
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'transactions')")
            if cur.fetchone()[0]:
                # Verificar y corregir columnas
                columns_to_check = [
                    ("wallet", "TEXT"),
                    ("token", "TEXT"),
                    ("tx_type", "TEXT"),
                    ("amount_usd", "NUMERIC"),
                    ("created_at", "TIMESTAMP")
                ]
                
                for column_name, column_type in columns_to_check:
                    cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'transactions' AND column_name = '{column_name}')")
                    if not cur.fetchone()[0]:
                        print(f"⚠️ Columna '{column_name}' falta en 'transactions'. Agregando...")
                        cur.execute(f"ALTER TABLE transactions ADD COLUMN {column_name} {column_type}")
                        if column_name == "created_at":
                            cur.execute("ALTER TABLE transactions ALTER COLUMN created_at SET DEFAULT NOW()")
                        conn.commit()
                        print(f"✅ Columna '{column_name}' agregada.")
            
            # 2. Verificar tabla signals
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'signals')")
            if cur.fetchone()[0]:
                # Verificar y corregir columnas
                columns_to_check = [
                    ("token", "TEXT"),
                    ("trader_count", "INTEGER"),
                    ("confidence", "NUMERIC"),
                    ("initial_price", "NUMERIC"),
                    ("created_at", "TIMESTAMP"),
                    ("outcome_collected", "BOOLEAN")
                ]
                
                for column_name, column_type in columns_to_check:
                    cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'signals' AND column_name = '{column_name}')")
                    if not cur.fetchone()[0]:
                        print(f"⚠️ Columna '{column_name}' falta en 'signals'. Agregando...")
                        cur.execute(f"ALTER TABLE signals ADD COLUMN {column_name} {column_type}")
                        if column_name == "created_at":
                            cur.execute("ALTER TABLE signals ALTER COLUMN created_at SET DEFAULT NOW()")
                        elif column_name == "outcome_collected":
                            cur.execute("ALTER TABLE signals ALTER COLUMN outcome_collected SET DEFAULT FALSE")
                        conn.commit()
                        print(f"✅ Columna '{column_name}' agregada.")
            
            # Si todo salió bien, confirmar transacción
            conn.commit()
            print("✅ Corrección de esquema completada.")
            
        except Exception as e:
            conn.rollback()
            print(f"🚨 Error al corregir esquema: {e}")
            # En este caso, podemos intentar un enfoque más radical
            try:
                print("⚠️ Intentando enfoque alternativo para la tabla transactions...")
                # Verificar si podemos hacer backup de datos existentes
                cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'transactions')")
                if cur.fetchone()[0]:
                    # Crear tabla de respaldo para datos existentes
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS transactions_backup AS
                        SELECT * FROM transactions
                    """)
                    # Contar registros respaldados
                    cur.execute("SELECT COUNT(*) FROM transactions_backup")
                    backup_count = cur.fetchone()[0]
                    print(f"✅ Se respaldaron {backup_count} registros de transactions.")
                    
                    # Eliminar tabla problemática
                    cur.execute("DROP TABLE transactions")
                    print("✅ Tabla transactions eliminada para recreación.")
                
                conn.commit()
                return True
            except Exception as backup_error:
                conn.rollback()
                print(f"🚨 Error al intentar enfoque alternativo: {backup_error}")
                return False

def init_db():
    """
    Crea las tablas necesarias si no existen y los índices.
    """
    # Primero intentar corregir cualquier problema de esquema
    fix_database_schema()
    
    with get_connection() as conn:
        cur = conn.cursor()

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
                timeframe TEXT,
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
        
        # Intentar crear índices con manejo seguro de errores
        try:
            # Crear índices para mejorar rendimiento
            cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_token ON transactions(token)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(wallet)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_token ON signals(token)")
        except Exception as e:
            print(f"⚠️ Error al crear índices: {e}")
            # Podemos continuar incluso si los índices fallan, el sistema seguirá funcionando
            conn.rollback()
            # Solo commit las operaciones principales
            conn.commit()
            print("✅ Base de datos inicializada sin índices.")
            return True

        conn.commit()
        
    print("✅ Base de datos inicializada correctamente")
    
    # Verificar si hay datos de respaldo para restaurar
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'transactions_backup')")
            if cur.fetchone()[0]:
                # Verificar si la tabla de respaldo tiene datos
                cur.execute("SELECT COUNT(*) FROM transactions_backup")
                backup_count = cur.fetchone()[0]
                
                if backup_count > 0:
                    print(f"ℹ️ Encontrados {backup_count} registros en respaldo, intentando restaurar...")
                    # Restaurar datos desde el backup
                    cur.execute("""
                        INSERT INTO transactions (wallet, token, tx_type, amount_usd, created_at)
                        SELECT wallet, token, tx_type, amount_usd, created_at
                        FROM transactions_backup
                    """)
                    conn.commit()
                    print("✅ Datos restaurados correctamente.")
                    
                    # Opcional: eliminar la tabla de respaldo
                    cur.execute("DROP TABLE transactions_backup")
                    conn.commit()
                    print("✅ Tabla de respaldo eliminada.")
        except Exception as e:
            conn.rollback()
            print(f"⚠️ Error al restaurar datos: {e}")
    
    return True

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
        WHERE token = %s AND created_at > NOW() - INTERVAL %s HOUR
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
    """
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
        INSERT INTO signal_performance (token, signal_id, timeframe, percent_change, confidence, traders_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (token, timeframe)
        DO UPDATE SET 
            percent_change = EXCLUDED.percent_change,
            confidence = EXCLUDED.confidence,
            traders_count = EXCLUDED.traders_count,
            timestamp = NOW()
        """
        cur.execute(sql, (token, signal_id, timeframe, percent_change, confidence, traders_count))
        conn.commit()

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
        WHERE created_at > NOW() - INTERVAL %s HOUR
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
