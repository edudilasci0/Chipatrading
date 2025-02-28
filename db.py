# db.py
import os
import psycopg2

def get_connection():
    """
    Crea y retorna la conexión a PostgreSQL usando la variable de entorno DATABASE_PATH.
    e.g. postgres://user:pass@hostname:5432/dbname
    """
    db_url = os.environ.get("DATABASE_PATH")
    if not db_url:
        raise ValueError("DATABASE_PATH no está configurado en variables de entorno.")
    conn = psycopg2.connect(db_url)
    return conn

def init_db():
    """
    Crea las tablas 'transactions' y 'wallet_scores' si no existen.
    """
    conn = get_connection()
    cur = conn.cursor()

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

    conn.commit()
    cur.close()
    conn.close()

def save_transaction(tx_data):
    """
    Guarda una transacción en la tabla 'transactions'.
    tx_data: dict con keys: wallet, token, type, amount_usd
    """
    conn = get_connection()
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
    cur.close()
    conn.close()

def update_wallet_score(wallet, new_score):
    """
    Actualiza el score de la wallet en la tabla 'wallet_scores'.
    """
    conn = get_connection()
    cur = conn.cursor()
    sql = """
    INSERT INTO wallet_scores (wallet, score)
    VALUES (%s, %s)
    ON CONFLICT (wallet)
    DO UPDATE SET score = EXCLUDED.score, updated_at = NOW()
    """
    cur.execute(sql, (wallet, new_score))
    conn.commit()
    cur.close()
    conn.close()

def get_wallet_score(wallet):
    """
    Retorna el score de la wallet. Si no existe, retorna 5.0 por defecto.
    """
    conn = get_connection()
    cur = conn.cursor()
    sql = "SELECT score FROM wallet_scores WHERE wallet=%s"
    cur.execute(sql, (wallet,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return float(row[0])
    else:
        return 5.0

# Funciones extra para el resumen diario
def count_signals_today():
    """
    Si deseas contar cuántas transacciones se registraron hoy (ej. para resumen diario).
    """
    conn = get_connection()
    cur = conn.cursor()
    sql = """
    SELECT COUNT(*) FROM transactions
    WHERE created_at::date = CURRENT_DATE
    """
    cur.execute(sql)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else 0

def count_daily_runners_today():
    """
    Placeholder. Depende de tu definición de daily runner.
    """
    return 0
