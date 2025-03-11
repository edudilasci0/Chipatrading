import time
import sqlite3

DATABASE = "tradingbot.db"

def registrar_feedback(signal_id, feedback_type, score, comentarios=""):
    timestamp = int(time.time())
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO signal_feedback (signal_id, feedback_type, score, created_at)
            VALUES (?, ?, ?, ?)
        """, (signal_id, feedback_type, score, timestamp))
        conn.commit()
        print(f"Feedback registrado para señal {signal_id}: {feedback_type}")
    except Exception as e:
        print(f"Error registrando feedback: {e}")
    finally:
        conn.close()

def obtener_feedback(signal_id=None):
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        if signal_id is not None:
            cursor.execute("""
                SELECT signal_id, feedback_type, score, created_at 
                FROM signal_feedback 
                WHERE signal_id = ?
            """, (signal_id,))
        else:
            cursor.execute("SELECT signal_id, feedback_type, score, created_at FROM signal_feedback")
        registros = cursor.fetchall()
        return registros
    except Exception as e:
        print(f"Error obteniendo feedback: {e}")
        return []
    finally:
        conn.close()

def analizar_feedback():
    registros = obtener_feedback()
    total = len(registros)
    if total == 0:
        return {"total": 0, "exito": 0, "fracaso": 0, "tasa_exito": 0.0}
    exitos = sum(1 for r in registros if r[1].lower() in ["ganadora", "exitosa"])
    fracasos = total - exitos
    tasa_exito = exitos / total
    return {"total": total, "exito": exitos, "fracaso": fracasos, "tasa_exito": round(tasa_exito, 3)}
