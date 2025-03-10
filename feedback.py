import time
import psycopg2.extras
import logging
from db import get_connection, retry_db_operation

logger = logging.getLogger("feedback")

@retry_db_operation()
def registrar_feedback(signal_id, feedback_type, score, comentarios=""):
    """
    Registra la retroalimentación para una señal en la tabla 'signal_feedback'.

    Args:
        signal_id (int): ID de la señal.
        feedback_type (str): Tipo de feedback (por ejemplo, "ganadora" o "fallida").
        score (float): Valor adicional asociado a la señal.
        comentarios (str): Comentarios opcionales.
    """
    query = """
        INSERT INTO signal_feedback (signal_id, feedback_type, score, comentarios, created_at)
        VALUES (%s, %s, %s, %s, NOW())
    """
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, (signal_id, feedback_type, score, comentarios))
            conn.commit()
        logger.info(f"Feedback registrado para señal {signal_id}: {feedback_type}")
    except Exception as e:
        logger.error(f"Error registrando feedback para señal {signal_id}: {e}")
        raise

@retry_db_operation()
def obtener_feedback(signal_id=None):
    """
    Recupera el feedback registrado.
    
    Args:
        signal_id (int, opcional): Si se proporciona, filtra el feedback para esa señal.
        
    Returns:
        list of dict: Lista de feedback con campos 'signal_id', 'feedback_type', 'score', 'comentarios' y 'created_at'.
    """
    if signal_id is not None:
        query = """
            SELECT signal_id, feedback_type, score, comentarios, created_at
            FROM signal_feedback
            WHERE signal_id = %s
        """
        params = (signal_id,)
    else:
        query = """
            SELECT signal_id, feedback_type, score, comentarios, created_at
            FROM signal_feedback
        """
        params = None

    try:
        with get_connection() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(query, params or ())
            results = cur.fetchall()
            feedback_list = []
            for row in results:
                feedback_list.append({
                    "signal_id": row["signal_id"],
                    "feedback_type": row["feedback_type"],
                    "score": float(row["score"]),
                    "comentarios": row["comentarios"],
                    "created_at": row["created_at"].isoformat() if hasattr(row["created_at"], "isoformat") else row["created_at"]
                })
            return feedback_list
    except Exception as e:
        logger.error(f"Error obteniendo feedback: {e}")
        raise

@retry_db_operation()
def analizar_feedback():
    """
    Analiza el feedback registrado para calcular métricas de desempeño.
    
    Returns:
        dict: Diccionario con total de feedback, número de éxitos, fracasos y tasa de éxito.
    """
    try:
        feedbacks = obtener_feedback()
        total = len(feedbacks)
        if total == 0:
            return {"total": 0, "exito": 0, "fracaso": 0, "tasa_exito": 0.0}
        exitos = sum(1 for fb in feedbacks if fb["feedback_type"].lower() in ["ganadora", "exitosa"])
        fracasos = total - exitos
        tasa_exito = exitos / total
        return {"total": total, "exito": exitos, "fracaso": fracasos, "tasa_exito": round(tasa_exito, 3)}
    except Exception as e:
        logger.error(f"Error analizando feedback: {e}")
        raise
