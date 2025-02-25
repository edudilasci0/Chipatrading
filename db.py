# db.py

import os
import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    token_symbol = Column(String, nullable=False)
    token_mint = Column(String, nullable=False)
    usd_value = Column(Float, nullable=False)
    transaction_type = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False)

# Se obtiene la URL de la base de datos desde la variable de entorno
DATABASE_PATH = os.getenv("DATABASE_PATH", "sqlite:///default.db")
engine = create_engine(DATABASE_PATH)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """Crea las tablas en la base de datos (si no existen)."""
    Base.metadata.create_all(bind=engine)
    logging.info("Base de datos inicializada.")

def save_transaction(transaction_data, send_telegram_alert):
    """
    Guarda la transacción en la base de datos y envía una alerta a Telegram.
    
    :param transaction_data: dict con la información de la transacción.
    :param send_telegram_alert: función para enviar alertas.
    """
    session = SessionLocal()
    try:
        ts = transaction_data.get("timestamp")
        if not isinstance(ts, datetime):
            try:
                ts = datetime.fromisoformat(ts)
            except Exception:
                ts = datetime.now()

        new_tx = Transaction(
            token_symbol=transaction_data.get("symbol", "N/D"),
            token_mint=transaction_data.get("mint", "N/D"),
            usd_value=transaction_data.get("usd_value", 0),
            transaction_type=transaction_data.get("type", "N/D"),
            timestamp=ts
        )
        session.add(new_tx)
        session.commit()
        logging.info("Transacción insertada: %s", transaction_data.get("id", "N/D"))
        send_telegram_alert(f"Transacción insertada: {transaction_data.get('symbol', 'N/D')}")
    except Exception as e:
        session.rollback()
        logging.exception("Error al guardar la transacción: %s", e)
        send_telegram_alert("Error al guardar la transacción.")
    finally:
        session.close()
