import os
import requests
import time
import logging
import json  # Añadido para usar json.dumps en send_telegram_button
from config import Config

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("telegram_utils")

# Historial de mensajes para evitar duplicados
message_history = []
MAX_HISTORY = 100

def send_telegram_message(text: str, parse_mode="Markdown", retry_count=3, disable_notification=False):
    """
    Envía un mensaje a Telegram con reintentos en caso de fallo.
    
    Args:
        text: Texto del mensaje
        parse_mode: Formato del mensaje ("Markdown" o "HTML")
        retry_count: Número de reintentos en caso de fallo
        disable_notification: Si es True, el mensaje se envía silenciosamente
        
    Returns:
        bool: True si se envió correctamente, False si no
    """
    # Verificar configuración
    if not Config.TELEGRAM_BOT_TOKEN or not Config.TELEGRAM_CHAT_ID:
        logger.warning("No se puede enviar mensaje: TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID no configurados")
        return False
    
    # Verificar si es un mensaje duplicado (mismos primeros 50 caracteres en los últimos 5 mensajes)
    message_prefix = text[:50]
    recent_messages = message_history[-5:] if len(message_history) > 5 else message_history
    
    if message_prefix in [m[:50] for m in recent_messages]:
        logger.info("Mensaje duplicado detectado, omitiendo envío")
        return False
    
    # Construir URL y payload
    url = f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": Config.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_notification": disable_notification
    }
    
    # Intentar enviar el mensaje con reintentos
    for attempt in range(retry_count):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            
            if resp.status_code == 200:
                logger.info("✅ Mensaje enviado a Telegram con éxito")
                
                # Guardar en historial para evitar duplicados
                message_history.append(text)
                if len(message_history) > MAX_HISTORY:
                    message_history.pop(0)  # Eliminar el más antiguo
                    
                return True
            else:
                logger.warning(f"⚠️ Error al enviar mensaje (intento {attempt+1}/{retry_count}): {resp.text}")
                
                # Si es un error de formato de mensaje, intentamos sin formato
                if resp.status_code == 400 and "can't parse entities" in resp.text.lower():
                    logger.info("🔄 Reintentando sin formato...")
                    payload["parse_mode"] = ""
                    continue
                    
                # Si es un error de longitud, truncar el mensaje
                if resp.status_code == 400 and "message is too long" in resp.text.lower():
                    logger.info("🔄 Mensaje demasiado largo, truncando...")
                    payload["text"] = text[:4000] + "..."  # Telegram tiene límite de 4096 caracteres
                    continue
                
        except Exception as e:
            logger.error(f"🚨 Excepción al enviar mensaje (intento {attempt+1}/{retry_count}): {e}")
        
        # Esperar antes de reintentar (excepto en el último intento)
        if attempt < retry_count - 1:
            time.sleep(2)
    
    return False

def send_telegram_photo(image_url, caption=None, parse_mode="Markdown"):
    """
    Envía una imagen a Telegram por URL.
    Útil para enviar gráficos de volumen o precio.
    
    Args:
        image_url: URL de la imagen
        caption: Texto opcional para la imagen
        parse_mode: Formato del texto ("Markdown" o "HTML")
        
    Returns:
        bool: True si se envió correctamente, False si no
    """
    if not Config.TELEGRAM_BOT_TOKEN or not Config.TELEGRAM_CHAT_ID:
        logger.warning("⚠️ No se puede enviar imagen: TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID no configurados")
        return False
        
    url = f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendPhoto"
    payload = {
        "chat_id": Config.TELEGRAM_CHAT_ID,
        "photo": image_url
    }
    
    if caption:
        payload["caption"] = caption
        payload["parse_mode"] = parse_mode
    
    # Implementar reintentos
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                logger.info("✅ Imagen enviada a Telegram con éxito")
                return True
            else:
                logger.warning(f"⚠️ Error al enviar imagen: {resp.text}")
                if attempt < max_attempts - 1:
                    logger.info(f"Reintentando ({attempt+1}/{max_attempts})...")
                    time.sleep(2)
                else:
                    return False
        except Exception as e:
            logger.error(f"🚨 Excepción al enviar imagen: {e}")
            if attempt < max_attempts - 1:
                logger.info(f"Reintentando ({attempt+1}/{max_attempts})...")
                time.sleep(2)
            else:
                return False
    
    return False

def send_telegram_button(text, button_text, button_url, parse_mode="Markdown"):
    """
    Envía un mensaje con un botón que redirige a una URL.
    
    Args:
        text: Texto del mensaje
        button_text: Texto del botón
        button_url: URL a la que redirige el botón
        parse_mode: Formato del mensaje
        
    Returns:
        bool: True si se envió correctamente, False si no
    """
    if not Config.TELEGRAM_BOT_TOKEN or not Config.TELEGRAM_CHAT_ID:
        logger.warning("⚠️ No se puede enviar mensaje con botón: TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID no configurados")
        return False
        
    url = f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendMessage"
    
    # Crear keyboard inline con el botón
    keyboard = {
        "inline_keyboard": [
            [
                {
                    "text": button_text,
                    "url": button_url
                }
            ]
        ]
    }
    
    payload = {
        "chat_id": Config.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "reply_markup": json.dumps(keyboard)
    }
    
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info("✅ Mensaje con botón enviado a Telegram con éxito")
            return True
        else:
            logger.warning(f"⚠️ Error al enviar mensaje con botón: {resp.text}")
            return False
    except Exception as e:
        logger.error(f"🚨 Excepción al enviar mensaje con botón: {e}")
        return False
