# Trading Bot Cuantitativo para Solana

Este proyecto es un bot de trading cuantitativo que utiliza diversas APIs (Cielo, RugCheck y Dexscreener) para detectar oportunidades en tokens nuevos y enviar alertas a Telegram.

## Estructura del Proyecto

- `requirements.txt`: Dependencias de Python.
- `wallet_tracker.py`: Módulo para gestionar traders.
- `cielo_api.py`: Módulo para interactuar con la API de Cielo.
- `rugcheck.py`: Módulo para autenticar y validar la seguridad del contrato mediante RugCheck.
- `main.py`: Lógica principal que integra todos los módulos.
- `render.yaml`: Configuración para desplegar en Render.
- `traders_data.json`: Base de datos en JSON para los traders.

## Variables de Entorno

En Render, debes configurar al menos las siguientes variables:

- `TELEGRAM_BOT_TOKEN`: Token de tu bot de Telegram.
- `TELEGRAM_CHAT_ID`: ID del chat donde se enviarán las alertas.
- `CIELO_API_KEY`: (Opcional) API Key para la API de Cielo.
- `SOLANA_PRIVATE_KEY`: Clave privada de tu wallet Solana (en formato string, asegúrate de almacenarla de forma segura).
- `SOLANA_PUBLIC_KEY`: Clave pública de tu wallet Solana.
- `DATABASE_URL`: URL de conexión a la base de datos PostgreSQL (para almacenar datos históricos y registros).

## Ejecución

Para ejecutar el bot localmente:

```bash
pip install -r requirements.txt
python main.py
