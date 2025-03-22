# Solana Alpha Bot - Bot de Trading Cuantitativo Avanzado

Un bot de trading cuantitativo sofisticado para Solana que detecta oportunidades de trading en tiempo real utilizando análisis de patrones de transacciones, actividad de ballenas, métricas de mercado y múltiples factores técnicos.

## 🌟 Características

- **Detección de Señales Avanzada** - Identifica potenciales oportunidades de trading basadas en actividad de traders de calidad
- **Análisis de Ballenas** - Monitoreo de actividad de wallets de alta influencia y su impacto en el mercado
- **Métricas de Mercado en Tiempo Real** - Seguimiento de liquidez, crecimiento de holders y tokens en trending
- **Análisis Técnico** - Detección de patrones de precio y volumen, rupturas de resistencias y soportes
- **Clasificación de Señales por Niveles** - Categorización en S, A, B y C según la calidad y confianza
- **Monitoreo de Rendimiento** - Seguimiento en tiempo real de la evolución de señales con informes detallados
- **Integración de DEX** - Datos de liquidez y rutas de swap de Raydium y Jupiter
- **Notificaciones en Telegram** - Alertas enriquecidas con todos los datos relevantes
- **Sistema de Scoring Inteligente** - Evaluación continua de la calidad de traders basada en su historial

## 🔧 Tecnologías Utilizadas

- **Python** - Lenguaje base para todo el desarrollo
- **PostgreSQL** - Base de datos para almacenamiento y análisis
- **WebSockets** - Conexiones en tiempo real con Cielo Finance
- **APIs** - Integración con Helius, GMGN, DexScreener y más
- **Telegram** - Para notificaciones y controles
- **Solana** - Blockchain objetivo para el análisis

## 📊 Arquitectura del Sistema

El bot está compuesto por varios módulos especializados:

- **`signal_logic.py`** - Núcleo del bot, coordina todos los análisis
- **`whale_detector.py`** - Detecta actividad de ballenas y su impacto
- **`market_metrics.py`** - Analiza métricas de mercado (liquidez, holders, trending)
- **`token_analyzer.py`** - Analiza patrones técnicos de precio y volumen
- **`trader_profiler.py`** - Crea y mantiene perfiles de actividad de traders
- **`dex_monitor.py`** - Integra datos de DEX (Raydium, Jupiter)
- **`performance_tracker.py`** - Monitorea rendimiento de señales en tiempo real
- **`scoring.py`** - Sistema de puntuación de traders y tokens

## 🚀 Instalación

### Prerrequisitos

- Python 3.9+
- PostgreSQL
- Claves API: Cielo, Helius

### Configuración

1. Clona el repositorio:
```bash
git clone https://github.com/yourusername/solana-alpha-bot.git
cd solana-alpha-bot

📝 Configuración Avanzada
Ajustes de Análisis
Puedes personalizar diversos parámetros del bot en la tabla bot_settings de la base de datos:

min_confidence_threshold - Umbral mínimo de confianza para generar una señal
trader_quality_weight - Peso de la calidad de traders en el cálculo de confianza
whale_activity_weight - Peso de la actividad de ballenas en el cálculo de confianza
liquidity_healthy_threshold - Umbral para considerar liquidez saludable en USD

Configuración de Notificaciones
Modifica el formato y detalle de las notificaciones en telegram_utils.py:

Señales - Edita send_enhanced_signal() para personalizar el formato
Actualizaciones - Modifica send_performance_report() para las actualizaciones

🔎 Uso
Ejecuta el bot con:
bashCopiarpython main.py
Comandos de Telegram

/start - Activa el bot
/stop - Desactiva el bot
/status - Muestra el estado actual del bot
/stats - Muestra estadísticas de rendimiento

📋 Ampliaciones Futuras Posibles

Sistema de Backtesting - Para validar estrategias con datos históricos
Señales de Salida - Recomendaciones sobre cuándo vender los activos
Integración con Exchanges - Trading automatizado
Adaptación para Otras Blockchains - Ethereum, BSC, etc.
UI Web - Panel de control web para el monitoreo y gestión

📊 Rendimiento de Señales
El sistema clasifica las señales en 4 niveles:

Nivel S: Confianza ≥ 0.9 - Altamente probables
Nivel A: Confianza ≥ 0.8 - Muy probables
Nivel B: Confianza ≥ 0.6 - Probables
Nivel C: Confianza ≥ 0.3 - Posibles oportunidades

📈 Monitoreo de Rendimiento
El bot rastrea cada señal y envía actualizaciones en los siguientes intervalos:

3 min, 5 min, 10 min, 30 min, 1h, 2h, 4h y 24h

Las actualizaciones incluyen:

Cambio porcentual
Volumen y liquidez actuales
Actividad de ballenas
Crecimiento de holders
Tendencia del precio

📚 Estructura del Proyecto
Copiartrading-bot/
├── main.py                  # Punto de entrada principal
├── config.py                # Configuración centralizada
├── db.py                    # Funciones de base de datos
├── signal_logic.py          # Lógica central de señales
├── performance_tracker.py   # Seguimiento de rendimiento
├── scoring.py               # Sistema de puntuación
├── whale_detector.py        # Detector de actividad de ballenas
├── market_metrics.py        # Analizador de métricas de mercado
├── token_analyzer.py        # Análisis técnico de tokens
├── trader_profiler.py       # Perfiles de traders
├── dex_monitor.py           # Integración con DEX
├── telegram_utils.py        # Utilidades de Telegram
├── cielo_api.py             # Cliente de Cielo Finance
├── helius_client.py         # Cliente de Helius
├── requirements.txt         # Dependencias
└── traders_data.json        # Wallets de traders a seguir
📜 Licencia
Este proyecto está licenciado bajo MIT License.
👨‍💻 Contribuciones
Las contribuciones son bienvenidas. Por favor, abra un issue para discutir las funcionalidades propuestas antes de enviar un PR.
⚠️ Descargo de Responsabilidad
Este bot es una herramienta de análisis y no constituye asesoramiento financiero. Utilícelo bajo su propio riesgo.
