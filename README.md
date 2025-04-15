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

# Análisis y Recomendaciones de Refactorización para Chipatrading

## Situación Actual

He realizado una revisión exhaustiva del código del bot de trading en Solana y he identificado varios patrones, fortalezas y oportunidades de mejora. El sistema tiene una buena base con componentes bien definidos, pero presenta algunas inconsistencias y áreas que podrían optimizarse.

## Fortalezas del Sistema

- **Arquitectura modular**: Los componentes están bien separados en archivos específicos.
- **Manejo de configuración centralizado**: Uso de `Config` para gestionar variables de entorno y configuraciones.
- **Sistemas de caché**: Implementación de caché para optimizar consultas a APIs externas.
- **Logs detallados**: Buen uso de logs para diagnóstico y seguimiento.
- **Gestión de errores**: Manejo de excepciones y reintentos en operaciones críticas.

## Áreas de Mejora Identificadas

1. **Dependencias Circulares**: Algunos módulos se importan mutuamente.
2. **Inconsistencias en la API**: Algunos componentes tienen métodos asincrónicos y otros síncronos.
3. **Duplicación de Código**: Funcionalidades similares implementadas de diferentes maneras.
4. **Manejo de Estado**: Control de estado disperso a través de múltiples componentes.
5. **Inicialización de Componentes**: Proceso de inicialización inconsistente.
6. **Gestión de Wallets**: Fragmentada entre `wallet_tracker.py` y `wallet_manager.py`.

## Plan de Refactorización

### 1. Estandarización de Interfaces

#### Interfaz para Fuentes de Datos
```python
from abc import ABC, abstractmethod

class DataSourceInterface(ABC):
    @abstractmethod
    async def connect(self, wallets):
        pass
        
    @abstractmethod
    async def disconnect(self):
        pass
        
    @abstractmethod
    def set_message_callback(self, callback):
        pass
        
    @abstractmethod
    async def check_health(self):
        pass
```

### 2. Unificación de Gestión de Wallets

Combinar `wallet_tracker.py` y `wallet_manager.py` en un único módulo con responsabilidades claras:

```python
class WalletService:
    def __init__(self, db_service, config):
        self.db_service = db_service
        self.config = config
        self.wallets = {}
        self.categories = set()
        
    async def load_wallets(self):
        # Cargar desde JSON
        json_wallets = self._load_from_json()
        
        # Cargar desde BD
        db_wallets = await self.db_service.get_wallets()
        
        # Combinar wallets eliminando duplicados
        all_wallets = self._merge_wallets(json_wallets, db_wallets)
        
        return all_wallets
```

### 3. Inyección de Dependencias

Crear un sistema de inyección de dependencias para simplificar la inicialización:

```python
class ServiceContainer:
    def __init__(self, config):
        self.config = config
        self.services = {}
        
    def register(self, service_name, service_instance):
        self.services[service_name] = service_instance
        
    def get(self, service_name):
        if service_name not in self.services:
            raise KeyError(f"Servicio {service_name} no registrado")
        return self.services[service_name]
```

### 4. Patrón Repositorio para Acceso a Datos

```python
class DatabaseRepository:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        
    async def get_wallets(self):
        # Implementación
        
    async def save_transaction(self, transaction):
        # Implementación
        
    async def get_recent_transactions(self, hours=1):
        # Implementación
```

### 5. Implementar Control de Estado Centralizado

```python
class SystemState:
    def __init__(self):
        self.data_sources_health = {}
        self.component_status = {}
        self.statistics = {}
        
    def update_data_source_health(self, source_name, is_healthy, last_check=None):
        if last_check is None:
            last_check = time.time()
        self.data_sources_health[source_name] = {
            "healthy": is_healthy,
            "last_check": last_check
        }
```

### 6. Implementar Modelo de Eventos

```python
class EventBus:
    def __init__(self):
        self.subscribers = {}
        
    def subscribe(self, event_type, callback):
        if event_type not in self.subscribers:
            self.subscribers[event_type] = []
        self.subscribers[event_type].append(callback)
        
    async def publish(self, event_type, event_data):
        if event_type not in self.subscribers:
            return
            
        for callback in self.subscribers[event_type]:
            await callback(event_data)
```

## Recomendaciones Adicionales

### 1. Mejora del Sistema de Logging
- Implementar un sistema de rotación de logs
- Añadir contexto a los logs (ID de transacción, etc.)
- Centralizar la configuración de logs

### 2. Sistema de Pruebas
- Implementar pruebas unitarias para componentes críticos
- Crear mocks para APIs externas
- Implementar pruebas de integración

### 3. Mejora de Manejo de Errores
- Crear una jerarquía de excepciones personalizadas
- Mejorar la recuperación ante fallos de conexión
- Implementar circuit breaker para servicios externos

### 4. Arquitectura Escalable
- Considerar separar componentes en microservicios
- Implementar colas para procesamiento asíncrono
- Mejorar el almacenamiento de datos históricos

## Priorización de Tareas

1. **Alta prioridad**:
   - Resolver dependencias circulares
   - Unificar gestión de wallets
   - Implementar inyección de dependencias

2. **Media prioridad**:
   - Implementar modelo de eventos
   - Mejorar sistema de logging
   - Estandarizar interfaces

3. **Baja prioridad**:
   - Implementar pruebas
   - Refactorizar para arquitectura más escalable
   - Optimizar rendimiento

## Conclusión

La arquitectura actual tiene una buena base, pero puede beneficiarse significativamente de estas mejoras de refactorización. Estas cambios harán el sistema más mantenible, testeable y escalable, mientras reducen la duplicación de código y las inconsistencias en la API.

Los cambios propuestos se pueden implementar gradualmente, comenzando por la estandarización de interfaces y la unificación de la gestión de wallets, que proporcionarán las mayores ganancias inmediatas en términos de mantenibilidad y claridad del código.
