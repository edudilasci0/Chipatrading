# signal_logic.py

import time
from config import Config

class SignalLogic:
    def __init__(self, rugcheck_api=None):
        """
        Inicializa la lógica de señales.
        
        Args:
            rugcheck_api: Instancia de la API para validación de tokens con RugCheck (opcional).
        """
        self.rugcheck_api = rugcheck_api

    def add_transaction(self, token, transaction_data):
        """
        Procesa una nueva transacción para un token.
        
        Args:
            token (str): Dirección o identificador del token.
            transaction_data (dict): Datos de la transacción.
        """
        now = int(time.time())
        
        # NUEVO: Ignorar tokens especiales o conocidos
        if token in Config.IGNORE_TOKENS:
            print(f"⚠️ Token {token} en lista de ignorados, no se procesará")
            return
        
        # Resto del procesamiento de la transacción...
        print(f"Procesando transacción para token {token} a las {now}")
        # Aquí iría la lógica para almacenar la transacción, actualizar registros, etc.

    def _validate_token_safety(self, token):
        """
        Valida la seguridad de un token usando RugCheck.
        Versión modificada para que no filtre tokens.
        
        Args:
            token (str): Dirección del token.
            
        Returns:
            bool: Siempre retorna True para permitir que todos los tokens pasen.
        """
        # MODIFICADO: Verificar si está habilitado el filtrado de RugCheck
        if not Config.get("ENABLE_RUGCHECK_FILTERING", False):
            return True
        
        # Si no hay API de RugCheck, retornar True
        if not self.rugcheck_api:
            return True
        
        try:
            # Obtener umbral mínimo de score
            min_score = int(Config.get("rugcheck_min_score", 50))
            
            # Validar con RugCheck pero solo para logging, no para filtrar
            is_safe = self.rugcheck_api.validate_token_safety(token, min_score)
            
            # Registrar el resultado pero no filtrar
            if not is_safe:
                print(f"⚠️ Token {token} no pasó validación de seguridad, pero aún se procesará")
            
            # Siempre retornar True para permitir todos los tokens
            return True
        except Exception as e:
            print(f"⚠️ Error validando seguridad de {token}: {e}")
            # No filtrar por errores
            return True

    def check_signals(self, candidates):
        """
        Evalúa los candidatos para generar señales de trading.
        
        Args:
            candidates (list): Lista de diccionarios con información de cada token candidato.
                               Cada diccionario debería incluir, al menos, las claves:
                               "token", "metric", "recent_transactions", "confidence",
                               "large_tx_count" y "elite_traders".
        """
        # Ordenar candidatos según algún criterio (por ejemplo, 'metric')
        sorted_candidates = sorted(candidates, key=lambda x: x.get('metric', 0), reverse=True)
        
        # Definición de umbrales por defecto
        min_traders = 3
        min_volume = 5000
        min_confidence = 0.5
        min_growth = 0.03  # MEJORADO: 3% crecimiento en 5min
        
        # MEJORADO: Reducir umbrales si hay pocos candidatos
        if len(sorted_candidates) < 5:
            print(f"Pocos candidatos ({len(sorted_candidates)}), reduciendo umbrales")
            min_traders = max(1, min_traders - 1)
            min_volume = max(1000, int(min_volume * 0.8))
            min_confidence = max(0.2, min_confidence * 0.9)
        
        tokens_filtered_confidence = 0
        
        # Procesar cada candidato
        for candidate in sorted_candidates:
            token = candidate.get("token")
            recent_transactions = candidate.get("recent_transactions", [])
            confidence = candidate.get("confidence", 0)
            large_tx_count = candidate.get("large_tx_count", 0)
            elite_traders = candidate.get("elite_traders", 0)
            
            # Verificar actividad reciente: se requiere al menos 1 transacción
            if len(recent_transactions) < 1:
                print(f"Token {token} sin actividad reciente suficiente.")
                continue
            
            # Aquí se pueden agregar más verificaciones según traders, volumen, crecimiento, etc.
            # ...
            
            # MEJORADO: Filtrar señales con baja confianza de forma más flexible
            if confidence < min_confidence:
                # Boost por transacciones grandes o traders de élite
                if large_tx_count > 1 or elite_traders > 0:
                    print(f"Token {token} con confianza baja ({confidence:.2f}) pero con factores positivos, continuando")
                else:
                    print(f"⚠️ Token {token} filtrado por baja confianza: {confidence:.2f}")
                    tokens_filtered_confidence += 1
                    continue
            
            # Si pasa todas las verificaciones, se genera la señal de trading
            print(f"Generando señal para token {token}")
            # Aquí implementar la lógica para generar y emitir la señal
        
        print(f"Total tokens filtrados por confianza: {tokens_filtered_confidence}")

# Ejemplo de uso:
if __name__ == "__main__":
    # Instanciar la clase SignalLogic (puedes pasar la API de RugCheck si la tienes)
    signal_logic = SignalLogic(rugcheck_api=None)
    
    # Ejemplo de lista de candidatos
    candidates = [
        {
            "token": "TOKEN1",
            "metric": 10,
            "recent_transactions": [1, 2],
            "confidence": 0.6,
            "large_tx_count": 0,
            "elite_traders": 0
        },
        {
            "token": "TOKEN2",
            "metric": 8,
            "recent_transactions": [],
            "confidence": 0.4,
            "large_tx_count": 2,
            "elite_traders": 1
        },
        {
            "token": "TOKEN3",
            "metric": 7,
            "recent_transactions": [1],
            "confidence": 0.3,
            "large_tx_count": 0,
            "elite_traders": 0
        }
    ]
    
    # Llamada al método check_signals con los candidatos de ejemplo
    signal_logic.check_signals(candidates)
