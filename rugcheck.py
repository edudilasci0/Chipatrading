import time
import json
import requests
from binascii import unhexlify
from config import Config

# Intenta diferentes importaciones según la versión de Solana instalada
try:
    # Intenta la importación de versiones más recientes (solders)
    from solders.keypair import Keypair
except ImportError:
    try:
        # Intenta la importación original
        from solana.keypair import Keypair
    except ImportError:
        try:
            # Última opción con solana.key
            from solana.key import Keypair
        except ImportError:
            # Si todas fallan, crea una implementación básica para evitar que el programa falle
            print("⚠️ No se pudo importar Keypair de solana o solders.")
            print("⚠️ La funcionalidad de RugCheck estará desactivada.")
            
            # Clase mock para permitir que el programa continúe sin la funcionalidad completa
            class Keypair:
                @staticmethod
                def from_secret_key(secret_key):
                    class MockKeypair:
                        def sign(self, message):
                            class MockSignature:
                                signature = b'0' * 64  # Firma ficticia
                            return MockSignature()
                    return MockKeypair()

class RugCheckAPI:
    """
    Clase para interactuar con la API de RugCheck.
    """
    
    def __init__(self):
        """
        Inicializa la API de RugCheck.
        """
        self.base_url = "https://api.rugcheck.xyz/v1"
        self.jwt_token = None
        self.token_expiry = 0
        self.token_cache = {}  # {token: {'report': report, 'timestamp': ts}}
        self.cache_expiry = 3600  # 1 hora de caché para reportes
    
    def authenticate(self):
        """
        Autentica en RugCheck y obtiene un JWT token.
        
        Returns:
            str: JWT token o None si falla
        """
        # Si tenemos un token válido, usarlo
        if self.jwt_token and time.time() < self.token_expiry:
            return self.jwt_token
            
        # Intentar obtener nuevo token
        self.jwt_token = login_rugcheck_solana()
        if self.jwt_token:
            self.token_expiry = time.time() + 3600  # Expira en 1 hora
            
        return self.jwt_token
    
    def get_token_report(self, token_mint):
        """
        Obtiene el reporte completo de un token.
        
        Args:
            token_mint: Dirección del token
            
        Returns:
            dict: Reporte completo o None si hay error
        """
        # Verificar caché
        if token_mint in self.token_cache:
            cache_time = self.token_cache[token_mint]['timestamp']
            if time.time() - cache_time < self.cache_expiry:
                return self.token_cache[token_mint]['report']
        
        # Autenticar si es necesario
        if not self.jwt_token:
            self.jwt_token = self.authenticate()
            if not self.jwt_token:
                return None
        
        # Obtener reporte
        report = get_token_report_summary(self.jwt_token, token_mint)
        
        # Guardar en caché
        if report:
            self.token_cache[token_mint] = {
                'report': report,
                'timestamp': time.time()
            }
        
        return report
    
    def validate_token_safety(self, token_mint, min_score=50):
        """
        Valida la seguridad de un token.
        
        Args:
            token_mint: Dirección del token
            min_score: Score mínimo aceptable
            
        Returns:
            bool: True si es seguro, False si no
        """
        # Autenticar si es necesario
        if not self.jwt_token:
            self.jwt_token = self.authenticate()
            if not self.jwt_token:
                print(f"⚠️ No se pudo autenticar con RugCheck para validar {token_mint}")
                return False
                
        return validar_seguridad_contrato(self.jwt_token, token_mint, min_score)

# Implementación de funciones para mantener compatibilidad
def login_rugcheck_solana(private_key=None, wallet_public_key=None):
    """
    Autentica en RugCheck firmando un mensaje con la clave privada de Solana.
    Retorna un JWT token en caso de éxito.
    
    Si no se proporcionan los parámetros, usa los valores de Config
    """
    # Usar valores de configuración si no se proporcionan
    if private_key is None and Config.RUGCHECK_PRIVATE_KEY:
        try:
            # Convertir string hexadecimal a bytes
            private_key = unhexlify(Config.RUGCHECK_PRIVATE_KEY)
            print(f"✅ Clave privada decodificada correctamente para RugCheck")
        except Exception as e:
            print(f"⚠️ Error al decodificar RUGCHECK_PRIVATE_KEY: {e}")
            print("Debe ser una cadena hexadecimal (ejemplo: '4a2c3d...')")
            return None
            
    if wallet_public_key is None:
        wallet_public_key = Config.RUGCHECK_WALLET_PUBKEY
        
    if not private_key or not wallet_public_key:
        print("⚠️ Credenciales de RugCheck no configuradas")
        return None
    
    # Preparar mensaje para firmar
    message_text = "Sign-in to Rugcheck.xyz"
    timestamp = int(time.time())
    message_data = {
        "message": message_text,
        "publicKey": wallet_public_key,
        "timestamp": timestamp
    }
    
    # Implementar reintentos
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            # Crear keypair y firmar mensaje
            keypair = Keypair.from_secret_key(private_key)
            signature = keypair.sign(message_text.encode("utf-8")).signature
            signature_list = list(signature)
            
            # Preparar payload para login
            payload = {
                "message": message_data,
                "signature": {
                    "data": signature_list,
                    "type": "ed25519"
                },
                "wallet": wallet_public_key
            }
            
            # Llamar a API de login
            url = "https://api.rugcheck.xyz/v1/auth/login/solana"
            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                jwt_token = data.get("token")
                print("✅ Autenticación con RugCheck exitosa")
                return jwt_token
            else:
                print(f"❌ Error en login de RugCheck: {response.status_code} - {response.text}")
                if attempt < max_attempts - 1:
                    print(f"Reintentando ({attempt+1}/{max_attempts})...")
                    time.sleep(2)
                else:
                    return None
                
        except Exception as e:
            print(f"🚨 Excepción en login de RugCheck: {e}")
            if attempt < max_attempts - 1:
                print(f"Reintentando ({attempt+1}/{max_attempts})...")
                time.sleep(2)
            else:
                return None
    
    return None

def get_token_report_summary(jwt_token, token_mint):
    """
    Consulta el reporte del token usando el endpoint /tokens/{mint}/report/summary.
    Retorna los datos del reporte o None si hay error.
    """
    if not jwt_token:
        print("⚠️ No hay JWT token para consultar RugCheck")
        return None
        
    # Implementar reintentos
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            url = f"https://api.rugcheck.xyz/v1/tokens/{token_mint}/report/summary"
            headers = {"Authorization": f"Bearer {jwt_token}"}
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                report = response.json()
                return report
            elif response.status_code == 404:
                print(f"ℹ️ Token {token_mint} no encontrado en RugCheck")
                return None
            else:
                print(f"⚠️ Error al obtener reporte de RugCheck: {response.status_code} - {response.text}")
                if attempt < max_attempts - 1:
                    print(f"Reintentando ({attempt+1}/{max_attempts})...")
                    time.sleep(2)
                else:
                    return None
                
        except Exception as e:
            print(f"🚨 Excepción al consultar RugCheck: {e}")
            if attempt < max_attempts - 1:
                print(f"Reintentando ({attempt+1}/{max_attempts})...")
                time.sleep(2)
            else:
                return None
    
    return None

def validar_seguridad_contrato(jwt_token, token_mint, min_score=50):
    """
    Valida la seguridad del contrato de un token.
    Se considera inseguro si el campo 'rugged' es True o el 'score' es inferior al umbral.
    
    Retorna True si el token es seguro, False si es riesgoso o hay error.
    """
    # Intentar obtener reporte
    report = get_token_report_summary(jwt_token, token_mint)
    
    # Si no hay reporte, considerar que no podemos validar (mejor prevenir)
    if not report:
        print(f"⚠️ No se pudo obtener reporte de RugCheck para {token_mint}")
        return False
        
    # Verificar si está marcado como rugged
    rugged = report.get("rugged", False)
    if rugged:
        print(f"🚫 Token {token_mint} está marcado como 'rugged' en RugCheck")
        return False
        
    # Verificar score mínimo
    score = report.get("score", 0)
    if score < min_score:
        print(f"⚠️ Token {token_mint} tiene score bajo en RugCheck: {score}")
        return False
        
    # Si pasó todas las validaciones, es seguro
    print(f"✅ Token {token_mint} pasó la validación de RugCheck con score {score}")
    return True
