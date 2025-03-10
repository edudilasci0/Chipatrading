import time
import requests
from binascii import unhexlify
from config import Config
from solana.keypair import Keypair  # Asumiendo uso del paquete solana

def login_rugcheck_solana(private_key=None, wallet_public_key=None):
    if private_key is None and Config.RUGCHECK_PRIVATE_KEY:
        try:
            private_key = unhexlify(Config.RUGCHECK_PRIVATE_KEY)
            print("✅ Clave privada decodificada correctamente para RugCheck")
        except Exception as e:
            print(f"⚠️ Error al decodificar RUGCHECK_PRIVATE_KEY: {e}")
            print("Debe ser una cadena hexadecimal (ejemplo: '4a2c3d...')")
            return None
    if wallet_public_key is None:
        wallet_public_key = Config.RUGCHECK_WALLET_PUBKEY
    if not private_key or not wallet_public_key:
        print("⚠️ Credenciales de RugCheck no configuradas")
        return None
    message_text = "Sign-in to Rugcheck.xyz"
    timestamp = int(time.time())
    message_data = {
        "message": message_text,
        "publicKey": wallet_public_key,
        "timestamp": timestamp
    }
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            keypair = Keypair.from_secret_key(private_key)
            signature = keypair.sign(message_text.encode("utf-8")).signature
            signature_list = list(signature)
            payload = {
                "message": message_data,
                "signature": {
                    "data": signature_list,
                    "type": "ed25519"
                },
                "wallet": wallet_public_key
            }
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
                    wait_time = 2 * (attempt + 1)
                    print(f"Reintentando ({attempt+1}/{max_attempts}) en {wait_time}s...")
                    time.sleep(wait_time)
        except Exception as e:
            print(f"🚨 Excepción en login de RugCheck: {e}")
            if attempt < max_attempts - 1:
                wait_time = 2 * (attempt + 1)
                print(f"Reintentando ({attempt+1}/{max_attempts}) en {wait_time}s...")
                time.sleep(wait_time)
    return None
