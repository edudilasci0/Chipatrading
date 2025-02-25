# rugcheck.py

import time
import json
import requests
from solana.keypair import Keypair

def login_rugcheck_solana(private_key: bytes, wallet_public_key: str) -> str:
    """
    Autentica en RugCheck firmando un mensaje con la clave privada de Solana.
    Retorna un JWT token en caso de éxito.
    """
    message_text = "Sign-in to Rugcheck.xyz"
    timestamp = int(time.time())
    message_data = {
        "message": message_text,
        "publicKey": wallet_public_key,
        "timestamp": timestamp
    }
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
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        data = response.json()
        jwt_token = data.get("token")
        print("JWT token obtenido:", jwt_token)
        return jwt_token
    else:
        print("Error en login:", response.status_code, response.text)
        return None

def get_token_report_summary(jwt_token: str, token_mint: str):
    """
    Consulta el reporte del token usando el endpoint /tokens/{mint}/report/summary.
    """
    url = f"https://api.rugcheck.xyz/v1/tokens/{token_mint}/report/summary"
    headers = {"Authorization": f"Bearer {jwt_token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        report = response.json()
        print("Reporte del token:")
        print(json.dumps(report, indent=2))
        return report
    else:
        print("Error al obtener reporte del token:", response.status_code, response.text)
        return None

def validar_seguridad_contrato(jwt_token: str, token_mint: str) -> bool:
    """
    Valida la seguridad del contrato de un token.
    Se considera inseguro si el campo 'rugged' es True o el 'score' es inferior a un umbral.
    """
    report = get_token_report_summary(jwt_token, token_mint)
    if not report:
        return False
    rugged = report.get("rugged", False)
    score = report.get("score", 0)
    if rugged:
        print("El token ha sido marcado como 'rugged' (riesgoso).")
        return False
    if score < 50:  # Umbral de ejemplo
        print(f"El token tiene un score bajo ({score}).")
        return False
    print("El token ha pasado la validación de seguridad.")
    return True
