from datetime import datetime
import json

class WalletTracker:
    """
    Administra la lista de wallets definida en traders_data.json
    """

    def __init__(self, json_path='traders_data.json'):
        self.json_path = json_path
        self.wallets = []
        self.load_wallets()

    def load_wallets(self):
        """
        Carga la lista de wallets desde el JSON, esperando
        que cada entrada tenga 'Wallet' como clave.
        """
        try:
            with open(self.json_path, 'r') as f:
                data = json.load(f)
            self.wallets = [entry["Wallet"] for entry in data if "Wallet" in entry]
            print(f"📝 Cargadas {len(self.wallets)} wallets desde {self.json_path}")
        except FileNotFoundError:
            print(f"⚠️ {self.json_path} no existe. Se inicia sin wallets.")
            self.wallets = []

    def get_wallets(self):
        return self.wallets

    def add_wallet(self, wallet_dict):
        """
        Agrega una nueva wallet al listado en memoria.
        Si quieres persistir, reescribe el JSON.
        """
        if "Wallet" in wallet_dict:
            self.wallets.append(wallet_dict["Wallet"])
            # Podrías reescribir traders_data.json
            # ...
            print(f"➕ Wallet agregada: {wallet_dict['Wallet']}")
