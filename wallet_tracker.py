# wallet_tracker.py

import json

class WalletTracker:
    def __init__(self, json_path='traders_data.json'):
        self.json_path = json_path
        self.traders = self.load_traders()

    def load_traders(self):
        try:
            with open(self.json_path, 'r') as f:
                data = json.load(f)
            return data
        except FileNotFoundError:
            return []

    def save_traders(self):
        with open(self.json_path, 'w') as f:
            json.dump(self.traders, f, indent=4)

    def update_trader(self, trader_data):
        updated = False
        for idx, trader in enumerate(self.traders):
            if trader.get("Trader") == trader_data.get("Trader"):
                self.traders[idx] = trader_data
                updated = True
                break
        if not updated:
            self.traders.append(trader_data)
        self.save_traders()

    def get_trader(self, wallet_address):
        for trader in self.traders:
            if trader.get("Wallet") == wallet_address:
                return trader
        return None
