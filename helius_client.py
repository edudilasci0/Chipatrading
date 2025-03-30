# helius_client.py
import time
import requests
import logging
import asyncio
from typing import List, Dict, Any, Callable, Optional
from config import Config

logger = logging.getLogger("helius_client")

class HeliusClient:
    SOL_TOKEN = "So11111111111111111111111111111111111111112"  # Dirección del token SOL

    def __init__(self, api_key):
        self.api_key = api_key
        self.cache = {}
        self.cache_duration = int(Config.get("HELIUS_CACHE_DURATION", 300))
        # Referencia al cliente DexScreener si está disponible
        self.dexscreener_client = None
        
        # Configuración para emular WebSocket con polling
        self.polling_active = False
        self.polling_wallets = []
        self.transaction_callback = None
        self.last_tx_timestamps = {}  # Almacena los últimos timestamps de TX por wallet
        self.last_poll_time = 0
        self.polling_interval = int(Config.get("HELIUS_POLLING_INTERVAL", 15))
        self.poll_session = None
        
        # Estado de salud y métricas
        self.health_status = True
        self.error_count = 0
        self.last_success_time = time.time()
        self.api_requests_counter = 0
        self.api_error_counter = 0
        
        logger.info(f"HeliusClient inicializado (polling: {self.polling_interval}s)")
    
    def _request(self, endpoint, params, version="v1"):
        url = f"https://api.helius.xyz/{version}/{endpoint}"
        if version == "v1":
            params["apiKey"] = self.api_key
        else:  # v0
            params["api-key"] = self.api_key
        try:
            logger.debug(f"Solicitando Helius {version}: {url} con params: {params}")
            self.api_requests_counter += 1
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            self.last_success_time = time.time()
            self.error_count = 0
            self.health_status = True
            return response.json()
        except Exception as e:
            self.api_error_counter += 1
            self.error_count += 1
            if self.error_count > 3:
                self.health_status = False
            logger.error(f"Error en HeliusClient._request ({version}): {e}")
            return None
    
    async def _request_async(self, endpoint, params, version="v1"):
        """Versión asíncrona de _request"""
        url = f"https://api.helius.xyz/{version}/{endpoint}"
        if version == "v1":
            params["apiKey"] = self.api_key
        else:  # v0
            params["api-key"] = self.api_key
        
        try:
            import aiohttp
            self.api_requests_counter += 1
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.last_success_time = time.time()
                        self.error_count = 0
                        self.health_status = True
                        return data
                    else:
                        self.api_error_counter += 1
                        self.error_count += 1
                        if self.error_count > 3:
                            self.health_status = False
                        logger.error(f"Error en request_async: status {response.status}")
                        return None
        except Exception as e:
            self.api_error_counter += 1
            self.error_count += 1
            if self.error_count > 3:
                self.health_status = False
            logger.error(f"Error en HeliusClient._request_async: {e}")
            return None

    def get_token_data(self, token):
        now = time.time()
        # Si se solicita información del token SOL, se ignora
        if token == self.SOL_TOKEN:
            logger.debug("Ignorando consulta para token SOL en get_token_data")
            default_data = {
                "price": 0,
                "market_cap": 0,
                "volume": 0,
                "liquidity": 0,
                "holders": 0,
                "volume_growth": {"growth_5m": 0, "growth_1h": 0},
                "source": "helius",
                "meets_mcap_threshold": False,
                "meets_volume_threshold": False
            }
            self.cache[token] = {"data": default_data, "timestamp": now}
            return default_data

        if token in self.cache and now - self.cache[token]["timestamp"] < self.cache_duration:
            return self.cache[token]["data"]
        
        data = None
        
        # Intentar con API v1
        try:
            endpoint = f"tokens/{token}"
            data = self._request(endpoint, {}, version="v1")
            if not data:
                data = self._request(endpoint, {}, version="v0")
            if not data:
                endpoint = f"addresses/{token}/tokens"
                data = self._request(endpoint, {}, version="v0")
        except Exception as e:
            logger.warning(f"Error comunicando con Helius API: {e}")
        
        if data:
            if isinstance(data, list) and data:
                data = data[0]
            normalized_data = {
                "price": self._extract_value(data, ["price", "priceUsd"]),
                "market_cap": self._extract_value(data, ["marketCap", "market_cap"]),
                "volume": self._extract_value(data, ["volume24h", "volume", "volumeUsd"]),
                "liquidity": self._extract_value(data, ["liquidity", "totalLiquidity"]),
                "volume_growth": {
                    "growth_5m": self._normalize_percentage(self._extract_value(data, ["volumeChange5m", "volume_change_5m"])),
                    "growth_1h": self._normalize_percentage(self._extract_value(data, ["volumeChange1h", "volume_change_1h"]))
                },
                "name": self._extract_value(data, ["name", "tokenName"]),
                "symbol": self._extract_value(data, ["symbol", "tokenSymbol"]),
                "holders": self._extract_value(data, ["holders", "holderCount"]),
                "source": "helius"
            }
            
            # Validar umbrales críticos
            mcap_threshold = 100000  # $100K
            volume_threshold = 200000  # $200K
            normalized_data["meets_mcap_threshold"] = normalized_data["market_cap"] >= mcap_threshold
            normalized_data["meets_volume_threshold"] = normalized_data["volume"] >= volume_threshold
            
            self.cache[token] = {"data": normalized_data, "timestamp": now}
            return normalized_data
        
        # Intentar con DexScreener como respaldo
        if hasattr(self, 'dexscreener_client') and self.dexscreener_client:
            try:
                import asyncio
                dex_data = asyncio.run(self.dexscreener_client.fetch_token_data(token))
                if dex_data:
                    dex_data["source"] = "dexscreener"
                    mcap_threshold = 100000  # $100K
                    volume_threshold = 200000  # $200K
                    dex_data["meets_mcap_threshold"] = dex_data.get("market_cap", 0) >= mcap_threshold
                    dex_data["meets_volume_threshold"] = dex_data.get("volume", 0) >= volume_threshold
                    self.cache[token] = {"data": dex_data, "timestamp": now}
                    return dex_data
            except Exception as e:
                logger.warning(f"Error consultando DexScreener: {e}")
        
        # Si no hay datos, proporcionar datos predeterminados razonables
        default_data = {
            "price": 0.00001,
            "market_cap": 1000000,
            "volume": 10000,
            "liquidity": 5000,
            "holders": 25,
            "volume_growth": {"growth_5m": 0.1, "growth_1h": 0.05},
            "source": "default",
            "meets_mcap_threshold": False,
            "meets_volume_threshold": False
        }
        self.cache[token] = {"data": default_data, "timestamp": now}
        logger.info(f"Usando datos predeterminados para {token} - APIs fallaron")
        return default_data
    
    async def get_token_data_async(self, token):
        now = time.time()
        if token == self.SOL_TOKEN:
            logger.debug("Ignorando consulta para token SOL en get_token_data_async")
            default_data = {
                "price": 0,
                "market_cap": 0,
                "volume": 0,
                "liquidity": 0,
                "holders": 0,
                "volume_growth": {"growth_5m": 0, "growth_1h": 0},
                "source": "helius",
                "meets_mcap_threshold": False,
                "meets_volume_threshold": False
            }
            self.cache[token] = {"data": default_data, "timestamp": now}
            return default_data

        if token in self.cache and now - self.cache[token]["timestamp"] < self.cache_duration:
            return self.cache[token]["data"]
        
        data = None
        
        try:
            endpoint = f"tokens/{token}"
            data = await self._request_async(endpoint, {}, version="v1")
            if not data:
                data = await self._request_async(endpoint, {}, version="v0")
            if not data:
                endpoint = f"addresses/{token}/tokens"
                data = await self._request_async(endpoint, {}, version="v0")
        except Exception as e:
            logger.warning(f"Error comunicando con Helius API: {e}")
        
        if data:
            if isinstance(data, list) and data:
                data = data[0]
            normalized_data = {
                "price": self._extract_value(data, ["price", "priceUsd"]),
                "market_cap": self._extract_value(data, ["marketCap", "market_cap"]),
                "volume": self._extract_value(data, ["volume24h", "volume", "volumeUsd"]),
                "liquidity": self._extract_value(data, ["liquidity", "totalLiquidity"]),
                "volume_growth": {
                    "growth_5m": self._normalize_percentage(self._extract_value(data, ["volumeChange5m", "volume_change_5m"])),
                    "growth_1h": self._normalize_percentage(self._extract_value(data, ["volumeChange1h", "volume_change_1h"]))
                },
                "name": self._extract_value(data, ["name", "tokenName"]),
                "symbol": self._extract_value(data, ["symbol", "tokenSymbol"]),
                "holders": self._extract_value(data, ["holders", "holderCount"]),
                "source": "helius"
            }
            
            mcap_threshold = 100000
            volume_threshold = 200000
            normalized_data["meets_mcap_threshold"] = normalized_data["market_cap"] >= mcap_threshold
            normalized_data["meets_volume_threshold"] = normalized_data["volume"] >= volume_threshold
            
            self.cache[token] = {"data": normalized_data, "timestamp": now}
            return normalized_data
        
        if hasattr(self, 'dexscreener_client') and self.dexscreener_client:
            try:
                dex_data = await self.dexscreener_client.fetch_token_data(token)
                if dex_data:
                    dex_data["source"] = "dexscreener"
                    mcap_threshold = 100000
                    volume_threshold = 200000
                    dex_data["meets_mcap_threshold"] = dex_data.get("market_cap", 0) >= mcap_threshold
                    dex_data["meets_volume_threshold"] = dex_data.get("volume", 0) >= volume_threshold
                    self.cache[token] = {"data": dex_data, "timestamp": now}
                    return dex_data
            except Exception as e:
                logger.warning(f"Error consultando DexScreener: {e}")
        
        default_data = {
            "price": 0.00001,
            "market_cap": 1000000,
            "volume": 10000,
            "liquidity": 5000,
            "holders": 25,
            "volume_growth": {"growth_5m": 0.1, "growth_1h": 0.05},
            "source": "default",
            "meets_mcap_threshold": False,
            "meets_volume_threshold": False
        }
        self.cache[token] = {"data": default_data, "timestamp": now}
        logger.info(f"Usando datos predeterminados para {token} - APIs fallaron")
        return default_data
    
    def _extract_value(self, data, possible_keys):
        for key in possible_keys:
            if key in data:
                return data[key]
        return 0
    
    def _normalize_percentage(self, value):
        if value is None:
            return 0
        if value > 1 or value < -1:
            return value / 100
        return value
    
    async def get_token_price(self, token):
        token_data = self.get_token_data(token)
        if token_data and 'price' in token_data:
            return token_data['price']
        return 0
    
    # ----- Funciones para emular WebSocket mediante polling -----
    
    async def start_polling(self, wallets, interval=None):
        if interval is not None:
            self.polling_interval = interval
        
        if self.polling_active:
            logger.warning("Polling ya está activo")
            return
        
        self.polling_active = True
        self.polling_wallets = wallets
        self.last_poll_time = 0
        
        logger.info(f"Iniciando polling de Helius para {len(wallets)} wallets cada {self.polling_interval}s")
        
        try:
            import aiohttp
            self.poll_session = aiohttp.ClientSession()
        except ImportError:
            logger.error("Error: aiohttp no está instalado, necesario para polling")
            self.polling_active = False
            return
        
        while self.polling_active:
            try:
                await self._poll_transactions()
                await asyncio.sleep(self.polling_interval)
            except asyncio.CancelledError:
                logger.info("Polling de Helius cancelado")
                break
            except Exception as e:
                logger.error(f"Error en polling de Helius: {e}")
                await asyncio.sleep(self.polling_interval * 2)
        
        if self.poll_session and not self.poll_session.closed:
            await self.poll_session.close()
            self.poll_session = None
        
        self.polling_active = False
        logger.info("Polling de Helius detenido")
    
    def stop_polling(self):
        self.polling_active = False
        logger.info("Solicitada detención de polling de Helius")
    
    async def _poll_transactions(self):
        now = time.time()
        if now - self.last_poll_time < self.polling_interval / 2:
            return
        self.last_poll_time = now
        
        if not self.transaction_callback:
            logger.warning("No hay callback configurado para procesar transacciones")
            return
        
        if not self.polling_wallets:
            return
        
        chunk_size = 20
        for i in range(0, len(self.polling_wallets), chunk_size):
            chunk = self.polling_wallets[i:i+chunk_size]
            tasks = [asyncio.create_task(self._get_wallet_transactions(wallet)) for wallet in chunk]
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for wallet, result in zip(chunk, results):
                    if isinstance(result, Exception):
                        logger.warning(f"Error obteniendo transacciones para {wallet}: {result}")
                    elif result:
                        for tx in result:
                            if self._is_new_transaction(wallet, tx):
                                await self._process_transaction(wallet, tx)
            except Exception as e:
                logger.error(f"Error procesando chunk de wallets: {e}")
            if i + chunk_size < len(self.polling_wallets):
                await asyncio.sleep(1)
    
    async def _get_wallet_transactions(self, wallet, limit=10):
        try:
            url = f"https://api.helius.xyz/v0/addresses/{wallet}/transactions"
            params = {
                "api-key": self.api_key,
                "limit": limit
            }
            if self.poll_session and not self.poll_session.closed:
                session = self.poll_session
            else:
                import aiohttp
                session = aiohttp.ClientSession()
                self.poll_session = session
            self.api_requests_counter += 1
            async with session.get(url, params=params, timeout=5) as response:
                if response.status == 200:
                    transactions = await response.json()
                    self.last_success_time = time.time()
                    self.error_count = 0
                    self.health_status = True
                    return transactions
                else:
                    self.api_error_counter += 1
                    self.error_count += 1
                    if self.error_count > 3:
                        self.health_status = False
                    logger.warning(f"Error obteniendo transacciones: {response.status}")
                    return None
        except Exception as e:
            self.api_error_counter += 1
            self.error_count += 1
            if self.error_count > 3:
                self.health_status = False
            logger.error(f"Error en _get_wallet_transactions: {e}")
            return None
    
    def _is_new_transaction(self, wallet, tx):
        if not tx or "timestamp" not in tx:
            return False
        tx_time = tx["timestamp"]
        if wallet not in self.last_tx_timestamps:
            self.last_tx_timestamps[wallet] = tx_time
            return True
        if tx_time > self.last_tx_timestamps[wallet]:
            self.last_tx_timestamps[wallet] = tx_time
            return True
        return False
    
    async def _process_transaction(self, wallet, tx_data):
        try:
            tx_type = self._determine_tx_type(tx_data)
            if not tx_type:
                return
            token = self._extract_token(tx_data)
            if not token:
                return
            amount_usd = self._extract_amount_usd(tx_data)
            if amount_usd <= 0:
                return
            normalized_tx = {
                "wallet": wallet,
                "token": token,
                "type": tx_type,
                "amount_usd": amount_usd,
                "timestamp": time.time(),
                "source": "helius_polling"
            }
            if self.transaction_callback:
                await self.transaction_callback(normalized_tx)
        except Exception as e:
            logger.error(f"Error procesando transacción: {e}")
    
    def _determine_tx_type(self, tx_data):
        if "instructions" not in tx_data:
            return None
        for instruction in tx_data["instructions"]:
            if "programId" in instruction:
                program_id = instruction["programId"]
                if program_id in ["9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin", 
                                  "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB"]:
                    if "buy" in str(instruction).lower():
                        return "BUY"
                    elif "sell" in str(instruction).lower():
                        return "SELL"
                    else:
                        return "SWAP"
        return None
    
    def _extract_token(self, tx_data):
        if "accounts" in tx_data:
            for account in tx_data["accounts"]:
                if account and account.startswith("So") and account != self.SOL_TOKEN:
                    return account
        if "tokenTransfers" in tx_data:
            for transfer in tx_data["tokenTransfers"]:
                if "mint" in transfer:
                    return transfer["mint"]
        return None
    
    def _extract_amount_usd(self, tx_data):
        if "meta" in tx_data and "fee" in tx_data["meta"]:
            fee = float(tx_data["meta"]["fee"]) / 1000000000
            return fee * 200
        return 1000
    
    def set_transaction_callback(self, callback):
        self.transaction_callback = callback
        logger.info("Callback de transacciones configurado")
    
    async def check_availability(self):
        try:
            data = await self._request_async("stats", {}, version="v1")
            if data:
                return True
        except Exception as e:
            logger.warning(f"API de Helius no disponible: {e}")
        return False
    
    def get_health_metrics(self):
        return {
            "health_status": self.health_status,
            "error_count": self.error_count,
            "last_success": self.last_success_time,
            "time_since_success": time.time() - self.last_success_time,
            "api_requests": self.api_requests_counter,
            "api_errors": self.api_error_counter,
            "error_rate": self.api_error_counter / max(1, self.api_requests_counter),
            "polling_active": self.polling_active,
            "polling_wallets_count": len(self.polling_wallets) if self.polling_wallets else 0
        }
