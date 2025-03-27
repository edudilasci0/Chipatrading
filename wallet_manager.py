# wallet_manager.py
import json
import os
import logging
import time
from pathlib import Path
import threading
from typing import Dict, List, Optional, Set, Union, Any
from config import Config
import db

logger = logging.getLogger("wallet_manager")

class WalletManager:
    """
    Clase responsable de la gestión CRUD para wallets y sus categorías.
    Maneja la carga desde JSON/BD y proporciona métodos para añadir, actualizar
    y eliminar wallets y categorías.
    """
    
    def __init__(self, json_path='traders_data.json', auto_save=True):
        """
        Inicializa el gestor de wallets.
        
        Args:
            json_path: Ruta al archivo JSON de wallets
            auto_save: Si es True, guarda automáticamente los cambios en el archivo JSON
        """
        self.json_path = json_path
        self.auto_save = auto_save
        self.wallets = {}  # {address: {name, category, score, last_updated}}
        self.categories = set()  # Conjunto de categorías únicas
        self.wallets_by_category = {}  # {category: [addresses]}
        self.last_save_time = 0
        self.save_lock = threading.Lock()
        self.load_wallets()
        
        # Cache para optimizar consultas frecuentes
        self._address_list_cache = None
        self._address_list_cache_timestamp = 0
        self._cache_ttl = 60  # Cache válido por 60 segundos
        
        logger.info(f"WalletManager inicializado con {len(self.wallets)} wallets en {len(self.categories)} categorías")
    
    def load_wallets(self) -> bool:
        """
        Carga las wallets desde el archivo JSON.
        
        Returns:
            bool: True si la carga fue exitosa, False en caso contrario
        """
        try:
            if not os.path.exists(self.json_path):
                logger.warning(f"Archivo {self.json_path} no encontrado. Iniciando con lista vacía.")
                return False
            
            with open(self.json_path, 'r') as f:
                data = json.load(f)
            
            # Limpiar datos existentes
            self.wallets.clear()
            self.categories.clear()
            self.wallets_by_category.clear()
            self._address_list_cache = None
            
            # Procesar cada entrada
            for entry in data:
                if "Wallet" not in entry:
                    continue
                
                address = entry["Wallet"]
                category = entry.get("Categoria", "Default")
                trader_name = entry.get("Trader", "")
                score = float(entry.get("Puntaje", Config.DEFAULT_SCORE))
                
                # Agregar a la estructura principal
                self.wallets[address] = {
                    "name": trader_name,
                    "category": category,
                    "score": score,
                    "last_updated": time.time()
                }
                
                # Actualizar categorías
                self.categories.add(category)
                if category not in self.wallets_by_category:
                    self.wallets_by_category[category] = set()
                self.wallets_by_category[category].add(address)
            
            # Sincronizar con la base de datos si está disponible
            self._sync_with_database()
            
            logger.info(f"✅ Cargadas {len(self.wallets)} wallets desde {self.json_path}")
            return True
        except Exception as e:
            logger.error(f"Error cargando wallets desde {self.json_path}: {e}")
            return False
    
    def _sync_with_database(self) -> None:
        """
        Sincroniza datos entre el archivo JSON y la base de datos.
        Actualiza scores de la base de datos y los combina con los del JSON.
        """
        try:
            # Obtener scores de la base de datos
            db_scores = db.execute_cached_query("SELECT wallet, score FROM wallet_scores")
            if not db_scores:
                return
            
            # Actualizar scores con la información de la base de datos
            updated_count = 0
            for row in db_scores:
                wallet = row["wallet"]
                score = float(row["score"])
                
                if wallet in self.wallets:
                    # Prevalece el score más reciente de la BD
                    if self.wallets[wallet]["score"] != score:
                        self.wallets[wallet]["score"] = score
                        updated_count += 1
                else:
                    # Wallet en BD pero no en JSON, lo agregamos
                    category = "Default"
                    self.wallets[wallet] = {
                        "name": wallet[:8],  # Nombre truncado por defecto
                        "category": category,
                        "score": score,
                        "last_updated": time.time()
                    }
                    
                    # Actualizar categorías
                    self.categories.add(category)
                    if category not in self.wallets_by_category:
                        self.wallets_by_category[category] = set()
                    self.wallets_by_category[category].add(wallet)
                    updated_count += 1
            
            if updated_count > 0:
                logger.info(f"Sincronizados {updated_count} wallets con la base de datos")
                # Guardar cambios si hay actualizaciones y auto_save está activado
                if self.auto_save:
                    self.save_wallets()
        except Exception as e:
            logger.error(f"Error sincronizando con base de datos: {e}")
    
    def save_wallets(self) -> bool:
        """
        Guarda las wallets en el archivo JSON.
        
        Returns:
            bool: True si el guardado fue exitoso, False en caso contrario
        """
        # Evitar guardados frecuentes (máximo uno cada 5 segundos)
        now = time.time()
        if now - self.last_save_time < 5:
            return True
        
        with self.save_lock:
            try:
                # Crear directorio si no existe
                os.makedirs(os.path.dirname(os.path.abspath(self.json_path)), exist_ok=True)
                
                # Convertir a formato compatible con traders_data.json
                data = []
                for address, info in self.wallets.items():
                    entry = {
                        "Wallet": address,
                        "Trader": info.get("name", ""),
                        "Categoria": info.get("category", "Default"),
                        "Puntaje": info.get("score", Config.DEFAULT_SCORE)
                    }
                    data.append(entry)
                
                # Guardar con formato legible
                with open(self.json_path, 'w') as f:
                    json.dump(data, f, indent=4)
                
                self.last_save_time = now
                logger.info(f"✅ Guardadas {len(data)} wallets en {self.json_path}")
                return True
            except Exception as e:
                logger.error(f"Error guardando wallets en {self.json_path}: {e}")
                return False
    
    def add_wallet(self, address: str, name: str = "", category: str = "Default", 
                   score: float = None) -> bool:
        """
        Añade una nueva wallet o actualiza una existente.
        
        Args:
            address: Dirección de la wallet
            name: Nombre del trader
            category: Categoría de la wallet
            score: Puntuación (0-10)
            
        Returns:
            bool: True si se añadió/actualizó correctamente
        """
        if not address:
            logger.error("No se puede añadir wallet sin dirección")
            return False
        
        if score is None:
            score = float(Config.DEFAULT_SCORE)
        
        is_update = address in self.wallets
        action = "actualizada" if is_update else "añadida"
        
        # Si es una actualización, guardar la categoría anterior para actualizar índices
        old_category = None
        if is_update:
            old_category = self.wallets[address].get("category", "Default")
        
        # Actualizar datos principales
        self.wallets[address] = {
            "name": name,
            "category": category,
            "score": score,
            "last_updated": time.time()
        }
        
        # Actualizar categorías
        self.categories.add(category)
        if category not in self.wallets_by_category:
            self.wallets_by_category[category] = set()
        self.wallets_by_category[category].add(address)
        
        # Si es una actualización y cambió la categoría, actualizar índices
        if is_update and old_category != category and old_category in self.wallets_by_category:
            self.wallets_by_category[old_category].discard(address)
            # Limpiar categorías vacías
            if not self.wallets_by_category[old_category]:
                del self.wallets_by_category[old_category]
                self.categories.discard(old_category)
        
        # Actualizar en la base de datos si es necesario
        try:
            db.update_wallet_score(address, score)
        except Exception as e:
            logger.warning(f"Error actualizando score en BD: {e}")
        
        # Invalidar caché
        self._address_list_cache = None
        
        # Guardar cambios si auto_save está activado
        if self.auto_save:
            self.save_wallets()
        
        logger.info(f"Wallet {action}: {address} ({name}) en categoría '{category}' con score {score}")
        return True
    
    def remove_wallet(self, address: str) -> bool:
        """
        Elimina una wallet.
        
        Args:
            address: Dirección de la wallet a eliminar
            
        Returns:
            bool: True si se eliminó correctamente
        """
        if address not in self.wallets:
            logger.warning(f"Wallet no encontrada: {address}")
            return False
        
        # Obtener categoría para actualizar índices
        category = self.wallets[address].get("category", "Default")
        
        # Eliminar de la estructura principal
        del self.wallets[address]
        
        # Actualizar índices de categoría
        if category in self.wallets_by_category:
            self.wallets_by_category[category].discard(address)
            # Limpiar categorías vacías
            if not self.wallets_by_category[category]:
                del self.wallets_by_category[category]
                self.categories.discard(category)
        
        # Invalidar caché
        self._address_list_cache = None
        
        # Guardar cambios si auto_save está activado
        if self.auto_save:
            self.save_wallets()
        
        logger.info(f"Wallet eliminada: {address}")
        return True
    
    def update_wallet(self, address: str, name: str = None, category: str = None, 
                      score: float = None) -> bool:
        """
        Actualiza los datos de una wallet existente.
        
        Args:
            address: Dirección de la wallet
            name: Nuevo nombre (opcional)
            category: Nueva categoría (opcional)
            score: Nueva puntuación (opcional)
            
        Returns:
            bool: True si se actualizó correctamente
        """
        if address not in self.wallets:
            logger.warning(f"Wallet no encontrada para actualizar: {address}")
            return False
        
        # Obtener datos actuales
        current = self.wallets[address]
        
        # Actualizar solo los campos proporcionados
        update_data = {}
        if name is not None:
            update_data["name"] = name
        if category is not None:
            update_data["category"] = category
        if score is not None:
            update_data["score"] = score
        
        # Si no hay cambios, salir
        if not update_data:
            return True
        
        # Actualizar timestamp
        update_data["last_updated"] = time.time()
        
        # Si cambia la categoría, actualizar índices
        if "category" in update_data and update_data["category"] != current.get("category"):
            old_category = current.get("category", "Default")
            new_category = update_data["category"]
            
            # Actualizar categorías
            self.categories.add(new_category)
            if new_category not in self.wallets_by_category:
                self.wallets_by_category[new_category] = set()
            self.wallets_by_category[new_category].add(address)
            
            # Actualizar categoría anterior
            if old_category in self.wallets_by_category:
                self.wallets_by_category[old_category].discard(address)
                # Limpiar categorías vacías
                if not self.wallets_by_category[old_category]:
                    del self.wallets_by_category[old_category]
                    self.categories.discard(old_category)
        
        # Actualizar la estructura principal
        self.wallets[address].update(update_data)
        
        # Actualizar en la base de datos si cambia el score
        if "score" in update_data:
            try:
                db.update_wallet_score(address, update_data["score"])
            except Exception as e:
                logger.warning(f"Error actualizando score en BD: {e}")
        
        # Guardar cambios si auto_save está activado
        if self.auto_save:
            self.save_wallets()
        
        logger.info(f"Wallet actualizada: {address} con {update_data}")
        return True
    
    def get_wallet_info(self, address: str) -> Optional[Dict]:
        """
        Obtiene la información de una wallet.
        
        Args:
            address: Dirección de la wallet
            
        Returns:
            Dict: Información de la wallet o None si no existe
        """
        if address not in self.wallets:
            return None
        
        # Devolver copia para evitar modificaciones accidentales
        return dict(self.wallets[address])
    
    def get_wallets_by_category(self, category: str = None) -> List[str]:
        """
        Obtiene las wallets de una categoría o todas si no se especifica.
        
        Args:
            category: Categoría a filtrar (opcional)
            
        Returns:
            List[str]: Lista de direcciones de wallet
        """
        if category is None:
            return list(self.wallets.keys())
        
        if category not in self.wallets_by_category:
            return []
        
        return list(self.wallets_by_category[category])
    
    def get_categories(self) -> List[str]:
        """
        Obtiene todas las categorías disponibles.
        
        Returns:
            List[str]: Lista de categorías
        """
        return list(self.categories)
    
    def add_category(self, category: str) -> bool:
        """
        Añade una nueva categoría.
        
        Args:
            category: Nombre de la categoría
            
        Returns:
            bool: True si se añadió correctamente
        """
        if not category:
            logger.error("No se puede añadir categoría vacía")
            return False
        
        # Si ya existe, no hacer nada
        if category in self.categories:
            return True
        
        self.categories.add(category)
        self.wallets_by_category[category] = set()
        
        logger.info(f"Categoría añadida: {category}")
        return True
    
    def remove_category(self, category: str, move_to: str = "Default") -> bool:
        """
        Elimina una categoría y mueve sus wallets a otra categoría.
        
        Args:
            category: Categoría a eliminar
            move_to: Categoría a la que mover las wallets
            
        Returns:
            bool: True si se eliminó correctamente
        """
        if category not in self.categories:
            logger.warning(f"Categoría no encontrada: {category}")
            return False
        
        if category == move_to:
            logger.warning(f"No se puede mover a la misma categoría: {category}")
            return False
        
        # Asegurar que la categoría destino existe
        if move_to not in self.categories:
            self.add_category(move_to)
        
        # Mover wallets a la nueva categoría
        wallets_to_move = list(self.wallets_by_category.get(category, set()))
        for address in wallets_to_move:
            if address in self.wallets:
                self.wallets[address]["category"] = move_to
                self.wallets_by_category[move_to].add(address)
        
        # Eliminar la categoría
        if category in self.wallets_by_category:
            del self.wallets_by_category[category]
        self.categories.discard(category)
        
        # Guardar cambios si auto_save está activado
        if self.auto_save:
            self.save_wallets()
        
        logger.info(f"Categoría eliminada: {category}, {len(wallets_to_move)} wallets movidas a '{move_to}'")
        return True
    
    def rename_category(self, old_name: str, new_name: str) -> bool:
        """
        Renombra una categoría.
        
        Args:
            old_name: Nombre actual de la categoría
            new_name: Nuevo nombre para la categoría
            
        Returns:
            bool: True si se renombró correctamente
        """
        if old_name not in self.categories:
            logger.warning(f"Categoría no encontrada: {old_name}")
            return False
        
        if not new_name or old_name == new_name:
            return True
        
        # Si la nueva categoría ya existe, fusionar
        if new_name in self.categories:
            # Mover wallets a la categoría existente
            wallets_to_move = list(self.wallets_by_category.get(old_name, set()))
            for address in wallets_to_move:
                if address in self.wallets:
                    self.wallets[address]["category"] = new_name
                    self.wallets_by_category[new_name].add(address)
            
            # Eliminar la categoría antigua
            if old_name in self.wallets_by_category:
                del self.wallets_by_category[old_name]
            self.categories.discard(old_name)
            
            logger.info(f"Categorías fusionadas: '{old_name}' en '{new_name}'")
        else:
            # Crear nueva categoría
            self.categories.add(new_name)
            self.wallets_by_category[new_name] = set(self.wallets_by_category.get(old_name, set()))
            
            # Actualizar wallets
            for address in self.wallets_by_category[new_name]:
                if address in self.wallets:
                    self.wallets[address]["category"] = new_name
            
            # Eliminar la categoría antigua
            if old_name in self.wallets_by_category:
                del self.wallets_by_category[old_name]
            self.categories.discard(old_name)
            
            logger.info(f"Categoría renombrada: '{old_name}' -> '{new_name}'")
        
        # Guardar cambios si auto_save está activado
        if self.auto_save:
            self.save_wallets()
        
        return True
    
    def get_wallet_score(self, wallet: str) -> float:
        """
        Obtiene el score actual de una wallet.
        
        Args:
            wallet: Dirección de la wallet
            
        Returns:
            float: Score de la wallet (0-10) o score por defecto si no existe
        """
        if wallet in self.wallets and "score" in self.wallets[wallet]:
            return self.wallets[wallet]["score"]
        
        # Intentar obtener de la base de datos
        try:
            return db.get_wallet_score(wallet)
        except Exception:
            pass
        
        # Si no se encuentra, usar valor por defecto
        return float(Config.DEFAULT_SCORE)
    
    def update_wallet_score(self, wallet: str, score: float) -> bool:
        """
        Actualiza el score de una wallet.
        
        Args:
            wallet: Dirección de la wallet
            score: Nuevo score (0-10)
            
        Returns:
            bool: True si se actualizó correctamente
        """
        # Validar score
        score = max(0, min(float(score), 10.0))
        
        if wallet in self.wallets:
            # Actualizar en memoria
            self.wallets[wallet]["score"] = score
            self.wallets[wallet]["last_updated"] = time.time()
        else:
            # Crear wallet si no existe
            self.add_wallet(wallet, wallet[:8], "Default", score)
            return True
        
        # Actualizar en la base de datos
        try:
            db.update_wallet_score(wallet, score)
        except Exception as e:
            logger.warning(f"Error actualizando score en BD: {e}")
        
        # Guardar cambios si auto_save está activado
        if self.auto_save:
            self.save_wallets()
        
        logger.info(f"Score actualizado para {wallet}: {score}")
        return True
    
    def register_transaction(self, wallet: str, token: str, tx_type: str, 
                             amount_usd: float) -> None:
        """
        Registra una transacción para actualizar estadísticas.
        Actualmente no tiene efecto directo en la wallet, pero puede
        implementarse para tracking avanzado.
        
        Args:
            wallet: Dirección de la wallet
            token: Dirección del token
            tx_type: Tipo de transacción (BUY, SELL)
            amount_usd: Monto en USD
        """
        # Este método es un stub para futuras implementaciones
        # Por ahora, simplemente verificar si la wallet existe y crearla si no
        if wallet not in self.wallets:
            # Crear wallet con valores por defecto
            self.add_wallet(wallet, wallet[:8], "Default", float(Config.DEFAULT_SCORE))
    
    def get_wallets(self) -> List[str]:
        """
        Obtiene todas las direcciones de wallets.
        
        Returns:
            List[str]: Lista de direcciones de wallet
        """
        # Usar caché si está disponible y es reciente
        now = time.time()
        if self._address_list_cache and now - self._address_list_cache_timestamp < self._cache_ttl:
            return self._address_list_cache.copy()
        
        # Actualizar caché
        self._address_list_cache = list(self.wallets.keys())
        self._address_list_cache_timestamp = now
        
        return self._address_list_cache.copy()
    
    def get_wallet_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas generales de las wallets.
        
        Returns:
            Dict: Estadísticas de wallets
        """
        stats = {
            "total_wallets": len(self.wallets),
            "categories": len(self.categories),
            "wallets_per_category": {cat: len(wallets) for cat, wallets in self.wallets_by_category.items()},
            "high_score_wallets": sum(1 for w in self.wallets.values() if w.get("score", 0) >= 8.0),
            "avg_score": 0
        }
        
        # Calcular score promedio
        if self.wallets:
            total_score = sum(w.get("score", 0) for w in self.wallets.values())
            stats["avg_score"] = total_score / len(self.wallets)
        
        return stats
    
    def export_wallets(self, format: str = "json", path: str = None) -> str:
        """
        Exporta las wallets a un archivo en el formato especificado.
        
        Args:
            format: Formato de exportación ("json" o "csv")
            path: Ruta del archivo de salida (opcional)
            
        Returns:
            str: Ruta del archivo generado o datos en formato string
        """
        if format.lower() == "json":
            # Usar el mismo formato que traders_data.json
            data = []
            for address, info in self.wallets.items():
                entry = {
                    "Wallet": address,
                    "Trader": info.get("name", ""),
                    "Categoria": info.get("category", "Default"),
                    "Puntaje": info.get("score", Config.DEFAULT_SCORE)
                }
                data.append(entry)
            
            if path:
                try:
                    with open(path, 'w') as f:
                        json.dump(data, f, indent=4)
                    return path
                except Exception as e:
                    logger.error(f"Error exportando a JSON: {e}")
                    return ""
            else:
                return json.dumps(data, indent=4)
        
        elif format.lower() == "csv":
            # Formato CSV simple
            lines = ["wallet,name,category,score"]
            for address, info in self.wallets.items():
                line = f"{address},{info.get('name', '')},{info.get('category', 'Default')},{info.get('score', Config.DEFAULT_SCORE)}"
                lines.append(line)
            
            csv_content = "\n".join(lines)
            
            if path:
                try:
                    with open(path, 'w') as f:
                        f.write(csv_content)
                    return path
                except Exception as e:
                    logger.error(f"Error exportando a CSV: {e}")
                    return ""
            else:
                return csv_content
        
        else:
            logger.error(f"Formato de exportación no soportado: {format}")
            return ""
    
    def import_wallets(self, source: str, format: str = "json", merge: bool = True) -> int:
        """
        Importa wallets desde un archivo o string.
        
        Args:
            source: Ruta del archivo o string con datos
            format: Formato de los datos ("json" o "csv")
            merge: Si es True, combina con las wallets existentes
            
        Returns:
            int: Número de wallets importadas
        """
        data = None
        
        # Determinar si source es un archivo o un string con datos
        is_file = os.path.exists(source)
        
        try:
            if format.lower() == "json":
                if is_file:
                    with open(source, 'r') as f:
                        data = json.load(f)
                else:
                    data = json.loads(source)
                
                if not merge:
                    self.wallets.clear()
                    self.categories.clear()
                    self.wallets_by_category.clear()
                
                count = 0
                for entry in data:
                    if "Wallet" not in entry:
                        continue
                    
                    address = entry["Wallet"]
                    category = entry.get("Categoria", "Default")
                    trader_name = entry.get("Trader", "")
                    score = float(entry.get("Puntaje", Config.DEFAULT_SCORE))
                    
                    self.add_wallet(address, trader_name, category, score)
                    count += 1
                
                # Guardar cambios si auto_save está activado
                if self.auto_save:
                    self.save_wallets()
                
                return count
            
            elif format.lower() == "csv":
                if is_file:
                    with open(source, 'r') as f:
                        content = f.read()
                else:
                    content = source
                
                lines = content.strip().split("\n")
                
                # Verificar cabecera
                if lines and "wallet" in lines[0].lower():
                    lines = lines[1:]  # Saltar cabecera
                
                if not merge:
                    self.wallets.clear()
                    self.categories.clear()
                    self.wallets_by_category.clear()
                
                count = 0
                for line in lines:
                    parts = line.split(",")
                    if len(parts) < 4:
                        continue
                    
                    address = parts[0].strip()
                    name = parts[1].strip()
                    category = parts[2].strip()
                    
                    try:
                        score = float(parts[3].strip())
                    except ValueError:
                        score = float(Config.DEFAULT_SCORE)
                    
                    self.add_wallet(address, name, category, score)
                    count += 1
                
                # Guardar cambios si auto_save está activado
                if self.auto_save:
                    self.save_wallets()
                
                return count
            
            else:
                logger.error(f"Formato de importación no soportado: {format}")
                return 0
                
        except Exception as e:
            logger.error(f"Error importando wallets: {e}")
            return 0
