import json
import os
import logging
from datetime import datetime
import pandas as pd
import pickle
import config

class StorageService:
    """Сервис для хранения и кэширования данных"""
    
    def __init__(self, data_dir=None):
        """Инициализация хранилища данных"""
        self.data_dir = data_dir or config.DATA_FOLDER
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Настройка логирования
        self.logger = logging.getLogger('storage_service')
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def save_stats(self, project_key, weekly_stats):
        """Сохранение статистики проекта"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{self.data_dir}/{project_key}_stats_{timestamp}.pkl"
            
            with open(filename, 'wb') as f:
                pickle.dump(weekly_stats, f)
            
            self.logger.info(f"Статистика сохранена: {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении статистики: {str(e)}")
            return None
    
    def load_latest_stats(self, project_key):
        """Загрузка последней статистики проекта"""
        try:
            if not os.path.exists(self.data_dir):
                self.logger.warning(f"Директория {self.data_dir} не существует")
                return None
                
            files = [f for f in os.listdir(self.data_dir) 
                    if f.startswith(f"{project_key}_stats_") and f.endswith('.pkl')]
            
            if not files:
                self.logger.info(f"Файлы статистики для проекта {project_key} не найдены")
                return None
            
            latest_file = sorted(files, reverse=True)[0]
            
            with open(f"{self.data_dir}/{latest_file}", 'rb') as f:
                data = pickle.load(f)
                self.logger.info(f"Загружена статистика из файла: {latest_file}")
                return data
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке статистики: {str(e)}")
            return None
    
    def _convert_datetime_to_str(self, data):
        """Рекурсивно преобразует все объекты datetime в строки ISO формата"""
        if isinstance(data, datetime):
            return data.isoformat()
        elif isinstance(data, dict):
            return {k: self._convert_datetime_to_str(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_datetime_to_str(item) for item in data]
        else:
            return data

    def save_user_settings(self, user_settings):
        """Сохранение пользовательских настроек с рекурсивной обработкой datetime"""
        try:
            settings_file = f"{self.data_dir}/user_settings.json"
            backup_file = f"{self.data_dir}/user_settings_backup.json"
            
            # Создаем директорию, если она не существует
            os.makedirs(os.path.dirname(settings_file), exist_ok=True)
            
            # Создаем резервную копию, если файл существует
            if os.path.exists(settings_file):
                with open(settings_file, 'r', encoding='utf-8') as src:
                    with open(backup_file, 'w', encoding='utf-8') as dst:
                        dst.write(src.read())
                self.logger.info(f"Создана резервная копия настроек")
            
            # Преобразуем datetime в строки и идентификаторы пользователей в строки
            settings_to_save = {}
            for user_id, data in user_settings.items():
                # Рекурсивно преобразуем все объекты datetime
                converted_data = self._convert_datetime_to_str(data)
                settings_to_save[str(user_id)] = converted_data
            
            # Сохраняем настройки в файл
            with open(settings_file, 'w', encoding='utf-8') as f:
                json.dump(settings_to_save, f, ensure_ascii=False, indent=2)
                
            self.logger.info(f"Настройки пользователей сохранены")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении настроек пользователей: {str(e)}")
            return False


        
    def load_user_settings(self):
        """Загрузка пользовательских настроек"""
        settings_file = f"{self.data_dir}/user_settings.json"
        backup_file = f"{self.data_dir}/user_settings_backup.json"
        
        if not os.path.exists(settings_file):
            # Проверяем, есть ли резервная копия
            if os.path.exists(backup_file):
                self.logger.warning(f"Файл настроек не найден, восстанавливаем из резервной копии")
                os.rename(backup_file, settings_file)
            else:
                # Если файл не существует, создаем его с пустым объектом
                self.logger.info(f"Создан новый файл настроек: {settings_file}")
                with open(settings_file, 'w', encoding='utf-8') as f:
                    f.write('{}')
                return {}
        
        try:
            with open(settings_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:  # Если файл пустой
                    self.logger.warning("Файл настроек пуст")
                    return {}
                # Преобразуем идентификаторы пользователей обратно в int
                settings = json.loads(content)
                return {int(user_id): data for user_id, data in settings.items()}
        except json.JSONDecodeError:
            self.logger.error("Не удалось декодировать user_settings.json")
            
            # Пробуем восстановить из резервной копии
            if os.path.exists(backup_file):
                self.logger.info("Восстанавливаем настройки из резервной копии")
                try:
                    with open(backup_file, 'r', encoding='utf-8') as f:
                        backup_content = f.read().strip()
                        if backup_content:
                            settings = json.loads(backup_content)
                            # Сохраняем восстановленные настройки
                            with open(settings_file, 'w', encoding='utf-8') as f:
                                f.write(backup_content)
                            return {int(user_id): data for user_id, data in settings.items()}
                except:
                    self.logger.error("Не удалось восстановить из резервной копии")
            
            # Если восстановление не удалось, перезаписываем файл корректным JSON
            with open(settings_file, 'w', encoding='utf-8') as f:
                f.write('{}')
            return {}
        except Exception as e:
            self.logger.error(f"Ошибка при чтении файла настроек: {str(e)}")
            return {}
    
    def get_user_setting(self, user_id, key, default=None):
        """Получение конкретной настройки пользователя"""
        try:
            settings = self.load_user_settings()
            if user_id in settings and key in settings[user_id]:
                return settings[user_id][key]
            return default
        except Exception as e:
            self.logger.error(f"Ошибка при получении настройки пользователя: {str(e)}")
            return default
    
    def set_user_setting(self, user_id, key, value):
        """Установка конкретной настройки пользователя"""
        try:
            settings = self.load_user_settings()
            if user_id not in settings:
                settings[user_id] = {}
            settings[user_id][key] = value
            return self.save_user_settings(settings)
        except Exception as e:
            self.logger.error(f"Ошибка при установке настройки пользователя: {str(e)}")
            return False
    
    def cleanup_old_stats(self, max_files=20):
        """Очистка старых файлов статистики"""
        try:
            # Получаем список всех файлов статистики
            files = [f for f in os.listdir(self.data_dir) 
                    if f.endswith('.pkl') and '_stats_' in f]
            
            if len(files) <= max_files:
                return 0
            
            # Сортируем файлы по времени модификации
            files_with_time = [(f, os.path.getmtime(os.path.join(self.data_dir, f))) for f in files]
            files_sorted = [f for f, _ in sorted(files_with_time, key=lambda x: x[1])]
            
            # Удаляем самые старые файлы
            files_to_delete = files_sorted[:len(files_sorted) - max_files]
            for file in files_to_delete:
                file_path = os.path.join(self.data_dir, file)
                os.remove(file_path)
                self.logger.info(f"Удален устаревший файл статистики: {file}")
            
            return len(files_to_delete)
        except Exception as e:
            self.logger.error(f"Ошибка при очистке старых файлов: {str(e)}")
            return 0
