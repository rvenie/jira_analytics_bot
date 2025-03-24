import telebot
from telebot import types
import pandas as pd
import io
import config
from jira_service import JiraService
from analytics_service import AnalyticsService
from storage_service import StorageService
import schedule
import time
import threading
from datetime import datetime, timedelta
import requests
import logging
from typing import Dict, Any, Optional

class JiraTelegramBot:
    """Telegram-бот для анализа данных Jira с улучшенной обработкой ошибок и логированием"""
    
    def __init__(self):
        """Инициализация бота и необходимых сервисов"""
        # Настройка логирования
        self.logger = self._setup_logger()
        self.logger.info("Инициализация Jira Analytics Bot")
        
        # Инициализация бота и сервисов
        self.bot = telebot.TeleBot(config.TELEGRAM_TOKEN)
        self.jira_service = JiraService()
        self.analytics_service = AnalyticsService(self.jira_service)
        self.storage_service = StorageService()
        
        # Загружаем пользовательские настройки
        self.user_settings = self.storage_service.load_user_settings() or {}
        
        # Инициализация планировщика для еженедельных отчетов
        self.schedule_thread = None
        
        # Регистрируем обработчики команд
        self._register_handlers()
        
        # Запускаем планировщик
        self._schedule_weekly_reports()
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логгера для бота"""
        logger = logging.getLogger('jira_bot')
        logger.setLevel(logging.INFO)
        
        # Проверяем, есть ли уже обработчики
        if not logger.handlers:
            # Создаем обработчик для вывода в консоль
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # Создаем обработчик для записи в файл
            file_handler = logging.FileHandler('bot.log')
            file_handler.setLevel(logging.INFO)
            
            # Создаем форматтер
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)
            
            # Добавляем обработчики к логгеру
            logger.addHandler(console_handler)
            logger.addHandler(file_handler)
        
        return logger
        
    def _register_handlers(self):
        """Регистрация обработчиков команд"""
        # Обработчики команд меню
        @self.bot.message_handler(commands=['notifications'])
        def notifications_settings(message):
            self._handle_notifications_settings(message)

        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            self._handle_start_help(message)
        
        @self.bot.message_handler(commands=['stats'])
        def get_stats(message):
            self._handle_stats(message)
        
        @self.bot.message_handler(commands=['project'])
        def set_project(message):
            self._handle_project(message)
        
        @self.bot.message_handler(commands=['weeks'])
        def set_weeks(message):
            self._handle_weeks(message)
        
        @self.bot.message_handler(commands=['webapp'])
        def open_webapp(message):
            self._handle_webapp(message)
            
        @self.bot.message_handler(commands=['weekly'])
        def weekly_report(message):
            self._handle_weekly_report(message)
            

        @self.bot.message_handler(commands=['logout'])
        def logout(message):
            self._handle_logout(message)
            
        # Обработчик колбэков от инлайн-кнопок
        @self.bot.callback_query_handler(func=lambda call: True)
        def callback_handler(call):
            self._process_callback(call)

        # Этот обработчик должен быть последним
        @self.bot.message_handler(func=lambda message: True)
        def handle_all_messages(message):
            self._handle_token_input(message)

    #------ Обработчики команд ------#

    def _handle_notifications_settings(self, message):
        """Обработка команды /notifications - настройки уведомлений"""
        user_id = message.from_user.id
        
        # Проверка на наличие токена
        if not self._get_user_token(user_id):
            self.bot.reply_to(message, "⚠️ Необходимо указать токен доступа Jira. Используйте /start")
            return
        
        # Получаем текущие настройки
        if user_id not in self.user_settings:
            self.user_settings[user_id] = {}
        
        notifications_enabled = self.user_settings[user_id].get('weekly_notifications', False)
        
        # Создаем инлайн-клавиатуру для выбора
        markup = types.InlineKeyboardMarkup(row_width=2)
        
        enable_button = types.InlineKeyboardButton(
            "✅ Включить" if not notifications_enabled else "✅ Включено", 
            callback_data="enable_notifications"
        )
        
        disable_button = types.InlineKeyboardButton(
            "❌ Отключить" if notifications_enabled else "❌ Отключено", 
            callback_data="disable_notifications"
        )
        
        markup.add(enable_button, disable_button)
        
        # Отправляем сообщение с настройками
        status = "включены" if notifications_enabled else "отключены"
        message_text = (
            "🔔 *Настройки уведомлений*\n\n"
            f"Еженедельные напоминания по пятницам: *{status}*\n\n"
            "Эти уведомления будут напоминать вам о необходимости заполнить журнал работ "
            "и показывать текущее количество отработанных часов за неделю."
        )
        
        self.bot.send_message(message.chat.id, message_text, parse_mode='Markdown', reply_markup=markup)
        self.logger.info(f"Пользователь {user_id} запросил настройки уведомлений")

    def _handle_token_input(self, message):
        """Обработка ввода токена Jira"""
        user_id = message.from_user.id
        
        if user_id in self.user_settings and self.user_settings[user_id].get('waiting_for_token', False):
            token = message.text.strip()
            
            # Удаляем сообщение с токеном для безопасности
            try:
                self.bot.delete_message(message.chat.id, message.message_id)
            except Exception as e:
                self.logger.warning(f"Не удалось удалить сообщение с токеном: {e}")
            
            try:
                # Создаем временный сервис Jira для проверки токена
                temp_jira_service = JiraService(token=token)
                
                # Проверяем группы пользователя
                user_info = temp_jira_service.get_current_user()
                user_groups = temp_jira_service.get_user_groups()
                
                # Сохраняем токен и информацию о пользователе
                self.user_settings[user_id]['token'] = token
                self.user_settings[user_id]['jira_username'] = user_info.get('name', '')
                self.user_settings[user_id]['jira_display_name'] = user_info.get('displayName', '')
                self.user_settings[user_id]['jira_groups'] = user_groups
                self.user_settings[user_id]['waiting_for_token'] = False
                self.user_settings[user_id]['weekly_notifications'] = True  # По умолчанию включаем уведомления
                
                self.storage_service.save_user_settings(self.user_settings)
                self.logger.info(f"Пользователь {user_id} успешно авторизовался")
                
                # Выводим приветственное сообщение
                welcome_text = self._generate_welcome_message(user_id, user_info)
                
                try:
                    self.bot.send_message(message.chat.id, welcome_text, parse_mode='HTML')
                except Exception as e:
                    # Если не удалось отправить с HTML-форматированием, пробуем без него
                    self.logger.warning(f"Не удалось отправить сообщение с HTML: {e}")
                    self.bot.send_message(message.chat.id, self._strip_html_tags(welcome_text))
                
                # Запускаем планировщик
                self._schedule_weekly_reports()
                
            except Exception as e:
                self.logger.error(f"Ошибка при проверке токена для пользователя {user_id}: {e}")
                error_message = f"❌ Ошибка при проверке токена: {str(e)}\nПожалуйста, попробуйте еще раз или обратитесь к администратору."
                self.bot.send_message(message.chat.id, error_message)
    
    def _generate_welcome_message(self, user_id, user_info):
        """Генерация приветственного сообщения с информацией о пользователе и доступных командах"""
        welcome_text = (
            f"✅ Токен принят! Здравствуйте, <b>{user_info.get('displayName', '')}</b>!\n\n"
            "🤖 <b>Jira Analytics Bot</b>\n\n"
            "Я помогу вам анализировать статистику работы в Jira.\n\n"
            "<b>Доступные команды:</b>\n"
            "/stats - Получить статистику по проекту\n"
            "/weekly - Отчет по часам за <b>текущую</b> неделю\n"
            "/logout - Выйти из текущего аккаунта\n"
            "/project &lt;ключ&gt; - Изменить ключ проекта\n"
            "/weeks &lt;число&gt; - Изменить количество недель\n"
            "/notifications - Настройки уведомлений\n"
            # "/webapp - Открыть интерактивную статистику\n"
            "/help - Показать это сообщение\n\n"
            "<b>Текущие настройки:</b>\n"
            f"Проект: <code>{self._get_user_project(user_id)}</code>\n"
            f"Недель: <code>{self._get_user_weeks(user_id)}</code>\n"
            f"Еженедельные уведомления: <code>{self._get_user_notifications(user_id)}</code>\n\n"
        )
        
        # Показываем информацию о правах доступа
        if self._is_admin(user_id):
            welcome_text += "🔑 <b>У вас права администратора Jira</b>\n"
        elif self._is_power_user(user_id):
            welcome_text += "👥 <b>У вас расширенные права пользователя Jira</b>\n"
        else:
            welcome_text += "👤 <b>У вас базовые права пользователя Jira</b>\n"
            
        return welcome_text
    
    def _strip_html_tags(self, text):
        """Удаление HTML-тегов из текста для отправки без форматирования"""
        import re
        return re.sub(r'<.*?>', '', text)
    
    def _handle_start_help(self, message):
        """Обработка команд /start и /help"""
        user_id = message.from_user.id
        
        # Проверяем, есть ли у пользователя токен
        if not self._get_user_token(user_id):
            help_text = (
                "🤖 <b>Jira Analytics Bot</b>\n\n"
                "Для начала работы необходимо указать персональный токен доступа к Jira.\n\n"
                "Пожалуйста, введите ваш токен доступа Jira."
            )
            
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton("🔑 Открыть страницу получения токена", 
                                    url="#INPUT LINK HERE#")
            )
            
            # Устанавливаем состояние ожидания токена
            if user_id not in self.user_settings:
                self.user_settings[user_id] = {}
            self.user_settings[user_id]['waiting_for_token'] = True
            self.storage_service.save_user_settings(self.user_settings)
            
            self.bot.send_message(message.chat.id, 
                                  help_text,
                                  reply_markup=markup,
                                  parse_mode='HTML')
            
            self.logger.info(f"Пользователь {user_id} запустил бота и ожидает ввода токена")
        else:
            """Обработка команд /start и /help для авторизованного пользователя"""
            help_text = (
                "🤖 <b>Jira Analytics Bot</b>\n\n"
                "Я помогу вам анализировать статистику работы в Jira.\n\n"
                "<b>Доступные команды:</b>\n"
                "/stats - Получить статистику по проекту\n"
                "/weekly - Отчет по часам за <b>текущую</b> неделю\n"
                "/logout - Выйти из текущего аккаунта\n"
                "/project &lt;ключ&gt; - Изменить ключ проекта\n"
                "/weeks &lt;число&gt; - Изменить количество недель\n"
                "/notifications - Настройки уведомлений\n"
                # "/webapp - Открыть интерактивную статистику\n"
                "/help - Показать это сообщение\n\n"
                "<b>Текущие настройки:</b>\n"
                f"Проект: <code>{self._get_user_project(message.from_user.id)}</code>\n"
                f"Недель: <code>{self._get_user_weeks(message.from_user.id)}</code>\n"
                f"Еженедельные уведомления: <code>{self._get_user_notifications(message.from_user.id)}</code>\n"
            )
            
            # Добавляем информацию о роли пользователя
            if self._is_admin(user_id):
                help_text += "\n\n🔑 <b>Ваша роль:</b> Администратор Jira"
            elif self._is_power_user(user_id):
                help_text += "\n\n🛠️ <b>Ваша роль:</b> Расширенный пользователь Jira"
            else:
                help_text += "\n\n👤 <b>Ваша роль:</b> Стандартный пользователь Jira"
                
            self.bot.send_message(message.chat.id, help_text, parse_mode='HTML')
            self.logger.info(f"Пользователь {user_id} запросил справку")
    
    def _handle_stats(self, message):
        """Обработка команды /stats"""
        user_id = message.from_user.id
        
        # Проверяем наличие токена
        if not self._get_user_token(user_id):
            self.bot.reply_to(message, "⚠️ Необходимо указать токен доступа Jira. Используйте /start")
            return
        
        project_key = self._get_user_project(user_id)
        weeks_count = self._get_user_weeks(user_id)
        
        # Проверяем права пользователя на просмотр сводной статистики
        is_admin = self._is_admin(user_id)
        is_power_user = self._is_power_user(user_id)
        
        # Отправляем сообщение о начале анализа
        processing_msg = self.bot.send_message(
            message.chat.id, 
            f"⏳ Анализирую проект *{project_key}* за последние *{weeks_count}* недель...",
            parse_mode='Markdown'
        )
        self.logger.info(f"Пользователь {user_id} запросил статистику")
        try:
            # Получаем и анализируем данные в зависимости от прав
            if is_admin or is_power_user:
                # Полная статистика для админов и power users
                weekly_stats = self.analytics_service.analyze_project(project_key, weeks_count)
            else:
                # Только собственная статистика для обычных пользователей
                username = self.user_settings[user_id].get('jira_username', '')
                weekly_stats = self.analytics_service.analyze_user_project(project_key, weeks_count, username)
            
            # Сохраняем результаты
            self.storage_service.save_stats(project_key, weekly_stats)
            
            # Сохраняем в настройках пользователя
            if user_id not in self.user_settings:
                self.user_settings[user_id] = {}
            
            self.user_settings[user_id]['last_stats'] = weekly_stats
            self.storage_service.save_user_settings(self.user_settings)
            
            # Создаем кнопки для выбора отчета
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton(f"📊 Сводная статистика по отработанным часам за {self._get_user_weeks(user_id)} недель", callback_data="hours_report"),
                types.InlineKeyboardButton(f"🚫 Задачи без записи журнале работ за {self._get_user_weeks(user_id)} недель", callback_data="no_worklog_report"),
                types.InlineKeyboardButton("📈 Количество задач по неделям", callback_data="tasks_count_report")
                # types.InlineKeyboardButton("🌐 Открыть интерактивную статистику", 
                #                     url="http://51.250.114.37:8000/")
            )
            
            self.bot.edit_message_text(
                f"✅ Анализ завершен! Выберите тип отчета:",
                message.chat.id,
                processing_msg.message_id,
                reply_markup=markup
            )
            
        except Exception as e:
            self.bot.edit_message_text(
                f"❌ Ошибка при анализе:\n`{str(e)}`",
                message.chat.id,
                processing_msg.message_id,
                parse_mode='Markdown'
            )
    
    def _handle_webapp(self, message):
        """Обработка команды /webapp"""
        user_id = message.from_user.id
        
        # Проверяем наличие токена
        if not self._get_user_token(user_id):
            self.bot.reply_to(message, "⚠️ Необходимо указать токен доступа Jira. Используйте /start")
            return
            
        project_key = self._get_user_project(user_id)
        weeks_count = self._get_user_weeks(user_id)
        
        # Отправляем ссылку на веб-интерфейс
        markup = types.InlineKeyboardMarkup()
        webapp_button = types.InlineKeyboardButton(
            text="📊 Открыть интерактивную статистику", 
            url=f"http://51.250.114.37:8000/?user_id={user_id}&project={project_key}&weeks={weeks_count}"
        )
        markup.add(webapp_button)
        
        self.bot.send_message(
            message.chat.id,
            f"📱 Нажмите на кнопку ниже, чтобы открыть интерактивную статистику для проекта *{project_key}* за *{weeks_count}* недель.",
            reply_markup=markup,
            parse_mode='Markdown'
        )
    
    def _handle_weekly_report(self, message):
        """Обработка команды /weekly_report - отчет за текущую неделю"""
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        # Проверка на токен
        if not self._get_user_token(user_id):
            self.bot.reply_to(message, "⚠️ Необходимо указать токен доступа Jira. Используйте /start")
            return
        
        try:
            # Получаем данные за текущую неделю
            username = self.user_settings[user_id].get('jira_username', '')
            current_week_stats = self.analytics_service.analyze_current_week(username)
            
            total_hours = current_week_stats.get('total_hours', 0)
            remaining_hours = max(0, 40 - total_hours)
            
            # Формируем текст отчета
            report_text = f"📊 *Отчет о работе за текущую неделю*\n\n"
            report_text += f"👤 Пользователь: *{self.user_settings[user_id].get('jira_display_name', '')}*\n"
            report_text += f"⏱ Отработано часов: *{total_hours:.2f}* из 40.0\n"
            
            # Прогресс-бар
            progress = int(min(total_hours / 40 * 20, 20))
            progress_bar = "["
            progress_bar += "█" * progress
            progress_bar += "░" * (20 - progress)
            progress_bar += "]"
            
            report_text += f"{progress_bar}\n\n"
            
            # Проверка на достаточное количество часов
            if total_hours < 40:
                report_text += f"⚠️ *Внимание!* Вам осталось отработать еще *{remaining_hours:.2f}* часов до нормы в 40 часов.\n\n"
            else:
                report_text += f"✅ *Поздравляем!* Вы отработали норму в 40 часов на этой неделе.\n\n"
            
            self.bot.send_message(chat_id, report_text, parse_mode='Markdown')
            
        except Exception as e:
            self.bot.send_message(
                chat_id, 
                f"❌ Ошибка при формировании отчета: `{str(e)}`",
                parse_mode='Markdown'
            )


    
    def _handle_project(self, message):
        """Обработка команды /project"""
        user_id = message.from_user.id
        
        # Проверка на токен
        if not self._get_user_token(user_id):
            self.bot.reply_to(message, "⚠️ Необходимо указать токен доступа Jira. Используйте /start")
            return
            
        args = message.text.split()
        
        if len(args) != 2:
            self.bot.reply_to(message, "⚠️ Укажите ключ проекта. Пример: `/project PRO`", parse_mode='Markdown')
            return
        
        project_key = args[1].upper()
        
        # Сохраняем настройку
        if message.from_user.id not in self.user_settings:
            self.user_settings[message.from_user.id] = {}
        
        self.user_settings[message.from_user.id]['project'] = project_key
        self.storage_service.save_user_settings(self.user_settings)
        
        self.bot.reply_to(message, f"✅ Установлен проект: *{project_key}*", parse_mode='Markdown')
    
    def _handle_weeks(self, message):
        """Обработка команды /weeks"""
        user_id = message.from_user.id
        
        # Проверка на токен
        if not self._get_user_token(user_id):
            self.bot.reply_to(message, "⚠️ Необходимо указать токен доступа Jira. Используйте /start")
            return
            
        args = message.text.split()
        
        if len(args) != 2:
            self.bot.reply_to(message, "⚠️ Укажите количество недель. Пример: `/weeks 4`", parse_mode='Markdown')
            return
        
        try:
            weeks = int(args[1])
            if weeks < 1 or weeks > 12:
                self.bot.reply_to(message, "⚠️ Количество недель должно быть от 1 до 12")
                return
            
            # Сохраняем настройку
            if message.from_user.id not in self.user_settings:
                self.user_settings[message.from_user.id] = {}
            
            self.user_settings[message.from_user.id]['weeks'] = weeks
            self.storage_service.save_user_settings(self.user_settings)
            
            self.bot.reply_to(message, f"✅ Установлено недель: *{weeks}*", parse_mode='Markdown')
            
        except ValueError:
            self.bot.reply_to(message, "⚠️ Количество недель должно быть числом")
    
    def _handle_logout(self, message):
        """Обработка команды /logout - выход из аккаунта"""
        user_id = message.from_user.id
        
        if user_id in self.user_settings and 'token' in self.user_settings[user_id]:
            # Сохраняем проект и недели
            project = self.user_settings[user_id].get('project')
            weeks = self.user_settings[user_id].get('weeks')
            
            # Очищаем данные пользователя, кроме базовых настроек
            self.user_settings[user_id] = {
                'project': project,
                'weeks': weeks,
                'waiting_for_token': True
            }
            
            # Сохраняем обновленные настройки
            self.storage_service.save_user_settings(self.user_settings)
            self.logger.info(f"Пользователь {user_id} выполнил выход из системы")
            
            self.bot.send_message(
                message.chat.id,
                "🔒 <b>Выход выполнен успешно!</b>\n\nВаши настройки проекта и недель сохранены.\n\nПожалуйста, введите новый токен доступа Jira.",
                parse_mode='HTML'
            )
        else:
            self.bot.send_message(
                message.chat.id,
                "⚠️ Вы еще не авторизованы. Используйте команду /start для входа в систему.",
                parse_mode='HTML'
            )
    # Новый метод класса для устранения дублирования
    def _get_unique_no_worklog_tasks(self, weekly_stats):
        """Возвращает уникальные задачи без журнала работ из статистики"""
        tasks = []
        seen_keys = set()
        
        for week_data in weekly_stats.values():
            if 'tasks_without_worklog' in week_data:
                for task in week_data['tasks_without_worklog']:
                    if task['key'] not in seen_keys:
                        seen_keys.add(task['key'])
                        tasks.append(task)
        return tasks

    def _process_callback(self, call):
        """Обработка колбэков от кнопок"""
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id

        # Обработка настроек уведомлений
        if call.data == "enable_notifications":
            # Включаем уведомления
            if user_id not in self.user_settings:
                self.user_settings[user_id] = {}
            self.user_settings[user_id]['weekly_notifications'] = True
            self.storage_service.save_user_settings(self.user_settings)
            
            # Обновляем сообщение
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton("✅ Включено", callback_data="enable_notifications"),
                types.InlineKeyboardButton("❌ Отключить", callback_data="disable_notifications")
            )
            
            message_text = (
                "🔔 *Настройки уведомлений*\n\n"
                "Еженедельные напоминания по пятницам: *включены*\n\n"
                "Эти уведомления будут напоминать вам о необходимости заполнить журнал работ "
                "и показывать текущее количество отработанных часов за неделю."
            )
            
            self.bot.edit_message_text(
                message_text, 
                chat_id, 
                message_id, 
                parse_mode='Markdown', 
                reply_markup=markup
            )
            
            # Уведомляем о включении
            self.bot.answer_callback_query(call.id, "✅ Еженедельные уведомления включены")

        elif call.data == "download_no_worklog_excel":
            # Получаем сохраненную статистику
            if user_id not in self.user_settings or 'last_stats' not in self.user_settings[user_id]:
                self.bot.send_message(chat_id, "❌ Статистика не найдена. Сначала выполните /stats")
                return
            
            weekly_stats = self.user_settings[user_id]['last_stats']
            
            # Выносим общую логику в отдельный метод
            no_worklog_tasks = self._get_unique_no_worklog_tasks(weekly_stats)
            
            if not no_worklog_tasks:
                self.bot.send_message(chat_id, f"🎉 Задач без журнала работ за {self._get_user_weeks(user_id)} недель не найдено!")
                return
            
            try:
                # Создаем DataFrame
                data = [{
                    'Код': task['key'],
                    'Название': task['summary'],
                    'Статус': task.get('status', 'Не указан'),
                    'Исполнитель': task.get('assignee', 'Не назначен'),
                    'Оценка (ч)': task.get('estimated_hours', 0)
                } for task in no_worklog_tasks]
                
                df = pd.DataFrame(data)
                self._send_excel_report(chat_id, df, 
                                    f"Найдено {len(no_worklog_tasks)} задач без журнала работ", 
                                    "no_worklog_tasks.xlsx")
                
            except Exception as e:
                self.bot.send_message(chat_id, f"❌ Ошибка при создании отчета: {str(e)}")


        elif call.data == "disable_notifications":
            # Отключаем уведомления
            if user_id in self.user_settings:
                self.user_settings[user_id]['weekly_notifications'] = False
                self.storage_service.save_user_settings(self.user_settings)
                
            # Обновляем сообщение
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton("✅ Включить", callback_data="enable_notifications"),
                types.InlineKeyboardButton("❌ Отключено", callback_data="disable_notifications")
            )
            
            message_text = (
                "🔔 *Настройки уведомлений*\n\n"
                "Еженедельные напоминания по пятницам: *отключены*\n\n"
                "Эти уведомления будут напоминать вам о необходимости заполнить журнал работ "
                "и показывать текущее количество отработанных часов за неделю."
            )
            
            self.bot.edit_message_text(
                message_text, 
                chat_id, 
                message_id, 
                parse_mode='Markdown', 
                reply_markup=markup
            )
            
            # Уведомляем об отключении
            self.bot.answer_callback_query(call.id, "❌ Еженедельные уведомления отключены")

                
        # Проверяем наличие данных
        if user_id not in self.user_settings or 'last_stats' not in self.user_settings[user_id]:
            self.bot.answer_callback_query(call.id, "Данные не найдены. Запустите /stats снова")
            return
        
        weekly_stats = self.user_settings[user_id]['last_stats']

        def escape_markdown(text):
            """Экранирует специальные символы Markdown"""
            if not isinstance(text, str):
                text = str(text)
            escape_chars = r'_*[]()~`>#+-=|{}.!'
            return ''.join(r'\{}'.format(c) if c in escape_chars else c for c in text)

        # Сводная статистика
        if call.data == "hours_report":
            # Отчет по часам в сообщении
            df = self.analytics_service.create_hours_report(weekly_stats)
            
            # Создаем текстовое сообщение со статистикой
            message_text = f"📊 <b>Статистика по отработанным часам за {self._get_user_weeks(user_id)} недель</b>\n\n"
            
            # Добавляем топ-5 пользователей по часам
            top_users = df.loc[df.index != 'Всего'].sort_values('Всего часов', ascending=False).head(5)
            for user, row in top_users.iterrows():
                escaped_user = escape_markdown(user)
                message_text += f"• {user}: <b>{row['Всего часов']}</b> часов\n"
                
                # Получаем список задач для пользователя и отображаем их
                try:
                    # Используем full_data для получения задач с журналами работ
                    user_tasks_with_worklog = []
                    
                    # Проверяем наличие полных данных
                    if '_full_data' in weekly_stats and 'all_worklogs' in weekly_stats['_full_data']:
                        all_worklogs = weekly_stats['_full_data']['all_worklogs']
                        all_issues = weekly_stats['_full_data'].get('all_issues', {})
                        
                        # Собираем задачи с журналами работ для текущего пользователя
                        for issue_key, worklogs in all_worklogs.items():
                            issue_worklogs = [w for w in worklogs if w.get('author') == user]
                            
                            if issue_worklogs:
                                # Вычисляем общее количество часов в этой задаче
                                total_hours = sum(w.get('hours', 0) for w in issue_worklogs)
                                
                                # Получаем информацию о задаче
                                issue_info = all_issues.get(issue_key, {})
                                summary = issue_info.get('summary', 'Нет названия')
                                
                                user_tasks_with_worklog.append({
                                    'key': issue_key,
                                    'name': summary,
                                    'hours': round(total_hours, 2)
                                })
                    
                    # Добавляем информацию о задачах в сообщение, если они есть
                    if user_tasks_with_worklog:
                        message_text += "  Задачи с зарегистрированным временем:\n"
                        for task in user_tasks_with_worklog[:10]:
                            escaped_key = escape_markdown(task['key'])
                            escaped_name = escape_markdown(task['name'][:50])
                            message_text += f"  - [{task['key']}] {task['name'][:50]}...: {task['hours']}ч\n"
                        
                        if len(user_tasks_with_worklog) > 10:
                            message_text += f"  ... и еще {len(user_tasks_with_worklog) - 10} задач\n"
                    
                    message_text += "\n"
                        
                except Exception as e:
                    self.logger.error(f"Ошибка при обработке задач: {str(e)}")
            
            # Добавляем общую сумму
            total_hours = df.loc['Всего', 'Всего часов']
            message_text += f"\nОбщее количество часов: *{total_hours}*\n\n"
            
            # Добавляем кнопку для скачивания полного отчета
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton(
                "📥 Скачать полный отчет Excel", 
                callback_data="download_hours_excel"
            ))
            
            self.bot.send_message(chat_id, message_text, parse_mode='HTML', reply_markup=markup)

            
        elif call.data == "toggle_weekly_reports":
            # Переключаем настройку
            if user_id not in self.user_settings:
                self.user_settings[user_id] = {}
            
            current_state = self.user_settings[user_id].get('weekly_reports_enabled', False)
            self.user_settings[user_id]['weekly_reports_enabled'] = not current_state
            self.storage_service.save_user_settings(self.user_settings)
        
        elif call.data == "download_hours_excel":
            # Скачивание Excel отчета
            df = self.analytics_service.create_hours_report(weekly_stats)
            self._send_excel_report(chat_id, df, "Статистика по отработанным часам", "hours_report.xlsx")
            
        elif call.data == "no_worklog_report":
            # Проверяем наличие данных о задачах без журнала
            no_worklog_tasks = []
            for week_data in weekly_stats.values():
                if 'tasks_without_worklog' in week_data:
                    no_worklog_tasks.extend(week_data['tasks_without_worklog'])
            
            if not no_worklog_tasks:
                self.bot.send_message(chat_id, f"🎉 Задач без журнала работ за {self._get_user_weeks(user_id)} недель не найдено!")
                return
            
            # Удаляем дубликаты задач ПЕРЕД группировкой
            seen_task_keys = set()
            unique_tasks = []
            for task in no_worklog_tasks:
                task_key = task['key']
                if task_key not in seen_task_keys:
                    seen_task_keys.add(task_key)
                    unique_tasks.append(task)
            no_worklog_tasks = unique_tasks
            
            # Создаем текстовое сообщение с информацией о задачах
            message_text = f"🚫 *Задачи без журнала работ за {self._get_user_weeks(user_id)} недель:*\n\n"
            
            # Группируем задачи по исполнителям
            assignees = {}
            for task in no_worklog_tasks:
                assignee = task.get('assignee', 'Не назначен')
                if assignee not in assignees:
                    assignees[assignee] = []
                assignees[assignee].append(task)
            
            # Выводим информацию по исполнителям
            for assignee, tasks in assignees.items():
                message_text += f"• *{assignee}*: {len(tasks)} задач\n"
                
                # Вывод первых 3-5 задач для исполнителя
                for i, task in enumerate(tasks[:3]):
                    message_text += f"  - [{task['key']}] {task['summary'][:25]}...\n"
                    message_text += f"    Статус: *{task.get('status', 'Не указан')}*\n"
                
                if len(tasks) > 3:
                    message_text += f"  ... и еще {len(tasks) - 3} задач\n"
                
                message_text += "\n"
            
            # Добавляем общую статистику
            message_text += f"📊 *Общее количество задач без журнала:* {len(no_worklog_tasks)}\n\n"
            
            # Добавляем кнопку для скачивания полного отчета
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton(
                "📥 Скачать полный список Excel", 
                callback_data="download_no_worklog_excel"
            ))
            
            self.bot.send_message(chat_id, message_text, parse_mode='Markdown', reply_markup=markup)


            
        elif call.data == "tasks_count_report":
            # Отчет по количеству задач
            tasks_count = self.analytics_service.create_tasks_count_report(weekly_stats)
            
            message_text = "📈 *Количество задач по неделям:*\n\n"
            for week, count in tasks_count.items():
                message_text += f"• {week}: *{count}* задач\n"
            
            self.bot.send_message(chat_id, message_text, parse_mode='Markdown')
        
        # Отмечаем обработку колбэка
        self.bot.answer_callback_query(call.id)
    
    def _send_excel_report(self, chat_id, df, caption, filename):
        """Отправка DataFrame в виде Excel-файла"""
        # Создаем файл в памяти
        with io.BytesIO() as excel_file:
            df.to_excel(excel_file, engine='openpyxl')
            excel_file.seek(0)
            
            # Отправляем документ пользователю
            self.bot.send_document(
                chat_id,
                excel_file,
                caption=caption,
                visible_file_name=filename
            )
    
    def _schedule_weekly_reports(self):
        """Настройка расписания для еженедельных отчетов"""
        if self.schedule_thread is not None and self.schedule_thread.is_alive():
            return
        
        # Настраиваем расписание для отправки отчетов по пятницам
        schedule.every().friday.at("12:00").do(self._send_weekly_reports_to_all)
        
        # Запускаем планировщик в отдельном потоке
        self.schedule_thread = threading.Thread(target=self._run_scheduler)
        self.schedule_thread.daemon = True
        self.schedule_thread.start()
    
    def _run_scheduler(self):
        """Запуск планировщика задач"""
        while True:
            schedule.run_pending()
            time.sleep(360)  # Проверяем расписание каждый час

    
    def _send_weekly_reports_to_all(self):
        """Отправка еженедельных отчетов всем пользователям"""
        for user_id, settings in self.user_settings.items():
            # Проверяем, включены ли уведомления
            if not settings.get('weekly_notifications', True):
                continue
                
            # Проверяем, есть ли токен и имя пользователя
            if 'jira_username' in settings and 'token' in settings:
                try:
                    username = settings.get('jira_username', '')
                    current_week_stats = self.analytics_service.analyze_current_week(username)
                    
                    total_hours = current_week_stats.get('total_hours', 0)
                    remaining_hours = max(0, 40 - total_hours)
                    
                    # Формируем текст сообщения
                    report_text = f"📊 *Еженедельный отчет о времени работы*\n\n"
                    report_text += f"👤 Пользователь: *{settings.get('jira_display_name', '')}*\n"
                    report_text += f"⏱ Отработано часов за неделю: *{total_hours:.2f}* из 40.0\n\n"
                    
                    # Прогресс-бар (40 часов = 100%)
                    progress_percent = min(total_hours / 40 * 100, 100)
                    bar_length = 20  # Длина прогресс-бара
                    filled_blocks = int(bar_length * progress_percent / 100)
                    
                    progress_bar = "["
                    progress_bar += "█" * filled_blocks
                    progress_bar += "░" * (bar_length - filled_blocks)
                    progress_bar += f"] {progress_percent:.1f}%\n\n"
                    
                    report_text += progress_bar
                    
                    # Проверяем нормы
                    if total_hours < 40:
                        report_text += f"⚠️ *Внимание!* Вам осталось отработать еще *{remaining_hours:.2f}* часов до нормы в 40 часов.\n\n"
                        report_text += "Пожалуйста, не забудьте заполнить журнал работ в ваших задачах Jira!\n"
                        
                        # Добавляем напоминание с кнопкой для списка задач без журнала
                        markup = types.InlineKeyboardMarkup()
                        markup.add(types.InlineKeyboardButton(
                            "🔍 Показать задачи без журнала", 
                            callback_data="no_worklog_report"
                        ))
                        
                        self.bot.send_message(
                            user_id, 
                            report_text, 
                            parse_mode='Markdown',
                            reply_markup=markup
                        )
                    else:
                        report_text += "✅ *Поздравляем!* Вы отработали норму в 40 часов на этой неделе.\n"
                        self.bot.send_message(user_id, report_text, parse_mode='Markdown')
                        
                except Exception as e:
                    self.logger.error(f"Ошибка при отправке отчета пользователю {user_id}: {str(e)}")
    
    def _get_user_token(self, user_id):
        """Получение токена Jira для пользователя"""
        if user_id in self.user_settings and 'token' in self.user_settings[user_id]:
            return self.user_settings[user_id]['token']
        return None
    
    def _get_user_project(self, user_id):
        """Получение проекта для пользователя"""
        if user_id in self.user_settings and 'project' in self.user_settings[user_id]:
            return self.user_settings[user_id]['project']
        return config.DEFAULT_PROJECT_KEY
    
    def _get_user_weeks(self, user_id):
        """Получение количества недель для пользователя"""
        if user_id in self.user_settings and 'weeks' in self.user_settings[user_id]:
            return self.user_settings[user_id]['weeks']
        return config.DEFAULT_WEEKS_COUNT
    
    def _get_user_notifications(self, user_id):
        """Получение значения настройки уведомлений для пользователя"""
        if user_id in self.user_settings and 'weekly_notifications' in self.user_settings[user_id]:
            return "включены" if self.user_settings[user_id]['weekly_notifications'] else "отключены"
        return "не настроены"
    
    def _is_admin(self, user_id):
        """Проверка, является ли пользователь администратором Jira"""
        if user_id in self.user_settings and 'jira_groups' in self.user_settings[user_id]:
            groups = self.user_settings[user_id]['jira_groups']
            return any(group.lower() in ['jira-administrators', 'jira-software-administrators'] 
                      for group in groups)
        return False

    def _is_power_user(self, user_id):
        """Проверка, является ли пользователь в группе jira-software-users"""
        if user_id in self.user_settings and 'jira_groups' in self.user_settings[user_id]:
            groups = self.user_settings[user_id]['jira_groups']
            return any(group.lower() in ['jira-software-users'] for group in groups)
        return False
    
    def start(self):
        """Запуск бота"""
        self.logger.info("Jira Analytics Bot запущен и готов к работе")
        
        # Удаляем webhook и запускаем polling
        self.bot.remove_webhook()
        while True:
            try:
                self.bot.polling(none_stop=True, timeout=30)
            except requests.exceptions.ReadTimeout:
                self.logger.warning("Произошел таймаут. Переподключение...")
            except requests.exceptions.ConnectionError:
                self.logger.error("Ошибка подключения. Повторная попытка через 5 секунд...")
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"Неизвестная ошибка: {e}")
                time.sleep(5)
