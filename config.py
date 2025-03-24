import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройки Jira
JIRA_URL = os.getenv('JIRA_URL')
JIRA_USERNAME = os.getenv('JIRA_USERNAME')
JIRA_TOKEN = os.getenv('JIRA_TOKEN')
DEFAULT_PROJECT_KEY = os.getenv('DEFAULT_PROJECT_KEY')

# Настройки Telegram
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')

# Настройки приложения по умолчанию
DEFAULT_WEEKS_COUNT = int(os.getenv('DEFAULT_WEEKS_COUNT', 4))
DATA_FOLDER = os.getenv('DATA_FOLDER', 'data')

# Настройки Web App
WEBAPP_HOST = os.getenv('WEBAPP_HOST', '0.0.0.0')
WEBAPP_PORT = int(os.getenv('WEBAPP_PORT', 8000))
WEBAPP_URL = f"http://{os.getenv('WEBAPP_IP')}:{WEBAPP_PORT}"
