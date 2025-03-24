from bot_service import JiraTelegramBot
from web_app import start_webapp_thread

if __name__ == "__main__":
    # Запускаем веб-приложение в отдельном потоке
    start_webapp_thread()
    
    # Создаем и запускаем бота
    bot = JiraTelegramBot()
    bot.start()
