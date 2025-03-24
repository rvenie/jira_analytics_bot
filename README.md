jira_analytics_bot/
├── config.py             # Конфигурация приложения
├── jira_service.py       # Сервис для работы с Jira API
├── analytics_service.py  # Сервис для аналитики данных
├── storage_service.py    # Сервис для хранения и кэширования данных
├── bot_service.py        # Основной сервис Telegram-бота
├── web_app.py            # Модуль для обработки Web App (FastAPI)
├── web/                  # Статические файлы для Web App
│   ├── css/              # Стили
│   │   └── style.css
│   ├── js/               # JavaScript файлы
│   │   └── main.js
│   └── templates/        # HTML шаблоны
│       └── index.html    # Главная страница Web App
├── main.py               # Точка входа в приложение
└── requirements.txt      # Зависимости
