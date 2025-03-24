from fastapi import FastAPI, Request, Depends, HTTPException, Header
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import json
import pandas as pd
from io import BytesIO
import os
import asyncio
from threading import Thread
from typing import Optional

from jira_service import JiraService
from analytics_service import AnalyticsService
from storage_service import StorageService
import config

# Создаем FastAPI приложение
app = FastAPI(title="Jira Analytics Web App")

# Настройка CORS для работы с Telegram WebApp
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшн стоит указать только доверенные источники
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем статические файлы и шаблоны
app.mount("/static", StaticFiles(directory="web/css"), name="css")
app.mount("/js", StaticFiles(directory="web/js"), name="js")
templates = Jinja2Templates(directory="web/templates")

# Инициализируем сервисы
jira_service = JiraService()
analytics_service = AnalyticsService(jira_service)
storage_service = StorageService()

# Функция для проверки токена (опционально для защиты API)
async def validate_token(jira_token: Optional[str] = Header(None, alias="X-Jira-Token")):
    if not jira_token:
        return None
    try:
        # Проверяем токен, создавая временный экземпляр JiraService
        temp_service = JiraService(token=jira_token)
        # Если успешно, возвращаем информацию о пользователе
        return temp_service.get_current_user()
    except Exception:
        return None

@app.get("/")
async def root(request: Request):
    """Главная страница приложения"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/stats")
async def get_stats(project_key: str = None, weeks: int = None, user_id: int = None, 
                    current_user = Depends(validate_token)):
    """API для получения статистики из Jira"""
    # Используем настройки пользователя, если они переданы
    if user_id:
        user_settings = storage_service.load_user_settings().get(user_id, {})
        project_key = project_key or user_settings.get('project', config.DEFAULT_PROJECT_KEY)
        weeks = weeks or user_settings.get('weeks', config.DEFAULT_WEEKS_COUNT)
    else:
        project_key = project_key or config.DEFAULT_PROJECT_KEY
        weeks = weeks or config.DEFAULT_WEEKS_COUNT
    
    try:
        # Проверяем, запрашивает ли пользователь только свои данные
        username = None
        if current_user:
            # Проверяем, есть ли у пользователя расширенные права
            user_groups = jira_service.get_user_groups()
            is_admin = any(group.lower() in ['jira-administrators', 'jira-software-administrators'] 
                           for group in user_groups)
            
            # Если не админ, ограничиваем данные только этим пользователем
            if not is_admin:
                username = current_user.get('name')
        
        # Анализируем данные (с ограничением по пользователю, если требуется)
        if username:
            weekly_stats = analytics_service.analyze_user_project(project_key, weeks, username)
        else:
            weekly_stats = analytics_service.analyze_project(project_key, weeks)
        
        # Готовим результаты в формате для Web App
        result = prepare_stats_for_webapp(weekly_stats)
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/hours-chart")
async def get_hours_chart(project_key: str = None, weeks: int = None, user_id: int = None):
    """API для получения данных по часам в формате для графиков"""
    # Аналогично получаем настройки
    if user_id:
        user_settings = storage_service.load_user_settings().get(user_id, {})
        project_key = project_key or user_settings.get('project', config.DEFAULT_PROJECT_KEY)
        weeks = weeks or user_settings.get('weeks', config.DEFAULT_WEEKS_COUNT)
    else:
        project_key = project_key or config.DEFAULT_PROJECT_KEY
        weeks = weeks or config.DEFAULT_WEEKS_COUNT
    
    try:
        weekly_stats = analytics_service.analyze_project(project_key, weeks)
        df = analytics_service.create_hours_report(weekly_stats)
        
        # Формируем данные для графика
        chart_data = {
            "labels": [col for col in df.columns if col != 'Всего часов'],
            "datasets": []
        }
        
        # Данные по каждому пользователю (исключаем итоговую строку)
        for user in df.index[:-1]:
            chart_data["datasets"].append({
                "label": user,
                "data": [float(df.loc[user, col]) for col in df.columns if col != 'Всего часов']
            })
        
        return chart_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/no-worklog")
async def get_no_worklog(project_key: str = None, weeks: int = None, user_id: int = None):
    """API для получения задач без журнала работ"""
    # Аналогично получаем настройки
    if user_id:
        user_settings = storage_service.load_user_settings().get(user_id, {})
        project_key = project_key or user_settings.get('project', config.DEFAULT_PROJECT_KEY)
        weeks = weeks or user_settings.get('weeks', config.DEFAULT_WEEKS_COUNT)
    else:
        project_key = project_key or config.DEFAULT_PROJECT_KEY
        weeks = weeks or config.DEFAULT_WEEKS_COUNT
    
    try:
        weekly_stats = analytics_service.analyze_project(project_key, weeks)
        df = analytics_service.create_no_worklog_report(weekly_stats)
        
        # Преобразуем DataFrame в список словарей
        if df.empty:
            return {"tasks": []}
        
        return {"tasks": df.to_dict(orient='records')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/done-no-worklog")
async def get_done_no_worklog(project_key: str = None, username: str = None, user_id: int = None):
    """API для получения задач в статусе Done без журнала работ"""
    # Получаем настройки
    if user_id:
        user_settings = storage_service.load_user_settings().get(user_id, {})
        project_key = project_key or user_settings.get('project', config.DEFAULT_PROJECT_KEY)
        username = username or user_settings.get('jira_username')
    else:
        project_key = project_key or config.DEFAULT_PROJECT_KEY
    
    try:
        tasks = jira_service.get_done_tasks_without_worklog(project_key, username)
        return {"tasks": tasks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/weekly-hours")
async def get_weekly_hours(username: str, user_id: int = None):
    """API для получения часов за текущую неделю"""
    try:
        if user_id:
            user_settings = storage_service.load_user_settings().get(user_id, {})
            username = username or user_settings.get('jira_username')
        
        if not username:
            raise HTTPException(status_code=400, detail="Username is required")
        
        weekly_data = analytics_service.analyze_current_week(username)
        return weekly_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/user-info")
async def get_user_info(user_id: int):
    """API для получения информации о пользователе из настроек"""
    try:
        if not user_id:
            raise HTTPException(status_code=400, detail="User ID is required")
        
        user_settings = storage_service.load_user_settings().get(user_id, {})
        if not user_settings:
            raise HTTPException(status_code=404, detail=f"User ID {user_id} not found")
        
        # Удаляем чувствительные данные
        if 'token' in user_settings:
            user_settings = user_settings.copy()
            user_settings.pop('token')
        
        return user_settings
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def prepare_stats_for_webapp(weekly_stats):
    """Подготовка статистики в формате для веб-приложения"""
    # Создаем словарь для всех пользователей
    all_users = set()
    for week_data in weekly_stats.values():
        all_users.update(week_data['total_worked_hours'].keys())
    
    # Формируем таблицу по часам
    hours_table = []
    for user in all_users:
        user_row = {"name": user}
        total_hours = 0
        
        for week_name, week_data in weekly_stats.items():
            hours = week_data['total_worked_hours'].get(user, 0)
            total_hours += hours
            user_row[week_name] = round(hours, 2)
        
        user_row["total"] = round(total_hours, 2)
        hours_table.append(user_row)
    
    # Сортируем по общей сумме часов
    hours_table.sort(key=lambda x: x["total"], reverse=True)
    
    # Задачи без журнала работ
    no_worklog_tasks = []
    for week_data in weekly_stats.values():
        no_worklog_tasks.extend(week_data['tasks_without_worklog'])
    
    # Количество задач по неделям
    tasks_count = {week: data['total_issues'] for week, data in weekly_stats.items()}
    
    # Итоговый результат
    result = {
        "hours_table": hours_table,
        "no_worklog_tasks": no_worklog_tasks,
        "tasks_count": tasks_count,
        "weeks": list(weekly_stats.keys())
    }
    
    return result

def run_webapp():
    """Запуск веб-сервера в отдельном потоке"""
    try:
        print(f"Запуск веб-сервера на http://{config.WEBAPP_HOST}:{config.WEBAPP_PORT}")
        uvicorn.run(app, host=config.WEBAPP_HOST, port=config.WEBAPP_PORT)
    except Exception as e:
        print(f"Ошибка при запуске веб-сервера: {str(e)}")

def start_webapp_thread():
    """Запуск веб-приложения в отдельном потоке"""
    try:
        webapp_thread = Thread(target=run_webapp)
        webapp_thread.daemon = True  # Делаем поток демоном, чтобы он завершался при выходе из программы
        webapp_thread.start()
        print(f"Веб-приложение запущено на http://{config.WEBAPP_HOST}:{config.WEBAPP_PORT}")
        return webapp_thread
    except Exception as e:
        print(f"Ошибка при запуске веб-приложения: {str(e)}")
        return None
