import pandas as pd
from datetime import datetime, timedelta
import logging
from collections import defaultdict
pd.set_option('future.no_silent_downcasting', True)

class AnalyticsService:
    """Сервис для анализа данных из Jira с полным сохранением информации о задачах"""
    
    def __init__(self, jira_service):
        """Инициализация сервиса аналитики"""
        self.jira_service = jira_service
        
        # Настройка логирования
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('analytics_service')
    
    def analyze_project(self, project_key, weeks_count=4):
        """Анализ проекта за указанное количество недель с сохранением полных данных"""
        try:
            # Получаем полные данные из Jira
            project_data = self.jira_service.collect_project_data(project_key, weeks_count)
            
            # Формируем недельную статистику на основе полных данных
            weekly_stats = self._create_weekly_stats(project_data)
            
            # Добавляем полные данные для будущего использования
            weekly_stats['_full_data'] = project_data
            
            return weekly_stats
        except Exception as e:
            self.logger.error(f"Ошибка при анализе проекта: {str(e)}")
            raise
    
    def analyze_user_project(self, project_key, weeks_count=4, username=None):
        """Анализ проекта для конкретного пользователя за указанное количество недель"""
        if not username:
            return self.analyze_project(project_key, weeks_count)
        
        try:
            # Получаем полные данные из Jira с фильтрацией по пользователю
            project_data = self.jira_service.collect_project_data(project_key, weeks_count, username)
            
            # Формируем недельную статистику
            weekly_stats = self._create_weekly_stats(project_data)
            
            # Добавляем полные данные
            weekly_stats['_full_data'] = project_data
            
            return weekly_stats
        except Exception as e:
            self.logger.error(f"Ошибка при анализе проекта пользователя: {str(e)}")
            raise
    
    def _create_weekly_stats(self, project_data):
        """Создание статистики по неделям из полных данных проекта"""
        weekly_stats = {}
        
        # Для каждой недели создаем статистику
        for week_name, week_info in project_data['weeks_info'].items():
            # Фильтруем журналы работ за эту неделю
            week_worklogs = {}
            week_users_hours = defaultdict(float)
            
            # Обрабатываем журналы работ
            for issue_key, worklogs in project_data['all_worklogs'].items():
                for worklog in worklogs:
                    worklog_date = datetime.strptime(worklog['date'], '%Y-%m-%d').date()
                    if week_info['start_date'].date() <= worklog_date <= week_info['end_date'].date():
                        if issue_key not in week_worklogs:
                            week_worklogs[issue_key] = []
                        week_worklogs[issue_key].append(worklog)
                        week_users_hours[worklog['author']] += worklog['hours']
            
            # Фильтруем задачи без журнала работ за эту неделю
            tasks_without_worklog = [
                task for task in project_data['tasks_without_worklog']
                if task['week'] == week_name
            ]
            
            # Часы из оценок времени для задач без журнала
            estimated_hours = defaultdict(float)
            for task in tasks_without_worklog:
                if task['estimated_hours']:
                    estimated_hours[task['assignee']] += task['estimated_hours']
            
            # Объединяем часы из журнала работ и оценки
            total_worked_hours = week_users_hours.copy()
            for user, hours in estimated_hours.items():
                total_worked_hours[user] = total_worked_hours.get(user, 0) + hours
            
            # Формируем статистику недели
            weekly_stats[week_name] = {
                'worked_hours': dict(week_users_hours), # Только журнаял работ
                'estimated_as_worked_hours': dict(estimated_hours), # Только оценка
                'total_worked_hours': dict(total_worked_hours), # Журнал работ + оценка
                'tasks_without_worklog': tasks_without_worklog, # Задачи без журнала работ
                'total_issues': week_info['issues_count'],
                'worklogs': week_worklogs,
                'start_date': week_info['start_date'],
                'end_date': week_info['end_date']
            }
        
        return weekly_stats
    
    def analyze_current_week(self, username):
        """Анализ работы пользователя за текущую неделю"""
        try:
            # Получаем данные о журналах работ за текущую неделю
            worklog_data = self.jira_service.get_current_week_worklogs(username)
            
            # Группируем работу по дням недели
            daily_hours = {}
            for worklog in worklog_data['worklogs']:
                date = worklog['date']
                hours = worklog['time_spent_hours']
                daily_hours[date] = daily_hours.get(date, 0) + hours
            
            # Создаем статистику по дням недели
            days_stats = []
            current_date = datetime.now()
            start_of_week = current_date - timedelta(days=current_date.weekday())
            
            for i in range(7):
                day_date = (start_of_week + timedelta(days=i)).strftime('%Y-%m-%d')
                day_name = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье'][i]
                hours = daily_hours.get(day_date, 0)
                
                days_stats.append({
                    'day': day_name,
                    'date': day_date,
                    'hours': round(hours, 2)
                })
            
            # Результат анализа
            return {
                'username': username,
                'total_hours': round(worklog_data['total_hours'], 2),
                'start_date': worklog_data['start_date'],
                'end_date': worklog_data['end_date'],
                'daily_stats': days_stats,
                'tasks_count': len(set([w['issue_key'] for w in worklog_data['worklogs']])),
                'worklogs': worklog_data['worklogs']  # Сохраняем полные данные о журналах
            }
        except Exception as e:
            self.logger.error(f"Ошибка при анализе текущей недели: {str(e)}")
            return {
                'username': username,
                'total_hours': 0,
                'start_date': None,
                'end_date': None,
                'daily_stats': [],
                'tasks_count': 0,
                'error': str(e)
            }
    
    def find_done_tasks_without_worklog(self, project_key, username=None):
        """Поиск задач в статусе Done без журнала работ"""
        return self.jira_service.get_done_tasks_without_worklog(project_key, username)
    
    def create_hours_report(self, weekly_stats):
        """Создание отчета по часам"""
        # Собираем всех пользователей
        all_users = set()
        for week_name, week_data in weekly_stats.items():
            if week_name != '_full_data':  # Пропускаем полные данные
                all_users.update(week_data['worked_hours'].keys())
        
        # Создаем DataFrame
        df = pd.DataFrame(index=list(all_users))
        
        # Заполняем данными по неделям
        for week_name, week_data in weekly_stats.items():
            if week_name != '_full_data':  # Пропускаем полные данные
                df[f"{week_name} (ч)"] = pd.Series(week_data['worked_hours'])
        
        # Заполняем пропуски явно указывая тип данных (float)
        df = df.fillna(0.0) # Избегаем предупреждения FutureWarning
        
        df['Всего часов'] = df.sum(axis=1)
        df = df.sort_values('Всего часов', ascending=False)
        df = df.round(2)
        df.loc['Всего'] = df.sum()
        
        return df
    
    def create_no_worklog_report(self, weekly_stats):
        """Создание отчета по задачам без журнала работ"""
        all_tasks = []
        for week_name, week_data in weekly_stats.items():
            if week_name != '_full_data':  # Пропускаем полные данные
                all_tasks.extend(week_data['tasks_without_worklog'])
        
        return pd.DataFrame(all_tasks) if all_tasks else pd.DataFrame()
    
    def create_tasks_count_report(self, weekly_stats):
        """Создание отчета по количеству задач"""
        return {week: data['total_issues'] for week, data in weekly_stats.items() if week != '_full_data'}
    
    def get_user_tasks_details(self, weekly_stats, user):
        """Получение детальной информации о задачах пользователя с учетом периода"""
        if '_full_data' in weekly_stats:
            user_tasks = []
            project_data = weekly_stats['_full_data']
            
            # Получаем периоды анализа
            periods = []
            for week_name, week_info in project_data.get('weeks_info', {}).items():
                if 'start_date' in week_info and 'end_date' in week_info:
                    periods.append((week_info['start_date'], week_info['end_date']))
            
            # Обрабатываем задачи с журналами работ
            for issue_key, worklogs in project_data['all_worklogs'].items():
                user_worklogs = []
                
                # Фильтруем журналы по автору и периоду
                for worklog in worklogs:
                    if worklog['author'] == user:
                        worklog_date = datetime.strptime(worklog['date'], '%Y-%m-%d').date()
                        
                        # Проверяем, попадает ли дата в нужный период
                        in_period = False
                        for start_date, end_date in periods:
                            if start_date.date() <= worklog_date <= end_date.date():
                                in_period = True
                                break
                        
                        if in_period:
                            user_worklogs.append(worklog)
                
                if user_worklogs:
                    # Собираем данные о задаче
                    if issue_key in project_data['all_issues']:
                        issue_data = project_data['all_issues'][issue_key]
                        total_hours = sum(w['hours'] for w in user_worklogs)
                        
                        task = {
                            'key': issue_key,
                            'summary': issue_data['summary'],
                            'status': issue_data['status'],
                            'hours': total_hours,
                            'worklogs': user_worklogs
                        }
                        user_tasks.append(task)
            
            # Добавляем задачи без журнала работ
            for task in project_data['tasks_without_worklog']:
                if task['assignee'] == user and not any(t['key'] == task['key'] for t in user_tasks):
                    user_tasks.append({
                        'key': task['key'],
                        'summary': task['summary'],
                        'status': 'Без журнала работ',
                        'hours': task['estimated_hours'] or 0,
                        'estimated': True
                    })
            
            return sorted(user_tasks, key=lambda x: x.get('hours', 0), reverse=True)
        
        # Если полные данные недоступны, используем упрощенный подход
        user_tasks = []
        for week_name, week_data in weekly_stats.items():
            if week_name == '_full_data':
                continue
                
            # Ищем задачи без журнала работ для пользователя
            for task in week_data['tasks_without_worklog']:
                if task['Пользователь'] == user:
                    user_tasks.append({
                        'key': task['Задача'],
                        'summary': task['Название'],
                        'hours': task['Оценка времени (ч)'] or 0,
                        'estimated': True,
                        'week': week_name
                    })
        
        return sorted(user_tasks, key=lambda x: x.get('hours', 0), reverse=True)
