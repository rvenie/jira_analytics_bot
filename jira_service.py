from jira import JIRA
from datetime import datetime, timedelta
import config
import logging
from collections import defaultdict

class JiraService:
    """Сервис для взаимодействия с Jira API с полным сбором данных о задачах"""
    
    def __init__(self, url=None, token=None):
        """Инициализация сервиса Jira"""
        self.url = url or config.JIRA_URL
        self.token = token or config.JIRA_TOKEN
        
        # Настройка логирования
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('jira_service')
        
        try:
            # Явно указываем все параметры для соединения
            options = {'server': self.url}
            self.jira = JIRA(options=options, token_auth=self.token)
            
            # Проверка подключения на простом запросе
            self.jira.projects()
            self.logger.info(f"✅ Успешно подключились к серверу Jira: {self.url}")
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения к Jira: {str(e)}")
            self.jira = None
    
    def get_current_user(self):
        """Получение информации о текущем пользователе"""
        try:
            user_data = self.jira.myself()
            return user_data
        except Exception as e:
            self.logger.error(f"Ошибка при получении данных пользователя: {str(e)}")
            raise
    
    def get_user_groups(self):
        """Получение групп текущего пользователя"""
        try:
            user_data = self.get_current_user()
            username = user_data.get('name', '')
            
            try:
                # Для Jira Server
                groups = self.jira.user(username).get('groups', {}).get('items', [])
                return [group['name'] for group in groups]
            except:
                # Альтернативные методы
                try:
                    groups = self.jira.user_groups(username)
                    return groups
                except:
                    return []
        except Exception as e:
            self.logger.error(f"Ошибка при получении групп пользователя: {str(e)}")
            return []
    
    def calculate_working_weeks(self, current_date=None, num_weeks=4):
        """Расчет дат начала и конца для последних n рабочих недель"""
        current_date = current_date or datetime.now()
        weeks = []
        
        for i in range(num_weeks):
            start_of_week = current_date - timedelta(days=current_date.weekday() + (i * 7))
            start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_week = start_of_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
            weeks.append((start_of_week, end_of_week))
            
        return weeks
    
    def collect_project_data(self, project_key, weeks_count=4, username=None):
        """Комплексный сбор данных по проекту за указанный период"""
        try:
            # Получаем периоды для анализа
            weeks = self.calculate_working_weeks(num_weeks=weeks_count)
            project_data = {
                'weeks_info': {},
                'all_issues': {},
                'all_worklogs': defaultdict(list),
                'users_hours': defaultdict(float),
                'tasks_without_worklog': []
            }
            
            # Собираем информацию по неделям
            for week_idx, (date_start, date_end) in enumerate(weeks):
                week_name = f"Неделя {week_idx+1}: {date_start.strftime('%Y-%m-%d')} - {date_end.strftime('%Y-%m-%d')}"
                
                # Сохраняем информацию о неделе
                project_data['weeks_info'][week_name] = {
                    'start_date': date_start,
                    'end_date': date_end,
                    'issues_count': 0
                }
                
                # Получаем задачи за период (всех или конкретного пользователя)
                if username:
                    issues = self.get_issues_for_user_week(project_key, date_start, date_end, username)
                else:
                    issues = self.get_issues_for_week(project_key, date_start, date_end)
                
                project_data['weeks_info'][week_name]['issues_count'] = len(issues)
                
                # Обрабатываем каждую задачу
                for issue in issues:
                    # Если задача уже была обработана, пропускаем
                    if issue.key in project_data['all_issues']:
                        continue
                    
                    # Получаем полную информацию о задаче
                    issue_data = self._extract_issue_data(issue)
                    project_data['all_issues'][issue.key] = issue_data
                    
                    # Получаем журналы работ
                    worklogs = self.get_worklogs(issue.key)
                    
                    if not worklogs:
                        # Добавляем задачу в список без журнала работ
                        project_data['tasks_without_worklog'].append({
                            'week': week_name,
                            'key': issue.key,
                            'summary': issue.fields.summary,
                            'assignee': issue_data['assignee'],
                            'status': issue_data['status'],
                            'estimated_hours': issue_data['original_estimate_hours']
                        })
                    else:
                        # Обрабатываем журналы работ
                        for worklog in worklogs:
                            worklog_data = self._extract_worklog_data(worklog, issue.key)
                            worklog_date = datetime.strptime(worklog.started[:10], '%Y-%m-%d').date()
                            
                            # Добавляем журнал в общий список
                            project_data['all_worklogs'][issue.key].append(worklog_data)
                            
                            # Если журнал попадает в анализируемый период, учитываем часы
                            if date_start.date() <= worklog_date <= date_end.date():
                                project_data['users_hours'][worklog_data['author']] += worklog_data['hours']
            
            return project_data
            
        except Exception as e:
            self.logger.error(f"Ошибка при сборе данных проекта: {str(e)}")
            raise
    
    def _extract_issue_data(self, issue):
        """Извлечение полных данных о задаче"""
        issue_data = {
            'key': issue.key,
            'summary': issue.fields.summary,
            'status': issue.fields.status.name,
            'created': issue.fields.created,
            'updated': issue.fields.updated,
            'assignee': issue.fields.assignee.displayName if issue.fields.assignee else "Не назначен",
            'reporter': issue.fields.reporter.displayName if issue.fields.reporter else "Не указан",
            'original_estimate_seconds': getattr(issue.fields, 'timeoriginalestimate', 0) or 0,
            'original_estimate_hours': (getattr(issue.fields, 'timeoriginalestimate', 0) or 0) / 3600,
            'priority': issue.fields.priority.name if hasattr(issue.fields, 'priority') and issue.fields.priority else "Не указан",
            'issue_type': issue.fields.issuetype.name,
            'components': [c.name for c in issue.fields.components] if hasattr(issue.fields, 'components') else [],
            'labels': issue.fields.labels if hasattr(issue.fields, 'labels') else []
        }
        return issue_data
    
    def _extract_worklog_data(self, worklog, issue_key):
        """Извлечение данных из журнала работ"""
        return {
            'id': worklog.id,
            'issue_key': issue_key,
            'author': worklog.author.displayName if worklog.author else "Неизвестный автор",
            'author_name': worklog.author.name if worklog.author else None,
            'date': worklog.started[:10],
            'created': worklog.created,
            'updated': worklog.updated,
            'seconds': worklog.timeSpentSeconds,
            'hours': worklog.timeSpentSeconds / 3600,
            'comment': worklog.comment
        }
    
    def get_issues_for_week(self, project_key, date_start, date_end):
        """Получение задач, перешедших в 'Тестирование' или 'Done' за период"""
        date_start_str = date_start.strftime("%Y-%m-%d")
        date_end_str = date_end.strftime("%Y-%m-%d")
        
        jql_query = (
            f"project = {project_key} AND status IN ('Тестирование', 'Done') "
            f"AND (status CHANGED TO 'Тестирование' DURING ('{date_start_str}', '{date_end_str}') "
            f"OR status CHANGED TO 'Done' DURING ('{date_start_str}', '{date_end_str}')) "
            f"ORDER BY updated DESC"
        )
        
        return self.jira.search_issues(jql_query, maxResults=0)
    
    def get_issues_for_user_week(self, project_key, date_start, date_end, username):
        """Получение задач пользователя за указанный период"""
        date_start_str = date_start.strftime("%Y-%m-%d")
        date_end_str = date_end.strftime("%Y-%m-%d")
        
        jql_query = (
            f"project = {project_key} AND assignee = '{username}' "
            f"AND status IN ('Тестирование', 'Done') "
            f"AND (status CHANGED TO 'Тестирование' DURING ('{date_start_str}', '{date_end_str}') "
            f"OR status CHANGED TO 'Done' DURING ('{date_start_str}', '{date_end_str}')) "
            f"ORDER BY updated DESC"
        )
        
        return self.jira.search_issues(jql_query, maxResults=0)
    
    def get_current_week_worklogs(self, username):
        """Получение журналов работ пользователя за текущую неделю"""
        current_date = datetime.now()
        start_of_week = current_date - timedelta(days=current_date.weekday())
        start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_week = start_of_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
        
        date_start_str = start_of_week.strftime("%Y-%m-%d")
        date_end_str = end_of_week.strftime("%Y-%m-%d")
        
        jql_query = (
            f"worklogAuthor = '{username}' AND worklogDate >= '{date_start_str}' AND "
            f"worklogDate <= '{date_end_str}' ORDER BY updated DESC"
        )
        
        issues = self.jira.search_issues(jql_query, maxResults=0)
        
        total_hours = 0
        worklogs_data = []
        
        for issue in issues:
            worklogs = self.jira.worklogs(issue.key)
            
            for worklog in worklogs:
                worklog_date = datetime.strptime(worklog.started[:10], '%Y-%m-%d').date()
                
                if start_of_week.date() <= worklog_date <= end_of_week.date() and \
                   worklog.author.name == username:
                    total_hours += worklog.timeSpentSeconds / 3600
                    worklogs_data.append({
                        'issue_key': issue.key,
                        'issue_summary': issue.fields.summary,
                        'date': worklog_date.strftime('%Y-%m-%d'),
                        'time_spent_hours': worklog.timeSpentSeconds / 3600,
                        'comment': worklog.comment
                    })
        
        return {
            'total_hours': total_hours,
            'worklogs': worklogs_data,
            'start_date': date_start_str,
            'end_date': date_end_str
        }
    
    def get_done_tasks_without_worklog(self, project_key, username=None):
        """Получение задач в статусе Done без журнала работ"""
        if username:
            jql_query = (
                f"project = {project_key} AND assignee = '{username}' "
                f"AND status IN ('Done') AND worklogAuthor is EMPTY"
            )
        else:
            jql_query = (
                f"project = {project_key} AND status IN ('Done') AND worklogAuthor is EMPTY"
            )
        
        issues = self.jira.search_issues(jql_query, maxResults=0)
        
        tasks_without_worklog = []
        
        for issue in issues:
            worklogs = self.jira.worklogs(issue=issue.key)
            
            if not worklogs:
                tasks_without_worklog.append({
                    'key': issue.key,
                    'summary': issue.fields.summary,
                    'assignee': issue.fields.assignee.displayName if issue.fields.assignee else "Не назначен",
                    'status': issue.fields.status.name,
                    'created': issue.fields.created[:10],
                    'updated': issue.fields.updated[:10]
                })
        
        return tasks_without_worklog
    
    def get_worklogs(self, issue_key):
        """Получение журнала работ для задачи"""
        return self.jira.worklogs(issue=issue_key)
