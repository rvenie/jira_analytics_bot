// Инициализация Telegram WebApp
const tg = window.Telegram.WebApp;
tg.expand(); // Раскрываем приложение на весь экран

// Получаем параметры из URL
const urlParams = new URLSearchParams(window.location.search);
const userId = urlParams.get('user_id');
const projectKey = urlParams.get('project');
const weeksCount = urlParams.get('weeks');

// Объекты для хранения данных
let statsData = null;
let hoursChartObj = null;
let tasksChartObj = null;

// Инициализация приложения
document.addEventListener('DOMContentLoaded', function() {
    // Загружаем данные
    loadData();
    
    // Устанавливаем обработчики для вкладок
    document.querySelectorAll('.nav-link').forEach(link => {
        link.addEventListener('click', function(e) {
            const tabId = this.getAttribute('id');
            if (tabId === 'hours-tab' && !hoursChartObj && statsData) {
                initHoursChart();
            } else if (tabId === 'tasks-tab' && !tasksChartObj && statsData) {
                initTasksChart();
            }
        });
    });
});

// Загрузка данных с сервера
async function loadData() {
    try {
        // Формируем URL запроса
        let apiUrl = '/api/stats';
        const params = [];
        
        if (userId) params.push(`user_id=${userId}`);
        if (projectKey) params.push(`project_key=${projectKey}`);
        if (weeksCount) params.push(`weeks=${weeksCount}`);
        
        if (params.length > 0) {
            apiUrl += '?' + params.join('&');
        }
        
        // Выполняем запрос
        const response = await fetch(apiUrl);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        // Получаем данные
        statsData = await response.json();
        
        // Обновляем заголовок
        const header = document.getElementById('project-header');
        header.textContent = `Статистика проекта ${projectKey || 'по умолчанию'} за ${weeksCount || '4'} недели`;
        
        // Заполняем интерфейс данными
        updateHoursTable();
        updateNoWorklogTable();
        updateTasksSummary();
        
        // Инициализируем графики
        initHoursChart();
        
    } catch (error) {
        console.error('Ошибка при загрузке данных:', error);
        document.getElementById('project-header').textContent = 'Ошибка при загрузке данных';
    }
}

// Обновление таблицы с часами
function updateHoursTable() {
    if (!statsData || !statsData.hours_table) return;
    
    const table = document.getElementById('hoursTable');
    const thead = table.querySelector('thead tr');
    const tbody = table.querySelector('tbody');
    
    // Очищаем существующие данные
    while (thead.children.length > 1) {
        thead.removeChild(thead.children[1]);
    }
    tbody.innerHTML = '';
    
    // Добавляем заголовки недель
    statsData.weeks.forEach(week => {
        const th = document.createElement('th');
        th.textContent = week.split(':')[0]; // Берем только "Неделя N"
        thead.insertBefore(th, thead.lastElementChild);
    });
    
    // Добавляем строки с данными
    statsData.hours_table.forEach(userRow => {
        const tr = document.createElement('tr');
        
        // Имя пользователя
        const tdName = document.createElement('td');
        tdName.textContent = userRow.name;
        tr.appendChild(tdName);
        
        // Часы по неделям
        statsData.weeks.forEach(week => {
            const td = document.createElement('td');
            td.textContent = userRow[week] || 0;
            tr.appendChild(td);
        });
        
        // Общая сумма часов
        const tdTotal = document.createElement('td');
        tdTotal.textContent = userRow.total;
        tdTotal.style.fontWeight = 'bold';
        tr.appendChild(tdTotal);
        
        tbody.appendChild(tr);
    });
    
    // Добавляем итоговую строку
    const totalRow = document.createElement('tr');
    totalRow.className = 'table-info';
    
    const tdTotalLabel = document.createElement('td');
    tdTotalLabel.textContent = 'Всего';
    tdTotalLabel.style.fontWeight = 'bold';
    totalRow.appendChild(tdTotalLabel);
    
    // Сумма по каждой неделе
    statsData.weeks.forEach(week => {
        const td = document.createElement('td');
        const total = statsData.hours_table.reduce((sum, user) => sum + (user[week] || 0), 0);
        td.textContent = total.toFixed(2);
        td.style.fontWeight = 'bold';
        totalRow.appendChild(td);
    });
    
    // Общая сумма всех часов
    const tdGrandTotal = document.createElement('td');
    const grandTotal = statsData.hours_table.reduce((sum, user) => sum + user.total, 0);
    tdGrandTotal.textContent = grandTotal.toFixed(2);
    tdGrandTotal.style.fontWeight = 'bold';
    totalRow.appendChild(tdGrandTotal);
    
    tbody.appendChild(totalRow);
}

// Обновление таблицы с задачами без журнала работ
function updateNoWorklogTable() {
    if (!statsData || !statsData.no_worklog_tasks) return;
    
    const table = document.getElementById('noWorklogTable');
    const tbody = table.querySelector('tbody');
    tbody.innerHTML = '';
    
    const tasks = statsData.no_worklog_tasks;
    
    if (tasks.length === 0) {
        const header = document.getElementById('no-worklog-header');
        header.textContent = 'Все задачи имеют журнал работ! 🎉';
        table.style.display = 'none';
        return;
    }
    
    tasks.forEach(task => {
        const tr = document.createElement('tr');

        // Пользователь
        const tdUser = document.createElement('td');
        tdUser.textContent = task['Пользователь'];
        tr.appendChild(tdUser);
        
        // Неделя
        const tdWeek = document.createElement('td');
        tdWeek.textContent = task['Неделя'].split(':')[0]; // Только "Неделя N"
        tr.appendChild(tdWeek);
        
        // Задача (с ссылкой на Jira)
        const tdTask = document.createElement('td');
        const taskLink = document.createElement('a');
        taskLink.href = `https://jira.datageneration.ru/browse/${task['Задача']}`;
        taskLink.textContent = task['Задача'];
        taskLink.target = '_blank';
        tdTask.appendChild(taskLink);
        tr.appendChild(tdTask);
        
        // Название задачи
        const tdTitle = document.createElement('td');
        tdTitle.textContent = task['Название'];
        tr.appendChild(tdTitle);
        
        
        // Оценка времени
        const tdEstimate = document.createElement('td');
        tdEstimate.textContent = task['Оценка времени (ч)'] || '-';
        tr.appendChild(tdEstimate);
        
        tbody.appendChild(tr);
    });
}

// Обновление сводки по задачам
function updateTasksSummary() {
    if (!statsData || !statsData.tasks_count) return;
    
    const container = document.querySelector('.tasks-summary');
    container.innerHTML = '';
    
    const header = document.createElement('h4');
    header.textContent = 'Количество задач по неделям';
    container.appendChild(header);
    
    const list = document.createElement('ul');
    list.className = 'list-group';
    
    // Добавляем пункты для каждой недели
    Object.entries(statsData.tasks_count).forEach(([week, count]) => {
        const item = document.createElement('li');
        item.className = 'list-group-item d-flex justify-content-between align-items-center';
        
        const weekName = document.createElement('span');
        weekName.textContent = week;
        
        const badge = document.createElement('span');
        badge.className = 'badge bg-primary rounded-pill';
        badge.textContent = count;
        
        item.appendChild(weekName);
        item.appendChild(badge);
        list.appendChild(item);
    });
    
    container.appendChild(list);
}

// Инициализация графика часов
function initHoursChart() {
    if (!statsData || !statsData.hours_table || statsData.hours_table.length === 0) return;
    
    const ctx = document.getElementById('hoursChart').getContext('2d');
    
    // Подготовка данных для графика
    const labels = statsData.weeks.map(week => week.split(':')[0]);
    
    // Берем топ-5 пользователей по общему количеству часов
    const topUsers = [...statsData.hours_table]
        .sort((a, b) => b.total - a.total)
        .slice(0, 5);
    
    const datasets = topUsers.map((user, index) => {
        // Генерируем цвет на основе индекса
        const hue = (index * 137) % 360; // Золотое сечение для равномерного распределения цветов
        const color = `hsl(${hue}, 70%, 60%)`;
        
        return {
            label: user.name,
            data: statsData.weeks.map(week => user[week] || 0),
            backgroundColor: color,
            borderColor: color,
            borderWidth: 2,
            tension: 0.1
        };
    });
    
    // Создаем график
    hoursChartObj = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Отработанные часы по неделям'
                },
                legend: {
                    position: 'bottom'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Часы'
                    }
                }
            }
        }
    });
}

// Инициализация графика задач
function initTasksChart() {
    if (!statsData || !statsData.tasks_count) return;
    
    const ctx = document.getElementById('tasksChart').getContext('2d');
    
    // Подготовка данных для графика
    const labels = Object.keys(statsData.tasks_count).map(week => week.split(':')[0]);
    const data = Object.values(statsData.tasks_count);
    
    // Создаем график
    tasksChartObj = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Количество задач',
                data: data,
                backgroundColor: 'rgba(54, 162, 235, 0.2)',
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 2,
                tension: 0.1,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Динамика количества задач'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Количество задач'
                    }
                }
            }
        }
    });
}
