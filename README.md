# portfolio
События пользователей в приложении (новостная лента и мессенджер). 
База данный в ClickHouse.
1) файл 'task_AA_test.ipynb' проверка системы сплитования пользователей, АА тест на пользователях новостной ленты.
2) файл 'task_AB.ipynb' AB тестирование алгоритма подбора новостных постов, анализ полученных результатов.
3) файл 'task_AB_(new_CTR).ipynb' AB тестирование алгоритма подбора новостных постов, в качетсве метрики - линеаризованные лайки пользователей.
4) файл 'Task_ETL.py' ежедневная выгрузка событий (новостная лента, мессенджер) за предыдущий день, преобразование данных - расчет метрик в разрезе по полу, возрасту, ОС. Запись преобразованных данных в базу данных в ClickHouse. Автоматизация с помощью Airflow.
5) файл 'Task_ETL weekly report.py' скрипт для сборки отчета по новостной ленте. Рассылка отчета ботом в телеграмм чат. Автоматизация с помощью Airflow.
6) файл 'Task_ETL_app_metrics.py' скрипт для сборки отчета по приложению. Рассылка отчета ботом в телеграмм чат. Автоматизация с помощью Airflow.
7) файл 'Task_ETL alerts.py' скрипт для проверки ключевых метрик приложения на предмет аномальных значений (проверка каждые n-минут). В случае обнаружения аномального значения, отправка соотвествующего сообщения в чат. Автоматизация с помощью Airflow.
