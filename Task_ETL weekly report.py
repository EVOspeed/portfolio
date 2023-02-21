import pandas as pd
import pandahouse
import matplotlib.pyplot as plt
import seaborn as sns
import io
import telegram
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# Параметры daga

default_args = {
    'owner': 'm-zhuravkin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 3)
    }

# Интервал запуска DAG - в 11 часов дня, ежедневно
schedule_interval = '0 11 * * *'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'password': 'dpo_python_2020',
              'user': 'student',
              'database': 'simulator'
                }

# Создаем телеграмм бота, объявляем id чата для отправки сообщений.

bot_token = '6023759759:AAFPDGP4njYmQig_g-DHYVXo8IeBvSwCPls'
chat_id = '984280892'
report_chat_id = '-677113209'
bot = telegram.Bot(token=bot_token)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def get_report_MZ():
    
    @task()
    def extract_data():
        # SQL Запрос на получение необходимых данных из БД
        query = '''
            SELECT
              DATE(time) AS "reporting_date",
              COUNT(DISTINCT user_id) AS "DAU",
              COUNT(user_id) FILTER (WHERE action='view') AS "views",
              COUNT(user_id) FILTER (WHERE action='like') AS "likes",
              COUNT(user_id) FILTER (WHERE action='like') /
              COUNT(user_id) FILTER (WHERE action='view') AS "CTR"
            FROM 
              simulator_20230120.feed_actions
            WHERE 
              DATE(time) BETWEEN today() - 7 
              AND today() - 1
            GROUP BY 
              reporting_date
            ORDER BY 
              reporting_date
            '''

        df = pandahouse.read_clickhouse(query=query, connection=connection)
        return df
    
    @task()
    def send_report(df):
        message = f'Ключевые метрики за отчетный день - \
        {datetime.date(df.reporting_date.max())}:\n\
        - DAU - \
        {df.DAU[df.reporting_date == df.reporting_date.max()].values[0]} чел.;\n\
        - Количество просмотров постов - \
        {df.views[df.reporting_date == df.reporting_date.max()].values[0]} просмотра(ов);\n\
        - Количество лайков - \
        {df.likes[df.reporting_date == df.reporting_date.max()].values[0]} шт.;\n\
        - CTR из просмотров в лайки - \
        {round(df.CTR[df.reporting_date == df.reporting_date.max()].values[0], 3)}.'
        
        bot.send_message(chat_id=report_chat_id, text=message)
        
                
    @task()
    def send_plot(df):
        metrics_list = df.drop(columns='reporting_date').columns.tolist
        for i in metrics_list():
            if i == 'CTR':
                text_label = 'Значение CTR'
            else:
                text_label = 'Количество'
            plt.title(label=i, fontsize=13)
            plt.xticks(rotation=20)
            plt.grid()
            sns.lineplot(data=df, x='reporting_date', y=i)
            plt.xlabel('')
            plt.ylabel(text_label)
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plt.close()

            bot.sendPhoto(chat_id=report_chat_id, photo=plot_object)
            
    df = extract_data()
    send_report(df)
    send_plot(df)
    
get_report_MZ = get_report_MZ()