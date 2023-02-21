import pandas as pd
import pandahouse
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import datetime
import io
import telegram
from airflow.decorators import dag, task

# Параметры daga.

default_args = {
    'owner': 'm-zhuravkin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2023, 2, 9)
}

# Интервал запуска DAG - каждые 15 мин
schedule_interval = '*/15 * * * *'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'password': 'dpo_python_2020',
              'user': 'student',
              'database': 'simulator'
            }

# Создаем телеграмм бота, объявляем id чата для отправки сообщений.

bot_token = '6023759759:AAFPDGP4njYmQig_g-DHYVXo8IeBvSwCPls'
bot = telegram.Bot(token=bot_token)
chat_id = '984280892'
report_chat_id = '-677113209'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'password': 'dpo_python_2020',
                      'user': 'student',
                      'database': 'simulator'
                     }

# Запросы на получение данных из соответствующих таблиц (feed/message)

query_feed = '''
    SELECT
      os,
      toStartOfFifteenMinutes(time) AS "fifteen_min",
      --CAST(toHour(toStartOfFifteenMinutes(time)) AS String) || ':' ||
      --CAST(toMinute(toStartOfFifteenMinutes(time)) AS String) AS "h_m",
      --toHour(toStartOfFifteenMinutes(time)) AS "hour",
      --toMinute(toStartOfFifteenMinutes(time)) AS "minute",
      COUNT(DISTINCT user_id) AS "DAU",
      COUNT(user_id) FILTER (WHERE action = 'like') AS "likes",
      COUNT(user_id) FILTER (WHERE action = 'view') AS "views",
      COUNT(user_id) FILTER (WHERE action = 'like') /
      COUNT(user_id) FILTER (WHERE action = 'view') AS "CTR"
    FROM
      simulator_20230120.feed_actions
    WHERE 
      DATE(time) BETWEEN today() - 14 AND today() 
      AND toHour(toStartOfFifteenMinutes(time)) = toHour(datesub(minute, 15, toStartOfFifteenMinutes(now())))
      AND toMinute(toStartOfFifteenMinutes(time)) = toMinute(datesub(minute, 15, toStartOfFifteenMinutes(now())))
    GROUP BY 
      os, fifteen_min
    ORDER BY 
      fifteen_min, os
    '''
query_mess = '''
    SELECT
      os,
      toStartOfFifteenMinutes(time) AS "fifteen_min",
      --CAST(toHour(toStartOfFifteenMinutes(time)) AS String) || ':' ||
      --CAST(toMinute(toStartOfFifteenMinutes(time)) AS String) AS "h_m",
      --toHour(toStartOfFifteenMinutes(time)) AS "hour",
      --toMinute(toStartOfFifteenMinutes(time)) AS "minute",
      COUNT(DISTINCT user_id) AS "DAU",
      COUNT(user_id) AS "messages"
    FROM
        simulator_20230120.message_actions
    WHERE 
        DATE(time) BETWEEN today() - 14 AND today() 
        AND toHour(toStartOfFifteenMinutes(time)) = toHour(datesub(minute, 15, toStartOfFifteenMinutes(now())))
        AND toMinute(toStartOfFifteenMinutes(time)) = toMinute(datesub(minute, 15, toStartOfFifteenMinutes(now())))
    GROUP BY 
        os, fifteen_min
    ORDER BY 
        fifteen_min, os
    '''

# Функция для проверки значения в последнем пятнадцатиминутном интервале.
# на вход датафрейм и коэф. чем больше коэф., тем шире доверительный интервал для проверки значений

def check_alert(df, coef=1): 
    
    # Функция проверяет все метрики по срезу OS.
    os_list = df.os.unique().tolist
    metrics_list = df.drop(columns=['fifteen_min', 'os']).columns.tolist
    df['date'] = df.fifteen_min.apply(lambda x: datetime.datetime.strftime(x, '%d.%m'))
    time_check = str(df.fifteen_min[0]).split(' ')[1]
    
    # цикл для перебора разных типов OS
    for os in os_list():
        df_slice = df[df.os == os]
        df_verification = df_slice[~(df_slice.fifteen_min == df_slice.fifteen_min.max())]
        
        # Цикл для перебора всех метрик
        for metric in metrics_list():
            #current_value = 500
            current_value = df_slice[metric][df_slice.fifteen_min == df_slice.fifteen_min.max()].values[0]
            
            # Доверительный интервал. Расчитывается с помощью межквартильного размаха на основе данных за аналогичные 15 мин. за предыдущие 2 недели.
            qt_range = np.percentile(df_verification[metric], 75) - np.percentile(df_verification[metric], 25)
            
            # Проверка текущего значения на попадание в доверительный интервал 
            if current_value > np.percentile(df_verification[metric], 75) + coef * qt_range\
            or current_value < np.percentile(df_verification[metric], 25) - coef * qt_range:
                
                # В случае обнаружения алерта, отправка сообщения и соответствующего графика в чат.
                text = f'⚠⚠⚠\nМетрика {metric} в срезе os - {os}.\n\
Текущее значение {current_value} не попало в доверительный интервал от \
{np.percentile(df_verification[metric], 25) - coef * qt_range} до \
{np.percentile(df_verification[metric], 75) + coef * qt_range}.\n\
Лови график значений {metric} в {time_check} за последнии 14 дней!'
                
                plt.figure(figsize=(10, 6))
                plt.title(label=f'{metric} в {time_check}')
                plt.grid()
                sns.lineplot(data=df_slice, x='date', y=metric)
                plt.xticks(rotation=20)
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plt.close()
                
                bot.send_message(chat_id=report_chat_id, text=text)
                bot.sendPhoto(chat_id=report_chat_id, photo=plot_object)         
            else:
                # Принт для проверки
                print(f'Метрика {metric} в срезе os - {os}.\nТекущее значение {current_value} попало в доверительный интервал от\
{np.percentile(df_verification[metric], 25) - coef * qt_range} до \
{np.percentile(df_verification[metric], 75) + coef * qt_range}.')

# Теперь создадим ДАГ, который будет формировать датафреймы и запускать функцию проверки значений для каждого датафрейма.
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def check_alerts_MZ():
    
    @task()
    def extract_data_feed():
        df_feed = pandahouse.read_clickhouse(query=query_feed, connection=connection)
        return df_feed
    
    @task()
    def extract_data_mess():
        df_mess = pandahouse.read_clickhouse(query=query_mess, connection=connection)
        return df_mess
    
    @task()
    def check_in_df_feed(df_feed):
        check_alert(df_feed, coef=1.5)
        
        
    @task()
    def check_in_df_mess(df_mess):
        check_alert(df_mess, coef=1.5)
        
    df_feed = extract_data_feed()
    df_mess = extract_data_mess()
    check_in_df_feed(df_feed)
    check_in_df_mess(df_mess)
    
check_alerts_MZ = check_alerts_MZ()    