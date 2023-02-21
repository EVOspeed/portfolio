import pandas as pd
import pandahouse
import matplotlib.pyplot as plt
import seaborn as sns
import io
import telegram
import datetime
from airflow.decorators import dag, task

# Параметры daga.

default_args = {
    'owner': 'm-zhuravkin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2023, 2, 7)
}

# Интервал запуска DAG - в 11 часов дня, ежедневно
schedule_interval = '59 10 * * *'

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

query_feed = '''
        SELECT 
          DATE(time) AS "event_date",
          COUNT(DISTINCT user_id) AS "DAU",
          COUNT(user_id) FILTER (WHERE action = 'like') AS "likes",
          COUNT(user_id) FILTER (WHERE action = 'view') AS "views",
          likes / views AS "CTR",
          likes / DAU AS "likes_per_user",
          views / DAU AS "views_per_user"
        FROM 
          simulator_20230120.feed_actions
        WHERE 
          DATE(time) BETWEEN today() - 31
          AND today() - 1
        GROUP BY 
          event_date
        ORDER BY 
          event_date
    '''
query_message = '''
        SELECT 
          DATE(time) AS "event_date",
          COUNT(DISTINCT user_id) AS "DAU",
          COUNT(user_id) AS "messages",
          messages / DAU AS "messages_per_user"
        FROM
          simulator_20230120.message_actions
        WHERE 
          DATE(time) BETWEEN today() - 31
          AND today() - 1
        GROUP BY 
          event_date
        ORDER BY 
          event_date
  '''
query_rr_feed =  '''
        WITH t1 AS (SELECT 
          user_id,
          MIN(toMonday(time)) AS "date_start"
        FROM 
          simulator_20230120.feed_actions
        GROUP BY 
          user_id
        HAVING date_start >= today() - 70
        ORDER BY 
          date_start
        ),

        t2 AS (SELECT DISTINCT
          user_id,
          toMonday(time) AS "date_visit"
        FROM 
          simulator_20230120.feed_actions
        ORDER BY
          user_id, date_visit
        )

        SELECT
          COUNT(t1.user_id) AS "users",
          date_start,
          date_visit
        FROM t1
        JOIN t2 ON t2.user_id=t1.user_id
        GROUP BY 
          date_start, date_visit
        ORDER BY
          date_start, date_visit
  '''
query_rr_mess = '''
        WITH t1 AS (SELECT 
          user_id,
          MIN(toMonday(time)) AS "date_start"
        FROM 
          simulator_20230120.message_actions
        GROUP BY 
          user_id
        ORDER BY 
          date_start
        ),

        t2 AS (SELECT DISTINCT
          user_id,
          toMonday(time) AS "date_visit"
        FROM 
          simulator_20230120.message_actions
        ORDER BY
          user_id, date_visit
        )

        SELECT
          COUNT(t1.user_id) AS "users",
          date_start,
          date_visit
        FROM t1
        JOIN t2 ON t2.user_id=t1.user_id
        GROUP BY 
          date_start, date_visit
        ORDER BY
          date_start, date_visit
  '''
query_decomposition = '''
        SELECT 
          this_week,
          previous_week,
          -uniq(user_id) as num_users, status FROM

        (SELECT user_id, 
        groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
        addWeeks(arrayJoin(weeks_visited), +1) this_week, 
        if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status, 
        addWeeks(this_week, -1) as previous_week
        FROM simulator_20230120.feed_actions
        group by user_id)

        where status = 'gone'

        group by this_week, previous_week, status

        HAVING this_week != addWeeks(toMonday(today()), +1)

        union all


        SELECT this_week, previous_week, toInt64(uniq(user_id)) as num_users, status FROM

        (SELECT user_id, 
        groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
        arrayJoin(weeks_visited) this_week, 
        if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status, 
        addWeeks(this_week, -1) as previous_week
        FROM simulator_20230120.feed_actions
        group by user_id)

        group by this_week, previous_week, status
    '''
query_users = '''
        SELECT DISTINCT
          user_id,
          age,
          gender,
          os,
          source
        FROM 
          simulator_20230120.feed_actions
  '''

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def report_app_MZ():
    
    @task()
    def extract_data_feed():
        df_feed = pandahouse.read_clickhouse(query=query_feed, connection=connection)
        return df_feed
    
    @task()
    def extract_data_mess():
        df_message = pandahouse.read_clickhouse(query=query_message, connection=connection)
        return df_message
    
    @task()
    def extract_data_rr_feed():
        df_rr_feed = pandahouse.read_clickhouse(query=query_rr_feed, connection=connection)
        return df_rr_feed
    
    @task()
    def extract_data_rr_mess():
        df_rr_mess = pandahouse.read_clickhouse(query=query_rr_mess, connection=connection)
        return df_rr_mess
    
    @task()
    def extract_data_decomposition():
        df_decomposition = pandahouse.read_clickhouse(query=query_decomposition, connection=connection)
        return df_decomposition
    
    @task()
    def extract_data_users():
        df_users = pandahouse.read_clickhouse(query=query_users, connection=connection)
        return df_users
    
    @task()
    def send_app_metrics(df_feed, df_message):
        metrics_list = df_feed.drop(columns='event_date').columns.tolist
        df_feed['date'] = df_feed.event_date.apply(lambda x: datetime.datetime.strftime(x, '%d-%m'))
        plot_index = 1
        plot_object = io.BytesIO()
        plt.figure(figsize=(12, 14))
        for i in metrics_list():
            plt.subplot(3, 2, plot_index)
            plt.title(label=i, fontsize=10)
            plt.xticks(ticks=(range(0, len(df_feed), 3)), rotation=20)
            plt.grid()
            sns.lineplot(data=df_feed, x='date', y=i, legend='full')
            plt.xlabel('')
            plt.ylabel('')
            plot_index += 1

        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.close()

        metrics_list1 = df_message.drop(columns='event_date').columns.tolist
        df_message['date'] = df_message.event_date.apply(lambda x: datetime.datetime.strftime(x, '%d-%m'))
        plot_index1 = 1
        plot_object1 = io.BytesIO()
        plt.figure(figsize=(12, 14))
        for i in metrics_list1():
            plt.subplot(2, 2, plot_index1)
            plt.title(label=i, fontsize=10)
            plt.xticks(ticks=(range(0, len(df_message), 3)), rotation=20)
            plt.grid()
            sns.lineplot(data=df_message, x='date', y=i, legend='full')
            plt.xlabel('')
            plt.ylabel('')
            plot_index1 += 1

    
        plt.savefig(plot_object1)
        plot_object1.seek(0)
        plt.close()
        
        text = 'Изменения основных показателей новостной ленты и сообщений за последнии 31 календарных дней представлены на графиках.'
        bot.send_message(chat_id=report_chat_id, text=text)
        bot.sendPhoto(chat_id=report_chat_id, photo=plot_object)
        bot.sendPhoto(chat_id=report_chat_id, photo=plot_object1)
    
    @task()
    def send_RR(df_rr_feed, df_rr_mess):
        text = 'Показатели возвращаемости пользователей представлены на сл. тепловых картах'
        bot.send_message(chat_id=report_chat_id, text=text)
        title_num = 1
        for df in [df_rr_feed, df_rr_mess]:
            df = df.astype({'date_start':'string', 'date_visit':'string'})
            df = df.pivot_table(index=['date_start', 'date_visit'], values='users')
            def cohort(row):
                row['day'] = range(len(row))
                return row
            cohorts = df.groupby(level=0).apply(cohort)
            cohort_size = cohorts.users.groupby(level=0).first()
            retention = cohorts.users.unstack(0).divide(cohort_size, axis=1)
            plt.figure(figsize=(15, 10))
            if title_num == 1:
                plt.title(label='RR per weekly cohort (feed)')
                title_num += 1
            else:
                plt.title(label='RR per weekly cohort (message)')
            sns.heatmap(retention.T, annot=True, linewidths=0.1, linecolor='black', cbar=True, cmap='spring')
            plot_object = io.BytesIO()    
            plt.savefig(plot_object)
            plot_object.seek(0)
            plt.close()
            bot.sendPhoto(chat_id=report_chat_id, photo=plot_object)
            
    @task()
    def send_decomposition(df_decomposition):
        text = 'Декомпозиция недельных когорт пользоваетелей на графике.'
        bot.send_message(chat_id=report_chat_id, text=text)
        df_decomposition.sort_values(by='this_week', inplace=True)
        df_decomposition = df_decomposition.astype({'this_week':'string'})
        plt.figure(figsize=(15, 10))
        plt.title(label='Декомпозиция пользователей')
        sns.barplot(x=df_decomposition.this_week, y=df_decomposition.num_users, hue=df_decomposition.status)
        plt.grid()
        plot_object = io.BytesIO()    
        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=report_chat_id, photo=plot_object)
        
    @task()
    def send_slice_users(df_users):
        df_users['age_group'] = pd.cut(df_users.age, bins=[df_users.age.min() - 1, 18, 25, 35, 50, df_users.age.max()])
        df_users = df_users.astype({'age_group':'string'})
        df_users.gender = df_users.gender.apply(lambda x: 'female' if x == 0 else 'male')
        df_users.age_group = df_users.age_group.apply(lambda x: str.split(x, '(')[1].split(sep=']')[0].replace(', ', '-'))
        
        plot_object = io.BytesIO()
        plt.figure(figsize=(18, 8))
        plt.subplot(1, 2, 1)
        plt.title(label='slice gender-age', fontsize=10)
        sns.countplot(x=df_users.age_group.sort_values(), hue=df_users.gender)
        plt.subplot(1, 2, 2)
        plt.title(label='slice os-source', fontsize=10)
        sns.countplot(x=df_users.os, hue=df_users.source) 
        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.close()
        
        df1 = df_users.groupby(by='gender').count()[['user_id']].reset_index().rename(columns={'gender':'slice'})
        df2 = df_users.groupby(by='os').count()[['user_id']].reset_index().rename(columns={'os':'slice'})
        df3 = df_users.groupby(by='age_group').count()[['user_id']].reset_index().rename(columns={'age_group':'slice'})
        df_to_send = pd.concat([df1, df2, df3])
        file_object = io.StringIO()
        df_to_send.to_csv(file_object)
        file_object.name = 'slice_users.csv'
        file_object.seek(0)
                        
        text = 'Срезы пользователей представлены на графиках, более подробно с количеством пользователей в том или ином срезе можно ознакомиться в приложенном файле.'
        bot.send_message(chat_id=report_chat_id, text=text)
        bot.sendPhoto(chat_id=report_chat_id, photo=plot_object)
        bot.sendDocument(chat_id=report_chat_id, document=file_object)
        
        
    df_feed = extract_data_feed()
    df_message = extract_data_mess()
    df_rr_feed = extract_data_rr_feed()
    df_rr_mess = extract_data_rr_mess()
    df_decomposition = extract_data_decomposition()
    df_users = extract_data_users()
    send_app_metrics(df_feed, df_message)
    send_RR(df_rr_feed, df_rr_mess)
    send_decomposition(df_decomposition)
    send_slice_users(df_users)
    
report_app_MZ = report_app_MZ()
