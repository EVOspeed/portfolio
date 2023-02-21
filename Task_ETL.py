import pandas as pd
import pandahouse
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# Параметры

default_args = {
    'owner': 'm-zhuravkin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 2)
}

# Интервал запуска DAG - в 12 часов дня, ежедневно
schedule_interval = '0 12 * * *'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
              'password': 'dpo_python_2020',
              'user': 'student',
              'database': 'simulator'
                }

connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                   'password': '656e2b0c9c',
                   'user': 'student-rw',
                   'database': 'test'
                    }
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def get_count_action_MZ():
    
    @task()
    def extract_feed():
       
        # SQL Запросы на получение необходимых данных из БД

        query_feed = '''
                SELECT
                  user_id,
                  COUNT(action) FILTER (WHERE action = 'like') AS "likes",
                  COUNT(action) FILTER (WHERE action = 'view') AS "views"
                FROM 
                  simulator_20230120.feed_actions
                WHERE DATE(time) = today() - 1
                GROUP BY
                  user_id
            '''
        df_feed = pandahouse.read_clickhouse(query=query_feed, connection=connection)
        return df_feed
    
    @task()
    def extract_message():
        
        # SQL Запросы на получение необходимых данных из БД
        
        query_message = '''
                WITH t1 AS (
                SELECT 
                  user_id,
                  COUNT(DISTINCT reciever_id) AS "users_sent",
                  COUNT(reciever_id) AS "messages_sent"
                FROM 
                  simulator_20230120.message_actions
                WHERE DATE(time) = today() - 1
                GROUP BY
                  user_id
                ),

                t2 AS (
                SELECT 
                  reciever_id,
                  COUNT(DISTINCT user_id) AS "users_received",
                  COUNT(user_id) AS "messages_received"
                FROM 
                  simulator_20230120.message_actions
                WHERE 
                  DATE(time) = today() - 1
                GROUP BY 
                  reciever_id
                ),

                t3 AS (SELECT 
                  user_id,
                  reciever_id,
                  users_sent,
                  messages_sent,
                  users_received,
                  messages_received
                FROM t1
                FULL OUTER JOIN t2 
                ON t2.reciever_id=t1.user_id
                ORDER BY user_id
                )
                SELECT
                  CASE 
                    WHEN user_id = 0 THEN reciever_id
                    ELSE user_id
                  END AS "user_id",
                  users_sent,
                  messages_sent,
                  users_received,
                  messages_received
                FROM t3
                ORDER BY 
                   user_id
            '''
        df_message = pandahouse.read_clickhouse(query=query_message, connection=connection)
        return df_message
    
    @task()
    def extract_user_info():
        
        # SQL Запросы на получение необходимых данных из БД
        
        query_info = '''
                SELECT DISTINCT
                  user_id,
                  gender,
                  age,
                  os
                FROM 
                  simulator_20230120.feed_actions
                UNION ALL
                SELECT DISTINCT
                  user_id,
                  gender,
                  age,
                  os
                FROM 
                  simulator_20230120.message_actions
                WHERE user_id NOT IN (
                        SELECT DISTINCT user_id FROM simulator_20230120.feed_actions)
            '''
        
        df_info = pandahouse.read_clickhouse(query=query_info, connection=connection)
        return df_info
        
    
    @task()
    def transform_df(df_feed, df_message, df_info):
        
        # Объединение полуенных данных в один датафрейм
        
        df = pd.merge(left=df_feed, right=df_message, how='outer', left_on='user_id', right_on='user_id')
        df = df.merge(right=df_info, how='left', on='user_id')
        df.fillna(0, inplace=True)
        df['event_date'] = datetime.date(datetime.today()) - timedelta(days=1)
        return df
    
    @task()
    def transform_df_gender(df):
        
        # Получение необходимых метрик в рамках среза по полу пользователя
        
        df_gender = df.pivot_table(values=['views', 'likes', 'messages_received', 'messages_sent',\
                                           'users_received', 'users_sent'],\
                                   index=['event_date', 'gender'],\
                                   aggfunc='sum').reset_index()
        df_gender['dimension'] = 'gender'
        df_gender.rename(columns={'gender':'dimension_value'}, inplace=True)
        df_gender = df_gender[['event_date', 'dimension', 'dimension_value', 'views',\
                               'likes', 'messages_received', 'messages_sent', 'users_received',\
                               'users_sent']]
        return df_gender
        
    @task()
    def transform_df_age(df):
        
        # Получение необходимых метрик в рамках среза по возрасту пользователя
        
        df_age = df.pivot_table(values=['views', 'likes', 'messages_received', 'messages_sent',\
                                        'users_received', 'users_sent'],\
                                index=['event_date', 'age'],\
                                aggfunc='sum').reset_index()
        df_age['dimension'] = 'age'
        df_age.rename(columns={'age':'dimension_value'}, inplace=True)
        df_age = df_age[['event_date', 'dimension', 'dimension_value', 'views',\
                         'likes', 'messages_received', 'messages_sent', 'users_received',\
                         'users_sent']]
        return df_age
    
    @task()
    def transform_df_os(df):
        
        # Получение необходимых метрик в рамках среза по ОС пользователя
        
        df_os = df.pivot_table(values=['views', 'likes', 'messages_received', 'messages_sent',\
                                        'users_received', 'users_sent'],\
                                index=['event_date', 'os'],\
                                aggfunc='sum').reset_index()
        df_os['dimension'] = 'os'
        df_os.rename(columns={'os':'dimension_value'}, inplace=True)
        df_os = df_os[['event_date', 'dimension', 'dimension_value', 'views',\
                       'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        return df_os
        
    @task()
    def transform_final_df(df_gender, df_age, df_os):
        
        # Конкатенация преобразованных данных в один датафрейм и подготовка датафрейма к записи в БД
        
        df_final = pd.concat([df_age, df_gender, df_os], axis=0).reset_index(drop=True)
        
        # В первых версиях преобразовывал числовые столбцы в датафрейме в UInt32 в этом же формате создавал соответствующие столбцы в таблице (CH), но при записи данных постоянно натыкался на ошибку:
        # raise ValueError('Unknown type mapping in dtypes: {}'.format(dtypes)) ValueError: Unknown type mapping in dtypes: {'event_date': 'String', 'dimension': 'String', 'dimension_value': 'String',
        # 'views': None, 'likes': None, 'messages_received': None, 'messages_sent': None, 'users_received': None, 'users_sent': None} 
        # После многих попыток записал все в Float64
        
        #df_final = df_final.astype({'views':'UInt32', 'likes':'UInt32', 'messages_received':'UInt32',\
        #                            'messages_sent':'UInt32', 'users_received':'UInt32', 'users_sent':'UInt32'})
        return df_final
    
    @task()
    def load(df_final):
                        
        # Создание таблицы (при условии ее отсутствия) и добавления в нее подготовленных данных
        
        query_new_table = '''
            CREATE TABLE IF NOT EXISTS test.Zhuravkin_task_6_0 
            (
                event_date String,
                dimension String,
                dimension_value String,
                views Float64,
                likes Float64,
                messages_received Float64,
                messages_sent Float64,
                users_received Float64,
                users_sent Float64
            ) ENGINE = MergeTree()
            ORDER BY event_date
            '''
        pandahouse.execute(query=query_new_table, connection=connection_test)
        pandahouse.to_clickhouse(df=df_final, table='Zhuravkin_task_6_0', index=False, connection=connection_test)
    
    df_feed = extract_feed()
    df_message = extract_message()
    df_info = extract_user_info()
    df = transform_df(df_feed, df_message, df_info)
    df_gender = transform_df_gender(df)
    df_age = transform_df_age(df)
    df_os = transform_df_os(df)
    df_final = transform_final_df(df_gender, df_age, df_os)
    load(df_final)

get_count_action_MZ = get_count_action_MZ()