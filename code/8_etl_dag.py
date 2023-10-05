# coding=utf-8

from datetime import datetime, timedelta, date
import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import karpov_secrets as ks

#параметры соединения
connection = ks.connection

connection_rw = ks.connection_rw


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 's.miloserdov-14',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 7, 4),
}

# Интервал запуска DAG
schedule_interval = '0 04 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=True)
def dag_miloserdov_etl():

    @task
    def extract_feed():
        ds = get_current_context()['ds']
        query_feed = f"""
                select
                    user_id, gender, age, os,
                    countIf(post_id, action='view') views,
                    countIf(post_id, action='like') likes
                from simulator_20230820.feed_actions
                where toDate(time) = toDate('{ds}')-1
                group by user_id, gender, age, os"""

        df_feed = ph.read_clickhouse(query_feed, connection=connection)
        return df_feed

    @task
    def extract_mess():
        ds = get_current_context()['ds']
        query_mess = f"""
            select user_id, gender, age, os, messages_sent, messages_received, users_sent, users_received
            from(
            select
                user_id, gender, age, os,
                count() messages_sent,
                uniqExact(reciever_id) users_sent
            from simulator_20230820.message_actions
            where toDate(time) = toDate('{ds}')-1
            group by user_id, gender, age, os) t1
            join
            (
            select
                reciever_id,
                count() messages_received,
                uniqExact(user_id) users_received
            from simulator_20230820.message_actions
            where toDate(time) = toDate('{ds}')-1
            group by reciever_id) t2
            on t1.user_id = t2.reciever_id"""

        df_mess = ph.read_clickhouse(query_mess, connection=connection)
        return df_mess    

    @task
    def merge_data(df_feed, df_mess):
        df_merge = pd.merge(df_feed, df_mess, on='user_id', how='outer', suffixes=('', '_y'))

        df_merge.gender = df_merge.gender.fillna(df_merge.gender_y)
        df_merge.age = df_merge.age.fillna(df_merge.age_y)
        df_merge.os = df_merge.os.fillna(df_merge.os_y)

        df_merge = df_merge.drop(columns=['gender_y', 'age_y', 'os_y'])
        df_merge = df_merge.fillna(0)

        df_merge[['gender', 'age']] = df_merge[['gender', 'age']].astype('int32').astype('str')

        return df_merge

    @task
    def transfrom_gender(df_merge):
        df_gender = df_merge[['gender', 'views', 'likes',  'messages_sent', 'messages_received', 'users_sent', 'users_received']]\
            .groupby(['gender'])\
            .sum()\
            .reset_index()
        
        df_gender = df_gender.rename(columns={"gender": "dimension_value"})
        df_gender['dimension'] = 'gender'
        
        return df_gender

    @task
    def transfrom_age(df_merge):
        df_age = df_merge[['age', 'views', 'likes',  'messages_sent', 'messages_received', 'users_sent', 'users_received']]\
            .groupby(['age'])\
            .sum()\
            .reset_index()
        
        df_age = df_age.rename(columns={"age": "dimension_value"})
        df_age['dimension'] = 'age'
        
        return df_age

    @task
    def transfrom_os(df_merge):
        df_os = df_merge[['os', 'views', 'likes',  'messages_sent', 'messages_received', 'users_sent', 'users_received']]\
            .groupby(['os'])\
            .sum()\
            .reset_index()
        
        df_os = df_os.rename(columns={"os": "dimension_value"})
        df_os['dimension'] = 'os'
        
        return df_os

    @task
    def load(df_gender, df_age, df_os):
        df_final = pd.concat([df_gender, df_age, df_os], ignore_index = True)
        ds = get_current_context()['ds']
        df_final['event_date'] = pd.to_datetime(ds, format='%Y-%m-%d') - pd.Timedelta(days=1)
        df_final[['views', 'likes',  'messages_sent', 'messages_received', 'users_sent', 'users_received']] = \
        df_final[['views', 'likes',  'messages_sent', 'messages_received', 'users_sent', 'users_received']].astype('int64')

        create = '''CREATE TABLE IF NOT EXISTS test.s_miloserdov_etl
                    (event_date Date,
                    dimension String,
                    dimension_value String,
                    views Int64,
                    likes Int64,
                    messages_sent Int64,
                    users_sent Int64,
                    messages_received Int64,
                    users_received Int64
                    ) ENGINE = MergeTree()
                    ORDER BY event_date
                    '''
        ph.execute(query=create, connection=connection_rw)

        ph.to_clickhouse(df=df_final, table='s_miloserdov_etl', index=False, connection=connection_rw)

    df_feed = extract_feed()
    df_mess = extract_mess()
    df_merge = merge_data(df_feed, df_mess)

    df_gender = transfrom_gender(df_merge)
    df_age = transfrom_age(df_merge)
    df_os = transfrom_os(df_merge)

    load(df_gender, df_age, df_os)

dag_miloserdov_etl = dag_miloserdov_etl()
