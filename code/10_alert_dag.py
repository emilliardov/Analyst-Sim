import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import date, timedelta, datetime
from airflow.decorators import dag, task

sns.set(rc={'figure.figsize':(12,9)})
plt.style.use('fivethirtyeight')

connection = {'host': 'https://clickhouse.lab.karpov.courses',
'database':'simulator_20230820',
'user':'student',
'password':'dpo_python_2020'
}

#Bot stuff
token='6345594898:AAGavUThspIwOxq5SiJyKTzb6sLMRxiLXsc'
bot = telegram.Bot(token=token)

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 's.miloserdov-14',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 20),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

chat_id = -928988566

def detect_anomaly(df, metric, a=2.8, n=8):
    
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['low'] = df['q25'] - a * df['iqr']
    df['up'] = df['q75'] + a * df['iqr']
    
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()

    df['is_out'] = np.where((df[metric] < df['low']) | (df[metric] > df['up']), True, False)
    df['diff'] = abs((df[metric] - df[metric].shift(1)) * 100 / df[metric].shift(1))
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert=1
    else:
        is_alert=0
    
    return df, is_alert


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_miloserdov_alert_3():
    @task
    def send_alerts():
        
        query = """
        SELECT toStartOfFifteenMinutes(time) as t15,
            toDate(time) as day,
            formatDateTime(t15, '%R') as hm,
            uniqExact(user_id) as users,
            countIf(action='view') as views,
            countIf(action='like') as likes,
            likes/views as ctr
        FROM simulator_20230820.feed_actions
        WHERE time > (today() - 1) AND time < toStartOfFifteenMinutes(now())      
        GROUP BY t15, day, hm
        ORDER BY t15"""

        df_feed = ph.read_clickhouse(query, connection = connection)

        metrics = ['users', 'views', 'likes', 'ctr']
        
        for metric in metrics:
            df, is_alert = detect_anomaly(df_feed, metric)
        
            if is_alert == 1:
                current_value = int(df[metric].iloc[-1])
                previous_value = int(df[metric].iloc[-2])
                diff = (current_value - previous_value) / previous_value * 100
                
                message = f"""Метрика {metric} отклонилась от предыдущего значения на {diff:.2%}\nТекущее знаечние {current_value:.2f}"""
                
                plt.tight_layout()

                ax = sns.lineplot(x=df['t15'], y=df[metric], label=metric)
                ax = sns.lineplot(x=df['t15'], y=df['up'], label='up')
                ax = sns.lineplot(x=df['t15'], y=df['low'], label='low')

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel='time')
                ax.set(ylabel=metric)
                ax.set_title(metric)

                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = f'{metric}.png'
                plt.close()

                bot.sendMessage(chat_id=chat_id, text=message)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    send_alerts = send_alerts()
dag_miloserdov_alert_3 = dag_miloserdov_alert_3()
