import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import date, datetime, timedelta
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
    'start_date': datetime(2023, 9, 19),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

chat_id = -928988566

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_miloserdov_alert_1():

	@task
	def extract_feed():
		ds = str(date.today() - timedelta(days = 1))
		query_feed = f'''
			select
				toDate(time) date,
				uniqExact(user_id) dau,
				countIf(user_id, action='view') views,
				countIf(user_id, action='like') likes,
				likes/views ctr
			from simulator_20230820.feed_actions
			where (toDate(time) <= toDate('{ds}')) and (toDate(time) > toDate('{ds}') - 7)
			group by date'''

		# эта функция выполнит запрос и запишет его результат в pandas DataFrame
		df_feed = ph.read_clickhouse(query_feed, connection=connection)
		df_feed = df_feed.set_index('date', drop=True)

		return df_feed

	@task
	def form_report(df_feed):
		ds = str(date.today() - timedelta(days = 1))
		media_array = []
		text = f'Метрики за {ds}\n'
		for column in df_feed.columns:

			metr_value = '{:,}'.format(round(df_feed[column][-1], 4)).replace(',', ' ')
			text += f"""Значение метрики {column} = {metr_value}\n""" #text crearion

			sns.lineplot(data = df_feed, x=df_feed.index, y=column) #plot creation
			plt.title(f'{column} за последние 7 дней')
			plot_object = io.BytesIO()
			plt.savefig(plot_object)
			plot_object.seek(0)
			plot_object.name = f'{column}_plot.png'
			media_array.append(telegram.InputMediaPhoto(plot_object))
			plt.close()

		bot.sendMessage(chat_id=chat_id, text=text)
		bot.sendMediaGroup(chat_id=chat_id, media=media_array)

	df_feed = extract_feed()
	form_report(df_feed)

dag_miloserdov_alert_1 = dag_miloserdov_alert_1()
