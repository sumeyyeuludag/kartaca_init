import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from copy import deepcopy
import mysql.connector
import requests
mydb = mysql.connector.connect(
  host="host.docker.internal",
  user="kartaca",
  password="kartaca",
  database="kartaca"
)

cursor = mydb.cursor()
#defaults
default_args = {
        'owner': 'kartaca',
        'start_date': datetime(2022, 3, 4),
        'retries': 3,
        'retry_delay': timedelta(minutes=5)}
currency_dag = DAG('currency_dag',
        default_args=default_args,
        description='CURRENCY DAG',
        schedule_interval='5 10 * * *',
        catchup=False,
)

def start():
    print('DAG has been started...')
    return 'DAG has been started...'

def read(**context):
    currency_response = requests.get("http://country.io/currency.json")
    currency_response = currency_response.json()
    context['ti'].xcom_push(key='currency_response', value=currency_response)
    return currency_response
def insert(**context):
    currency_response = context['ti'].xcom_pull(key='currency_response')
    for key in currency_response:
      cursor.execute("INSERT INTO currency (country_abbr, currency_type) VALUES ('"+key+"', '"+currency_response[key]+"')")
    mydb.commit()
    mydb.close()

def end():
    print('DAG is completed...')
    return 'DAG is completed...'


start_task = PythonOperator(task_id='start_task', python_callable=start, dag=currency_dag)
read_task = PythonOperator(task_id='read_task', python_callable=read, dag=currency_dag,provide_context=True)
insert_task = PythonOperator(task_id='insert_task', python_callable=insert, dag=currency_dag,provide_context=True)
end_task = PythonOperator(task_id='end_task', python_callable=end, dag=currency_dag)

start_task >>  read_task >>  insert_task >> end_task