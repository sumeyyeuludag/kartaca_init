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
data_merge_dag = DAG('data_merge_dag',
        default_args=default_args,
        description='DATA MERGE DAG',
        schedule_interval='10 10 * * *',
        catchup=False,
)
def start():
    print('DAG has been started...')
    return 'DAG has been started...'
def merge():
    cursor.execute("DROP TABLE IF EXISTS data_merge")
    cursor.execute("CREATE TABLE data_merge AS SELECT country.country_abbr , currency.currency_type , country.country_name FROM country JOIN currency ON country.country_abbr =currency.country_abbr ;")
    mydb.commit()
    mydb.close()

def end():
    print('DAG is completed...')
    return 'DAG is completed...'


start_task = PythonOperator(task_id='start_task', python_callable=start, dag=data_merge_dag)
merge_task = PythonOperator(task_id='merge_task', python_callable=merge, dag=data_merge_dag)
end_task = PythonOperator(task_id='end_task', python_callable=end, dag=data_merge_dag)

start_task >>  merge_task >> end_task