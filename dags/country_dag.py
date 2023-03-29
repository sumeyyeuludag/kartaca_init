import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from copy import deepcopy
#from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
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
country_dag = DAG('country_dag',
        default_args=default_args,
        description='COUNTRY DAG',
        schedule_interval='0 10 * * *',
        catchup=False,
)

def start():
    print('DAG has been started...')
    return 'DAG has been started...'

def read(**context):
    names_response = requests.get("http://country.io/names.json")
    names_response = names_response.json()
    context['ti'].xcom_push(key='names_response', value=names_response)
    return names_response

def insert(**context):
    names_response = context['ti'].xcom_pull(key='names_response')
    for key in names_response:
      cursor.execute("INSERT INTO country (country_abbr, country_name) VALUES ('"+key+"', '"+names_response[key]+"')")
    mydb.commit()
    mydb.close()

def end():
    print('DAG is completed...')
    return 'DAG is completed...'


start_task = PythonOperator(task_id='start_task', python_callable=start, dag=country_dag)
read_task = PythonOperator(task_id='read_task', python_callable=read, dag=country_dag,provide_context=True)
insert_task = PythonOperator(task_id='insert_task', python_callable=insert, dag=country_dag,provide_context=True)
end_task = PythonOperator(task_id='end_task', python_callable=end, dag=country_dag)

# Set the order of execution of tasks.
start_task >>  read_task >>  insert_task >> end_task