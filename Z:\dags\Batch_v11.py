from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import requests
from OverAllTasks import getName

default_args = {
    'owner': 'Rakul',
    'depends_on_past': False,
    'start_date': datetime(2023,12,5),
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'catchup': False,
    "email_on_failure":True,
    "email_on_retry":True,
    "email":["rahulsk8223@gmail.com"]
}

dag = DAG(
    'Batch_v11',
    default_args=default_args,
    description='creating dynamic dag using python flask',
    schedule_interval = '0 3 * * *'
)

email_fun=EmailOperator(
task_id="Success_email",
to=('rahulsk8223@gmail.com', ' '),
subject="Batch Processing success Alert",
html_content="""<h1>Batch Batch_v11 Executed Successfully !!! <h1>
""",
dag = dag
)

task_1 = PythonOperator(
    task_id = 'task_1',
    python_callable = getName,
    dag = dag
)

task_1 >> email_fun