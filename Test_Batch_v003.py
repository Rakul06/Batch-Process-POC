from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import requests
from OverAllTasks import getVersion

default_args = {
    'owner': 'Rakul',
    'depends_on_past': False,
    'start_date': datetime(2024,1,5),
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
    'catchup': False,
    "email_on_failure":True,
    "email_on_retry":True,
    "email":["rahulsk8223@gmail.com"]
}

dag = DAG(
    'Test_Batch_v003',
    default_args=default_args,
    description='Test batch from batch service',
    schedule_interval = '0 12 * * *'
)

email_fun=EmailOperator(
task_id="Success_email",
to=('rahulsk8223@gmail.com', ' '),
subject="Batch Processing success Alert",
html_content="""<h1>Batch Test_Batch_v003 Executed Successfully !!! <h1>
""",
dag = dag
)

task_1 = PythonOperator(
    task_id = 'task_1',
    python_callable = getVersion,
    dag = dag
)

task_1 >> email_fun
