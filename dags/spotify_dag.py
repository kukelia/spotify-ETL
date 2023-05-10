from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime,timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="spotify_etl", default_args=default_args, schedule="0 13 * * *") as dag:

    @task(task_id='top_50_etl')
    def top_50_etl():
        print('empieza task top_50')
        from spotify_top50_etl import run_top50_etl
        run_top50_etl()
        return "top_50 task finished"
    top_50_etl = top_50_etl() #Mandatory

    start = EmptyOperator(task_id='Start')
    finish = EmptyOperator(task_id='Finish')



    start >> top_50_etl >> finish


    