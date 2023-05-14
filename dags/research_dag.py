from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime,timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 14),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=5),
    'catchup_by_default' : False,
    'catchup':False
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

with DAG(dag_id="research_etl", default_args=default_args,catchup=False, schedule="0 0/2 * * *") as dag:

    @task(task_id='research_etl_task')
    def top_50_etl():
        print('empieza task research')
        from research_etl import run_research_etl
        run_research_etl()
        return "research task finished"
    
    research = top_50_etl() #Mandatory


    start = EmptyOperator(task_id='Start')
    finish = EmptyOperator(task_id='Finish')



    start >> research >> finish