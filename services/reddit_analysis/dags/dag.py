import datetime as dt

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import (
    PythonOperator, ShortCircuitOperator
)





'''
--------------------------------
Reddit Scrape - DAG Arguments
================================
'''


metric_default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'email': ['ccastleberry@covenanttransport.com', 
              'thenderson@covenanttransport.com',
              'cmin@convenanttransport.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'end_date': datetime(2024, 12, 31),
}



'''
--------------------------------
Reddit Scrape - DAG Definition
================================
'''


dag = DAG(
    'Reddit-Scrape-V0.1',
    default_args=metric_default_args,
    schedule_interval=timedelta(days=1),
    concurrency=4,
    catchup=True
)