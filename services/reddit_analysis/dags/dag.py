import datetime as dt
import yaml 
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import (
    PythonOperator, ShortCircuitOperator
)

from airflow_dags.services.reddit_analysis.dags import sub_overview_node as son


'''
-----------------------------
Helper Functions
=============================
'''

def subreddit_overview(subreddit, **context):
    date = context['prev_ds']
    summary_path = son.daily_summary_node(subreddit, date=date)
    return summary_path




'''
--------------------------------
Reddit Analysis - DAG Arguments
================================
'''


metric_default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2018, 1, 1),
    'email': ['castle.caleb@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'end_date': dt.datetime(2024, 12, 31),
}



'''
--------------------------------
Reddit Analysis - DAG Definition
================================
'''


dag = DAG(
    'Reddit-Analysis-V0.1',
    default_args=metric_default_args,
    schedule_interval=timedelta(days=1),
    concurrency=4,
    catchup=True
)

'''
-----------------------------
Reddit Analysis - Define Operators
=============================
'''

config_path = Path(__file__).parent.joinpath('config', 'dag_config.yaml')
with open(config_path) as f:
    dag_cfg = yaml.load(f, Loader=yaml.FullLoader)

summary_ops = []
for subreddit in dag_cfg['subreddits']:
    task = PythonOperator(
        task_id=f"{subreddit} summary",
        python_callable=subreddit_overview,
        op_args=[subreddit],
        dag=dag,
    )
    summary_ops.append(task)