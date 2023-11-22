"""
example_dag_with_teradata.py

This script defines an Airflow DAG that consists of three tasks:
- task1: DummyOperator
- task2: PythonOperator that executes a Teradata SQL query
- task3: DummyOperator

The DAG runs daily and is configured with default parameters.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import sys, os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from teradata_example import read_file_content, teradata_query_callback

@dag(
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),  # Run the DAG daily
    description='A simple example DAG with Teradata connection',
    doc_md=read_file_content('README.md'),
)
def example_dag_with_teradata():

    # Define two tasks: task1, and task2
    task1 = DummyOperator(
        task_id='task1',
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=teradata_query_callback,
        provide_context=True,
        retries=15,
        retry_delay=timedelta(seconds=0),
        execution_timeout=timedelta(minutes=15),
    )

    task3 = DummyOperator(
        task_id='task3',
    )

    # Set up the task dependencies
    task1 >> task2  # task2 depends on task1
    task2 >> task3  # task3 depends on task2

example_dag = example_dag_with_teradata()
