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

configs = {
    "example_dag_with_teradata": {
        "retries": 1,
        "schedule_interval": timedelta(minutes=5),
        "description": 'A simple example DAG with Teradata connection',
        "doc_md": read_file_content('README.md'),
    },
}

for dag_id, conf in configs.items():
    @dag(
        default_args={
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': datetime(2023, 1, 1),
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': conf.get("retries"),
            'retry_delay': timedelta(minutes=5),
        },
        dag_id=dag_id,
        schedule_interval=conf.get("schedule_interval"),  # Run the DAG daily
        description=conf.get("description"),
        doc_md=conf.get("doc_md"),
    )
    def generate_dag():
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

    dag = generate_dag()
