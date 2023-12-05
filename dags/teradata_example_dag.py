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
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import sys
import os
import teradatasql
import time

def read_file_content(file_name):
    file_path = os.path.join(os.path.dirname(__file__), file_name)
    with open(file_path, 'r') as file:
        return file.read()




# configs object
dag_configs = {
    "example_dag_with_teradata": {
        "schedule": timedelta(minutes=5),
        "description": 'A simple example DAG with Teradata connection',
        "params": {
            'retries': 15,
            'params': {
                'sql_file': "query.sql"
            }
        }
    },
    "example_dag_with_teradata_2": {
        "schedule": timedelta(minutes=5),
        "description": 'A simple example DAG with Teradata connection 2nd',
        "params": {
            'retries': 15,
            'params': {
                'sql_file': "query.sql"
            }
        }
    },
    "example_dag_with_teradata_3": {
        "schedule": timedelta(minutes=5),
        "description": 'A simple example DAG with Teradata connection 3rd',
        "params": {
            'retries': 15,
            'params': {
                'sql_file': "query.sql"
            }
        }
    },
    "example_dag_with_teradata_4": {
        "schedule": timedelta(minutes=5),
        "description": 'A simple example DAG with Teradata connection 4th',
        "params": {
            'retries': 15,
            'params': {
                'sql_file': "query.sql"
            }
        }
    },
}

for dag_id, dag_conf in dag_configs.items():
    from airflow.decorators import dag, task

    @dag(
        dag_id=dag_id,
        default_args={
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': datetime(2023, 1, 1),
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        doc_md=read_file_content('README.md'),
        **dag_conf,
    )
    def generate_dag():
        @task(
            task_id='task2',
            retry_delay=timedelta(seconds=0),
            execution_timeout=timedelta(minutes=15),
            **dag_conf['params'],
        )
        def teradata_query_callback(**kwargs):
            """
            teradata_query_callback(**kwargs)

            This function connects to Teradata and executes a SQL query.

            Args:
                **kwargs: Context passed by Airflow.

            Returns:
                None
            """
            # Directly assign Teradata connection details
            login = 'your_teradata_username'
            password = 'your_teradata_password'
            host = 'your_teradata_hostname'

            start_time = time.time()
            dag_id = kwargs['dag'].dag_id
            # Connect to Teradata using "with" statement
            with teradatasql.connect(host=host, user=login, password=password) as connection:
                end_time = time.time()
                connection_time = end_time - start_time
                # Log connection time
                print(f"Teradata Connection Time: {connection_time} seconds")
                # Assert that the connection time is less than or equal to 60 seconds
                assert connection_time <= 60, "Connection time exceeds 60 seconds!"

                # Get the path to the SQL file using os.path.join
                sql_file_path = os.path.join(os.path.dirname(__file__), '..', 'sql', kwargs['params']['sql_file'])
                # Read SQL query from the file
                with open(sql_file_path, 'r') as sql_file:
                    query = sql_file.read()

                # Execute a SQL query
                with connection.cursor() as cursor:
                    return_code = cursor.execute(query)

                    # Log the return code
                    print(f"Teradata Return Code: {return_code}")

                    # Assert that the return code is zero
                    assert return_code == 0, f"Teradata Query Execution Failed with Return Code: {return_code}"

                    # Log the result
                    result = cursor.fetchall()
                    for row in result:
                        print(row)

        task1 = EmptyOperator(task_id='task1')
        task2 = teradata_query_callback()    
        task3 = EmptyOperator(task_id='task3')

        # Set up the task dependencies
        task1 >> task2  # task2 depends on task1
        task2 >> task3  # task3 depends on task2

    dag = generate_dag()
