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
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import teradatasql
import time
import os
import sys

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
    # Connect to Teradata using "with" statement
    with teradatasql.connect(host=host, user=login, password=password) as connection:
        end_time = time.time()
        assert connection.ping(), "Connection is not available."

        connection_time = end_time - start_time
        # Log connection time
        print(f"Teradata Connection Time: {connection_time} seconds")
        # Assert that the connection time is less than or equal to 60 seconds
        assert connection_time <= 60, "Connection time exceeds 60 seconds!"

        # Get the path to the SQL file using os.path.join
        sql_file_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'query.sql')
        # Read SQL query from the file
        query = ""
        with open(sql_file_path, 'r') as sql_file:
            query = sql_file.read()

        # Execute a SQL query
        cursor = connection.cursor()
        return_code = cursor.execute(query)

        # Log the return code
        print(f"Teradata Return Code: {return_code}")

        # Assert that the return code is zero
        assert return_code == 0, f"Teradata Query Execution Failed with Return Code: {return_code}"

        # Log the result
        result = cursor.fetchall()
        for row in result:
            print(row)


# Define default_args dictionary to specify default parameters for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG object with the provided default_args
dag = DAG(
    'example_dag_with_teradata',
    default_args=default_args,
    description='A simple example DAG with Teradata connection',
    schedule_interval=timedelta(days=1),  # Run the DAG daily
)

# Define two tasks: task1, and task2
task1 = DummyOperator(
    task_id='task1',
    dag=dag,
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=teradata_query_callback,
    provide_context=True,
    dag=dag,
)

task3 = DummyOperator(
    task_id='task3',
    dag=dag,
)

# Set up the task dependencies
task1 >> task2  # task2 depends on task1
task2 >> task3  # task3 depends on task2
