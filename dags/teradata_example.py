import teradatasql
import time
import os

def read_file_content(file_name):
    file_path = os.path.join(os.path.dirname(__file__), file_name)
    with open(file_path, 'r') as file:
        return file.read()

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
    assert dag_id == 'example_dag_with_teradata', "check dag id"
    # Connect to Teradata using "with" statement
    with teradatasql.connect(host=host, user=login, password=password) as connection:
        end_time = time.time()
        connection_time = end_time - start_time
        # Log connection time
        print(f"Teradata Connection Time: {connection_time} seconds")
        # Assert that the connection time is less than or equal to 60 seconds
        assert connection_time <= 60, "Connection time exceeds 60 seconds!"

        # Get the path to the SQL file using os.path.join
        sql_file_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'query.sql')
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
