import os
import unittest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag

TASK_IDS = ['task1', 'task2', 'task3']

class TestTeradataExampleDAG(unittest.TestCase):
    def setUp(self):
        # Define the path to the DAGs folder
        self.dag_folder_path = './dag'

        # Check if the DAGs folder exists
        if not os.path.exists(self.dag_folder_path):
            raise FileNotFoundError(f'DAGs folder not found: {self.dag_folder_path}')

        # Initialize the DagBag
        self.dagbag = DagBag(dag_folder=self.dag_folder_path, include_examples=False)

        # Assign the DAG ID
        self.dag_id = 'example_dag_with_teradata'

    def test_dag_loaded_successfully(self):
        # Check if the DAG loaded successfully
        dag = self.dagbag.get_dag(self.dag_id)
        print(f'DAG: {dag}')  # Add this line to check if DAG is loaded

        # Add a line to check if dag is not None
        self.assertIsNotNone(dag, 'DAG is None. Loading failed.')

        self.assertFalse(
            len(self.dagbag.import_errors),
            f'DAG loading failed. Errors: {self.dagbag.import_errors}'
        )

    def test_task_count(self):
        # Check if the DAG has the expected number of tasks
        dag = self.dagbag.get_dag(self.dag_id)
        self.assertEqual(len(dag.tasks), 3)  # Updated to reflect the new task count

    def test_task_dependencies(self):
        # Check if the task dependencies are set up correctly
        dag = self.dagbag.get_dag(self.dag_id)
        task_ids = [task.task_id for task in dag.tasks]

        # Define your expected task dependencies
        expected_dependencies = {}
        for i in range(len(TASK_IDS)):
            if i < len(TASK_IDS) - 1:
                expected_dependencies[TASK_IDS[i]] = [TASK_IDS[i+1]]
            else:
                expected_dependencies[TASK_IDS[i]] = []

        for task_id, dependencies in expected_dependencies.items():
            task = dag.get_task(task_id)
            downstream_task_ids = [task.task_id for task in task.downstream_list]
            self.assertEqual(set(downstream_task_ids), set(dependencies), f'Incorrect dependencies for {task_id}')

    @patch('teradatasql.connect')
    def test_teradata_query_callback(self, mock_connect):
        import sys
        sys.path.append('./dag')
        from teradata_example_dag import teradata_query_callback
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, 'data1'), (2, 'data2')]
        mock_cursor.execute.return_value = 0

        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_connection

        # Call the function under test
        teradata_query_callback()

        # Assertions
        mock_connect.assert_called_once_with(host='your_teradata_hostname', user='your_teradata_username', password='your_teradata_password')
        mock_connection.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT * FROM your_table;")
        mock_cursor.fetchall.assert_called_once()

if __name__ == '__main__':
    unittest.main()
