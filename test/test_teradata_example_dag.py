import os
import unittest
from airflow.models import DagBag
from datetime import timedelta

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
        # Check if the DAG directory exists
        self.assertTrue(os.path.exists(self.dag_folder_path), f'DAGs folder not found: {self.dag_folder_path}')

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
        expected_dependencies = {
            'task1': ['task2'],
            'task2': ['task3'],
            'task3': [],
        }

        for task_id, dependencies in expected_dependencies.items():
            task = dag.get_task(task_id)
            downstream_task_ids = [task.task_id for task in task.downstream_list]
            self.assertEqual(set(downstream_task_ids), set(dependencies), f'Incorrect dependencies for {task_id}')

if __name__ == '__main__':
    unittest.main()
