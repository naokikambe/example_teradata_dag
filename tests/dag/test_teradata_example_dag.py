import os
import sys
import unittest
from datetime import timedelta
from unittest.mock import patch, MagicMock
from airflow.models import DagBag

TASK_IDS = ['task1', 'task2', 'task3']

class TestTeradataExampleDAG(unittest.TestCase):
    def setUp(self):
        self.dag_folder_path = './dags'
        if not os.path.exists(self.dag_folder_path):
            raise FileNotFoundError(f'DAGs folder not found: {self.dag_folder_path}')
        self.dagbag = DagBag(dag_folder=self.dag_folder_path,
                             include_examples=False)
        self.dag_id = 'example_dag_with_teradata'
        self.dag = self.dagbag.get_dag(self.dag_id)
        sys.path.append(self.dag_folder_path)

    def tearDown(self):
        sys.path.remove(self.dag_folder_path)

    def test_dag_loaded_successfully(self):
        print(f'DAG: {self.dag}')
        self.assertIsNotNone(self.dag, 'DAG is None. Loading failed.')
        self.assertFalse(len(self.dagbag.import_errors),
                         f'DAG loading failed. Errors: {self.dagbag.import_errors}' )
        self.assertEqual("example_dag_with_teradata",
                         self.dag.dag_id)
        self.assertEqual("A simple example DAG with Teradata connection",
                         self.dag.description)

    def test_task_count(self):
        self.assertEqual(len(self.dag.tasks), 3)

    def test_task_params(self):
        task = self.dag.get_task("task2")
        self.assertEqual(15, task.retries)
        self.assertEqual(timedelta(seconds=0), task.retry_delay)
        self.assertEqual(timedelta(minutes=15), task.execution_timeout)

    def test_task_dependencies(self):
        task_ids = [task.task_id for task in self.dag.tasks]
        expected_dependencies = {}
        for i in range(len(TASK_IDS)):
            if i < len(TASK_IDS) - 1:
                expected_dependencies[TASK_IDS[i]] = [TASK_IDS[i+1]]
            else:
                expected_dependencies[TASK_IDS[i]] = []
        for task_id, dependencies in expected_dependencies.items():
            task = self.dag.get_task(task_id)
            downstream_task_ids = [task.task_id for task in task.downstream_list]
            self.assertEqual(set(downstream_task_ids),
                             set(dependencies), f'Incorrect dependencies for {task_id}')

    @patch('teradatasql.connect', spec=True)
    @patch('builtins.open', spec=True)
    def test_teradata_query_callback(self, mock_open, mock_conn):
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute.return_value = 0
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchall.return_value = [(1, 'data1'), (2, 'data2')]
        mock_open.return_value.__enter__.return_value.read.return_value = "SELECT * FROM another_table;"
        self.dag.get_task("task2").python_callable(dag=self.dag, params=self.dag.get_task("task2").params)
        mock_conn.assert_called_once_with(host='your_teradata_hostname',
                                          user='your_teradata_username',
                                          password='your_teradata_password')
        mock_open.return_value.__enter__.return_value.read.assert_called_once()
        mock_conn.return_value.__enter__.return_value.cursor.assert_called_once()

        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute.assert_called_once_with("SELECT * FROM another_table;")

    @patch('teradatasql.connect', spec=True)
    @patch('builtins.open', spec=True)
    def test_teradata_query_callback_execute_failure(self, mock_open, mock_conn):
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute.return_value = 1
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchall.return_value = []
        mock_open.return_value.__enter__.return_value.read.return_value = "SELECT * FROM another_table;"
        with self.assertRaises(Exception) as context:
            self.dag.get_task("task2").python_callable(dag=self.dag, params=self.dag.get_task("task2").params)
        self.assertEquals("Teradata Query Execution Failed with Return Code: 1",
                          str(context.exception))
        mock_conn.assert_called_once_with(host='your_teradata_hostname',
                                          user='your_teradata_username',
                                          password='your_teradata_password')
        mock_open.return_value.__enter__.return_value.read.assert_called_once()
        mock_conn.return_value.__enter__.return_value.cursor.assert_called_once()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute.assert_called_once_with("SELECT * FROM another_table;")

    @patch('teradatasql.connect', spec=True)
    @patch('builtins.open', spec=True)
    @patch('time.time', side_effect=[0, 70])
    def test_teradata_query_callback_exceed_time_limit(self, mock_time, mock_open, mock_conn):
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute.return_value = 0
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchall.return_value = [(1, 'data1'), (2, 'data2')]
        mock_open.return_value.__enter__.return_value.read.return_value = "SELECT * FROM another_table;"
        with self.assertRaises(AssertionError) as context:
            self.dag.get_task("task2").python_callable(dag=self.dag)
        self.assertEquals("Connection time exceeds 60 seconds!",
                          str(context.exception))
        mock_conn.assert_called_once_with(host='your_teradata_hostname',
                                          user='your_teradata_username',
                                          password='your_teradata_password')
        mock_open.return_value.__enter__.return_value.read.assert_not_called()
        mock_conn.return_value.__enter__.return_value.cursor.assert_not_called()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
