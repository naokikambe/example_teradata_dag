import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag

TASK_IDS = ['task1', 'task2', 'task3']

class TestTeradataExampleDAG(unittest.TestCase):
    def setUp(self):
        self.dag_folder_path = './dag'
        if not os.path.exists(self.dag_folder_path):
            raise FileNotFoundError(f'DAGs folder not found: {self.dag_folder_path}')
        self.dagbag = DagBag(dag_folder=self.dag_folder_path,
                             include_examples=False)
        self.dag_id = 'example_dag_with_teradata'
        self.dag = self.dagbag.get_dag(self.dag_id)
        sys.path.append("./dag")

    def tearDown(self):
        sys.path.remove("./dag")

    def test_dag_loaded_successfully(self):
        print(f'DAG: {self.dag}')
        self.assertIsNotNone(self.dag, 'DAG is None. Loading failed.')
        self.assertFalse(len(self.dagbag.import_errors),
                         f'DAG loading failed. Errors: {self.dagbag.import_errors}' )

    def test_task_count(self):
        self.assertEqual(len(self.dag.tasks), 3)

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

    @patch('teradatasql.connect')
    def test_teradata_query_callback(self, mock_connect):
        from teradata_example_dag import teradata_query_callback

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.ping.return_value = True
        mock_cursor.execute.return_value = 0
        mock_cursor.fetchall.return_value = [(1, 'data1'), (2, 'data2')]

        mock_open = MagicMock()
        mock_open.return_value.__enter__.return_value.read.return_value = "SELECT * FROM another_table;"
        with patch('builtins.open', mock_open):
            # Call the function under test
            teradata_query_callback()

        mock_connect.assert_called_once_with(host='your_teradata_hostname',
                                             user='your_teradata_username',
                                             password='your_teradata_password')
        mock_connection.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT * FROM another_table;")
        mock_cursor.fetchall.assert_called_once()

    @patch('teradatasql.connect')
    def test_teradata_query_callback_execute_failure(self, mock_connect):
        from teradata_example_dag import teradata_query_callback

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.ping.return_value = True
        mock_cursor.execute.return_value = 1
        mock_cursor.fetchall.return_value = []

        with self.assertRaises(Exception) as context:
            mock_open = MagicMock()
            mock_open.return_value.__enter__.return_value.read.return_value = "SELECT * FROM another_table;"
            with patch('builtins.open', mock_open):
                # Call the function under test
                teradata_query_callback()

        self.assertEquals("Teradata Query Execution Failed with Return Code: 1",
                          str(context.exception))

        mock_connect.assert_called_once_with(host='your_teradata_hostname',
                                             user='your_teradata_username',
                                             password='your_teradata_password')
        mock_connection.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT * FROM another_table;")
        mock_cursor.fetchall.assert_not_called()

    @patch('teradatasql.connect')
    @patch('time.time', side_effect=[0, 70])
    def test_teradata_query_callback_exceed_time_limit(self, mock_time, mock_connect):
        from teradata_example_dag import teradata_query_callback

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.ping.return_value = True
        mock_cursor.execute.return_value = 1
        mock_cursor.fetchall.return_value = []

        with self.assertRaises(AssertionError) as context:
            teradata_query_callback()

        self.assertEquals("Connection time exceeds 60 seconds!",
                          str(context.exception))
        mock_connect.assert_called_once_with(host='your_teradata_hostname',
                                             user='your_teradata_username',
                                             password='your_teradata_password')
        mock_connection.cursor.assert_not_called()
        mock_cursor.execute.assert_not_called()
        mock_cursor.fetchall.assert_not_called()


if __name__ == '__main__':
    unittest.main()
