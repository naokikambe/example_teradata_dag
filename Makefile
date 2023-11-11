test:
	if [ ! -d "airflow_test_env" ]; then \
		python3 -m venv airflow_test_env && \
			. airflow_test_env/bin/activate && \
			pip install -r requirements.txt && \
			airflow db init && \
			python test/test_teradata_example_dag.py && \
			deactivate; \
	else \
			. airflow_test_env/bin/activate && \
			python test/test_teradata_example_dag.py && \
			deactivate; \
	fi

clean:
	rm -rf airflow_test_env
	find . -type d -name __pycache__ -exec rm -r {} +

.PHONY: test clean
