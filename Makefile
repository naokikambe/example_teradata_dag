VENV_DIR := .venv
AIRFLOW_DIR := .airflow

$(VENV_DIR):
	python3 -m venv $(VENV_DIR)
	. $(VENV_DIR)/bin/activate && \
	pip3 install -r requirements.txt ; \
	deactivate

$(AIRFLOW_DIR):
	. $(VENV_DIR)/bin/activate && \
	env AIRFLOW_HOME=`pwd`/$(AIRFLOW_DIR) airflow db init ; \
	deactivate

test: $(VENV_DIR) $(AIRFLOW_DIR)
	. $(VENV_DIR)/bin/activate && \
	python3 -m unittest discover -s test -p 'test_*.py' ; \
	deactivate

clean:
	rm -rf $(AIRFLOW_DIR) $(VENV_DIR)
	find . -type d -name __pycache__ -exec rm -r {} +

.PHONY: test clean
