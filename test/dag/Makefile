DAG_ID = example_dag_with_teradata

CUR_DIR = $(shell pwd)
VENV_DIR = $(CUR_DIR)/../../.venv
AIRFLOW_HOME = $(CUR_DIR)/../../.airflow
CONFIG = $(CUR_DIR)/.config/stg.json
DAG_DIR = $(CUR_DIR)/../../dag

dag-test: $(VENV_DIR) $(AIRFLOW_HOME) $(CONFIG) $(DAG_DIR)
	. $(VENV_DIR)/bin/activate && \
	export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	airflow variables import $(CONFIG) && \
	airflow dags test -S $(DAG_DIR) $(DAG_ID)

