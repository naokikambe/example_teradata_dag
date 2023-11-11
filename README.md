# Example Airflow DAG

## Description

Example Airflow DAG is an example Apache Airflow project that demonstrates how to create a DAG with tasks connecting to a Teradata database.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- Python 3.x
- Apache Airflow installed
- Teradata database access credentials

## Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/your-project.git
   cd your-project
   ```

2. Run the unit tests:

   ```bash
   make test
   ```

3. (Optional) Clean up the virtual environment:

   ```bash
   make clean
   ```

## DAG Overview

The main DAG (`teradata_example_dag.py`) in this project consists of the following tasks:

- **task1:** A dummy task.
- **task2:** A PythonOperator task connecting to a Teradata database.
- **task3:** Another PythonOperator task connecting to a Teradata database.

## Usage

1. Make sure your Apache Airflow instance is running.

2. Copy the `teradata_example_dag.py` file to your Airflow DAGs folder.

3. Access the Airflow web UI and trigger the DAG.

4. Monitor the DAG execution in the Airflow web UI.

## License

This project is licensed under the [MIT License](LICENSE).

## Contributing

To contribute to Example Airflow DAG, follow these steps:

1. Fork this repository.
2. Create a branch: `git checkout -b feature/your-feature-name`.
3. Make your changes and commit them: `git commit -m 'Your commit message'`.
4. Push to the original branch: `git push origin feature/your-feature-name`.
5. Create a pull request.

## Contact

If you have any questions or feedback, please feel free to reach out to [your.email@example.com](mailto:your.email@example.com).

## Acknowledgements

- [Apache Airflow](https://airflow.apache.org/)
- [Teradata](https://www.teradata.com/)
