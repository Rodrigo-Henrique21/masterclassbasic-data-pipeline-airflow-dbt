from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os

# Função para capturar dados simulados e salvar na camada Bronze
def fetch_data_from_api():
    simulated_data = [
        {"ticker": "AAPL", "price": 175.64, "volume": 1052000, "date": "2024-11-29"},
        {"ticker": "GOOG", "price": 143.15, "volume": 750300, "date": "2024-11-29"},
        {"ticker": "TSLA", "price": 230.27, "volume": 1204000, "date": "2024-11-29"}
    ]
    bronze_path = "/usr/local/airflow/data/bronze/financial_data.json"
    os.makedirs(os.path.dirname(bronze_path), exist_ok=True)
    with open(bronze_path, "w") as f:
        json.dump(simulated_data, f)

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='capture_financial_data',
    default_args=default_args,
    start_date=datetime(2024, 11, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    capture_task = PythonOperator(
        task_id='fetch_data_from_api',
        python_callable=fetch_data_from_api
    )

    capture_task
