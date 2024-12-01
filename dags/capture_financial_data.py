from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine
import json

# Função 1: Simular a captura de dados da API e salvar no volume Bronze
def capture_api_data():
    """
    Simula a captura de dados de uma API e salva em arquivos JSON na pasta Bronze.
    """
    # Simulando os dados retornados pela API
    simulated_data = [
        {"ticker": "AAPL", "price": 150.3, "date": "2023-12-01"},
        {"ticker": "GOOGL", "price": 2800.5, "date": "2023-12-01"},
    ]

    # Caminho para a Bronze
    bronze_path = '/usr/local/airflow/data/bronze'
    os.makedirs(bronze_path, exist_ok=True)

    # Salvar com a data de processamento no nome do arquivo
    filename = f"financial_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    file_path = os.path.join(bronze_path, filename)

    with open(file_path, 'w') as json_file:
        json.dump(simulated_data, json_file)

    print(f"Dados da API salvos em: {file_path}")

# Função 2: Consolidar arquivos JSON e subir para o PostgreSQL
def load_bronze_to_postgres():
    """
    Lê todos os arquivos JSON da camada Bronze e insere no PostgreSQL.
    """
    # Caminho da camada Bronze
    bronze_path = '/usr/local/airflow/data/bronze'
    json_files = [f for f in os.listdir(bronze_path) if f.endswith('.json')]

    # Consolida os arquivos JSON em um DataFrame
    dataframes = []
    for file in json_files:
        file_path = os.path.join(bronze_path, file)
        dataframes.append(pd.read_json(file_path))
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Insere no PostgreSQL
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    combined_df.to_sql('bronze_financial_data', engine, if_exists='replace', index=False)
    print("Dados da Bronze carregados no PostgreSQL.")

# Configuração padrão da DAG
default_args = {'owner': 'airflow', 'retries': 1}
with DAG(
    dag_id='capture_and_transform_financial_data',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Tarefa 1: Capturar dados da API e salvar no volume Bronze
    capture_data = PythonOperator(
        task_id='capture_api_data',
        python_callable=capture_api_data,
    )

    # Tarefa 2: Carregar os arquivos JSON no PostgreSQL
    load_data_to_postgres = PythonOperator(
        task_id='load_bronze_to_postgres',
        python_callable=load_bronze_to_postgres,
    )

    # Tarefa 3: Executar os modelos DBT para Silver
    run_dbt_silver = BashOperator(
        task_id="run_dbt_silver",
        bash_command=(
            "cd /usr/local/airflow/dbt && "
            "dbt run --profiles-dir /usr/local/airflow/dbt --project-dir /usr/local/airflow/dbt --select silver"
        ),
        env=None,  # Deixe como `None` para usar as variáveis do ambiente
    )


    # Tarefa 4: Executar os modelos DBT para Gold
    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold",
        bash_command=(
            "cd /usr/local/airflow/dbt && "
            "dbt run --profiles-dir /usr/local/airflow/dbt --project-dir /usr/local/airflow/dbt --select gold"
        ),
        env=None,  # Deixe como `None` para usar as variáveis do ambiente
    )

    # Dependências
    capture_data >> load_data_to_postgres >> run_dbt_silver >> run_dbt_gold

