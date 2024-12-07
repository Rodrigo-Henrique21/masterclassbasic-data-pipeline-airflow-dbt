from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
from datetime import datetime, timedelta

import requests
import os
import json
import pandas as pd

# Parâmetros gerais
CHAVE_API_ALPHA_VANTAGE = "V5QSR6OB7WZ5LHGQ"
URL_BASE = "https://www.alphavantage.co/query"
CAMINHO_BRONZE = "/usr/local/airflow/data/bronze"
INDICADORES = [
    "EMA", "SMA", "CCI", "WMA", "DEMA", 
    "TEMA", "KAMA", "ADX", "RSI", 
    "WILLR", "OBV"
]
TICKERS = [
    "AAPL", "GOOGL", "MSFT",  # Ações estrangeiras
    "PETR4.SA", "VALE3.SA", "ITUB4.SA"  # Ações brasileiras
]

# Funções de captura de dados
# Função 1.0: Captura os dados do tesouro
def captura_ativos(ticker):
    """
    Captura dados das ações e salva na camada Bronze.
    """
    os.makedirs(CAMINHO_BRONZE, exist_ok=True)
    parametros = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "apikey": CHAVE_API_ALPHA_VANTAGE,
        "datatype": "json",
        "outputsize": "compact"
    }
    resposta = requests.get(URL_BASE, params=parametros)
    if resposta.status_code == 200:
        dados = resposta.json()
        nome_arquivo = f"acao_{ticker}.json"
        caminho_arquivo = os.path.join(CAMINHO_BRONZE, nome_arquivo)
        with open(caminho_arquivo, "w") as arquivo_json:
            json.dump(dados, arquivo_json)
        print(f"Dados do ticker {ticker} salvos em {caminho_arquivo}")
    else:
        print(f"Erro ao capturar dados para {ticker}: {resposta.status_code} - {resposta.text}")


# Função 1.1: Captura os dados do tesouro
def captura_dados_tesouro():
    """
    Captura dados do Tesouro e salva na camada Bronze.
    """
    os.makedirs(CAMINHO_BRONZE, exist_ok=True)
    parametros = {
        "function": "TREASURY_YIELD",
        "apikey": CHAVE_API_ALPHA_VANTAGE,
        "datatype": "json",
        "maturity": "2year",
        "interval": "daily"
    }
    resposta = requests.get(URL_BASE, params=parametros)
    if resposta.status_code == 200:
        dados = resposta.json()
        nome_arquivo = f"tesouro.json"
        caminho_arquivo = os.path.join(CAMINHO_BRONZE, nome_arquivo)
        with open(caminho_arquivo, "w") as arquivo_json:
            json.dump(dados, arquivo_json)
        print(f"Dados do Tesouro salvos em {caminho_arquivo}")
    else:
        print(f"Erro na API ao capturar dados do Tesouro: {resposta.status_code} - {resposta.text}")


# Função 1.2: Captura os dados dos indicadores tecnicos
def captura_indicadores_tecnicos(ticker):
    """
    Captura indicadores técnicos de um ativo e salva na camada Bronze.
    """
    os.makedirs(CAMINHO_BRONZE, exist_ok=True)
    for indicador in INDICADORES:
        parametros = {
            "function": indicador,
            "symbol": ticker,
            "interval": "daily",
            "time_period": 10,
            "series_type": "open",
            "apikey": CHAVE_API_ALPHA_VANTAGE
        }
        resposta = requests.get(URL_BASE, params=parametros)
        if resposta.status_code == 200:
            dados = resposta.json()
            nome_arquivo = f"indicador_{indicador}_{ticker}.json"
            caminho_arquivo = os.path.join(CAMINHO_BRONZE, nome_arquivo)
            with open(caminho_arquivo, "w") as arquivo_json:
                json.dump(dados, arquivo_json)
            print(f"Dados do indicador {indicador} para o ativo {ticker} salvos em {caminho_arquivo}")
        else:
            print(f"Erro ao capturar indicador {indicador} para {ticker}: {resposta.status_code} - {resposta.text}")


# Funções de carregamento no PostgreSQL
# Função 2.0: consolidada e persiste os dados na bronze ativos
def carrega_ativos_para_postgres():
    arquivos_json = [f for f in os.listdir(CAMINHO_BRONZE) if f.startswith("acao_")]
    dataframes = []
    for arquivo in arquivos_json:
        caminho_arquivo = os.path.join(CAMINHO_BRONZE, arquivo)
        with open(caminho_arquivo, 'r') as f:
            dados = json.load(f)
            if "Time Series (Daily)" in dados:
                df = pd.DataFrame.from_dict(dados["Time Series (Daily)"], orient="index")
                df.reset_index(inplace=True)
                df.rename(columns={"index": "data"}, inplace=True)
                df["ticker"] = dados["Meta Data"]["2. Symbol"]
                dataframes.append(df)
    if dataframes:
        df_final = pd.concat(dataframes, ignore_index=True)
        engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
        df_final.to_sql('ativos', engine, if_exists='replace', index=False)
        print("Dados de ações carregados no PostgreSQL com sucesso.")


# Função 2.1: consolidada e persiste os dados na bronze indicadores
def carrega_indicadores_para_postgres():
    arquivos_json = [f for f in os.listdir(CAMINHO_BRONZE) if f.startswith("indicador_")]
    dataframes = []
    for arquivo in arquivos_json:
        caminho_arquivo = os.path.join(CAMINHO_BRONZE, arquivo)
        with open(caminho_arquivo, 'r') as f:
            dados = json.load(f)
            for key in dados:
                if key.startswith("Technical Analysis"):
                    df = pd.DataFrame.from_dict(dados[key], orient="index")
                    df.reset_index(inplace=True)
                    indicador = dados["Meta Data"]["2: Indicator"]
                    df.rename(columns={"index": "data", df.columns[1]: indicador}, inplace=True)
                    df["ticker"] = dados["Meta Data"]["1: Symbol"]
                    dataframes.append(df)
    if dataframes:
        df_final = pd.concat(dataframes, ignore_index=True)
        engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
        df_final.to_sql('indicadores', engine, if_exists='replace', index=False)
        print("Dados de indicadores carregados no PostgreSQL com sucesso.")


# Função 2.2: consolidada e persiste os dados na bronze tesouro
def carrega_tesouro_para_postgres():
    arquivos_json = [f for f in os.listdir(CAMINHO_BRONZE) if f.startswith("tesouro_")]
    dataframes = []
    for arquivo in arquivos_json:
        caminho_arquivo = os.path.join(CAMINHO_BRONZE, arquivo)
        with open(caminho_arquivo, 'r') as f:
            dados = json.load(f)
            if "data" in dados:
                df = pd.DataFrame(dados["data"])
                df.rename(columns={"date": "data"}, inplace=True)
                dataframes.append(df)
    if dataframes:
        df_final = pd.concat(dataframes, ignore_index=True)
        engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
        df_final.to_sql('tesouro', engine, if_exists='replace', index=False)
        print("Dados do Tesouro carregados no PostgreSQL com sucesso.")


# Configuração da DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="captura_e_carregamento_dados_financeiros",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    with TaskGroup("captura_dados") as captura_dados:
        for ticker in TICKERS:
            PythonOperator(
                task_id=f"capturar_acao_{ticker}",
                python_callable=captura_ativos,
                op_kwargs={"ticker": ticker}
            )
            PythonOperator(
                task_id=f"capturar_indicadores_{ticker}",
                python_callable=captura_indicadores_tecnicos,
                op_kwargs={"ticker": ticker}
            )

        PythonOperator(
            task_id="capturar_tesouro",
            python_callable=captura_dados_tesouro
        )

    carregar_ativos = PythonOperator(
        task_id="carregar_ativos_postgres",
        python_callable=carrega_ativos_para_postgres
    )

    carregar_indicadores = PythonOperator(
        task_id="carregar_indicadores_postgres",
        python_callable=carrega_indicadores_para_postgres
    )

    carregar_tesouro = PythonOperator(
        task_id="carregar_tesouro_postgres",
        python_callable=carrega_tesouro_para_postgres
    )

    captura_dados >> [carregar_ativos, carregar_indicadores, carregar_tesouro]
