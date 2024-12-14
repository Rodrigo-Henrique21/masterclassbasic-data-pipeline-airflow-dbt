import pytest
import os
from dags.captura_dados import captura_ativos, captura_dados_tesouro, captura_indicadores_tecnicos

# Teste para verificar se a função captura_ativos não levanta exceções
def test_captura_ativos():
    TICKERS = ["AAPL"]  # Teste com um único ticker
    try:
        captura_ativos(TICKERS)
    except Exception as e:
        pytest.fail(f"Falhou com a exceção: {e}")

# Teste para verificar captura_dados_tesouro
def test_captura_dados_tesouro():
    try:
        captura_dados_tesouro()
    except Exception as e:
        pytest.fail(f"Falhou com a exceção: {e}")

# Teste para captura_indicadores_tecnicos
def test_captura_indicadores_tecnicos():
    INDICADORES = ["EMA"]
    TICKERS = ["AAPL"]
    try:
        captura_indicadores_tecnicos(INDICADORES, TICKERS)
    except Exception as e:
        pytest.fail(f"Falhou com a exceção: {e}")
