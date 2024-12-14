FROM quay.io/astronomer/astro-runtime:6.0.0

# Alterar para root temporariamente para instalar o git
USER root
RUN apt-get update && apt-get install -y git && apt-get clean

# Voltar para o usuário padrão
USER astro

# Copiando o arquivo de dependências do Python
COPY requirements.txt /tmp/requirements.txt

# Copiando parte de CD - testes 
COPY pytest.ini /usr/local/airflow/

# Copiando parte de CD - testes 
COPY tests /usr/local/airflow/tests

# Instalando dependências Python
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copiando o restante do código do projeto para o contêiner
COPY . /usr/local/airflow/

# Configurando o diretório de trabalho
WORKDIR /usr/local/airflow
