FROM python:3.10.12

# diretorio de trabalho
WORKDIR /src

# instalando poetry
RUN pip install --no-cache-dir poetry

# Ccopiando arquivos do pyproject do poetry pra isntalar
COPY pyproject.toml poetry.lock ./


#aqui ele roda 
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry install --only main

# copiando o resto dos arquivos
COPY ./tables /src/tables
COPY pyspark-basico.py /src/

#expondo pra porta 8501
EXPOSE 8501




