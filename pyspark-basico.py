import pyspark.sql.types as T         #Define os tipos nativos do PySpark
import pyspark.sql.functions as F     #Importa as funções nativas do Spark para manipulação dos dados
from pyspark.sql.window import Window #Importa a função utilizada para criação de janelas
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os
import pandas as pd
import streamlit as st
import numpy as pd


# Inicializando a SparkSession
spark = SparkSession.builder .appName("VendasApp").getOrCreate()

# aturozia Arrow-based transferencia de dados
spark.conf.set("Spark.sql.execution.arrow.pyspark.enabled", "True")


# Caminho do arquivo com o caractere ~ para expandir o diretório home
file_path = os.path.expanduser("~/workshop/vendas-de-produtos-pyspark/tables/olist_customers_dataset.csv")

# Lendo o arquivo CSV
df = spark.read.option("header", True).csv(file_path)

# Exibindo os primeiros registros
# df.show()


df_limitando = df.limit(10) #limita o dataframe do pyspark para 10 linhas

limitado1 = df_limitando.toPandas() #converte o df limitado pra pandas


# rows = df.limit(10).collect() #coleta os dados como uma lista de Row


st.dataframe(limitado1)



