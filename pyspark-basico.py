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


df_limitando = df.limit(20) #limita o dataframe do pyspark para 10 linhas

limitado1 = df_limitando.toPandas() #converte o df limitado pra pandas   (só funciona na 3.11 ou menor)

customers = dl = spark.read.option("header", True).csv(file_path)

customers.printSchema()


#st.dataframe(limitado1)  # imprime o DF no streamlits local

#st.dataframe(customers.select("customer_id", "customer_state").limit(10)) # faz um select e imprime em baixo do de cima
#st.dataframe(customers.select(F.countDistinct(F.col("customer_id")).alias("count_distinct_cutomer_id")).limit(10)) # 
#st.dataframe(customers.filter(F.col("customer_state") == "SP").limit(10))



################################################################################################

file_path2 = os.path.expanduser("~/workshop/vendas-de-produtos-pyspark/tables/olist_orders_dataset.csv")


orders = spark.read.option("header", True).csv(file_path2)


#st.dataframe(orders.withColumn("days_between_purchase_and_approval",F.datediff(F.col("order_approved_at"),F.col("order_purchase_timestamp"))))


orders_limited = orders.withColumn("days_between_purchase_and_approval",F.datediff(F.col("order_approved_at"),F.col("order_purchase_timestamp"))).limit(10)

st.dataframe(orders_limited)



#df1 = df.select("customer_id", "customer_state")
#print(df1)

#n_rows = customers.count()

#print(f"number od rows in this dataframe: {n_rows}")

