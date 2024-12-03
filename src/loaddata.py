import pyspark.sql.types as T         #Define os tipos nativos do PySpark
import pyspark.sql.functions as F     #Importa as funções nativas do Spark para manipulação dos dados
from pyspark.sql.window import Window #Importa a função utilizada para criação de janelas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,  to_date


#starta o spark
spark = SparkSession.builder \
    .appName("##### Olist Data Processing #####")\
    .getOrCreate()

data_path = "../vendas-de-produtos-pyspark/data/olist/"


# o + "nome do arquivo" é pra selecionar o arquivo dentro do path    (header = primeira linha aonde define oq é cada coisa)
customers_df = spark.read.csv(data_path + "olist_customers_dataset.csv", header=True, inferSchema=True)

#dropa as colunas com mvalores nul
# customers_df.dropna()


customers_df.printSchema()
customers_df.show(5)

print("=-"* 30)
# o + "nome do arquivo" é pra selecionar o arquivo dentro do path    (header = primeira linha aonde define oq é cada coisa)
orders_df = spark.read.csv(data_path + "olist_orders_dataset.csv", header=True, inferSchema=True)
orders_df = orders_df.withColumn("order_date", to_date(col("order_purchase_timestamp")))

orders_df.printSchema()
orders_df.show(5)

# pra renomear só usar esse comando / nome antigo na frente , nome novo
# customers_df = customers_df.withColumnRenamed("customer_id", "id_cliente")
