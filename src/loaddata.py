import os
import pyspark.sql.types as T         #Define os tipos nativos do PySpark
import pyspark.sql.functions as F     #Importa as funções nativas do Spark para manipulação dos dados
from pyspark.sql.window import Window #Importa a função utilizada para criação de janelas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,  to_date
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Obter as variáveis do arquivo .env
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

# Criar a URL de conexão do banco de dados
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Criar o engine de conexão com o banco de dados
engine = create_engine(DATABASE_URL)

####################################################################################
#starta o spark
spark = SparkSession.builder \
    .appName("##### Olist Data Processing #####")\
    .getOrCreate()

data_path = "../vendas-de-produtos-pyspark/data/olist/"


# o + "nome do arquivo" é pra selecionar o arquivo dentro do path    (header = primeira linha aonde define oq é cada coisa)
customers_df = spark.read.csv(data_path + "olist_customers_dataset.csv", header=True, inferSchema=True)
orders_df = spark.read.csv(data_path + "olist_orders_dataset.csv", header=True, inferSchema=True)
orders_df = orders_df.withColumn("order_date", to_date(col("order_purchase_timestamp")))
products_df = spark.read.csv(data_path + "olist_products_dataset.csv", header=True, inferSchema=True)
seller_df = spark.read.csv(data_path + "olist_sellers_dataset.csv", header=True, inferSchema=True)
product_cat_name_pt_eng_df = spark.read.csv(data_path + "product_category_name_translation.csv", header=True, inferSchema=True)
payment_df = spark.read.csv(data_path + "olist_order_payments_dataset.csv", header=True, inferSchema=True)

#dropa as colunas com mvalores nul
# customers_df.dropna()
cleaned_orders_df = orders_df.dropna()
cleaned_customers_df = customers_df.dropna()


# pra renomear só usar esse comando / nome antigo na frente , nome novo
# customers_df = customers_df.withColumnRenamed("customer_id", "id_cliente")


# Mostrar as primeiras linhas e esquemas para conferência
#customers_df.printSchema()
#customers_df.show(5)
#print("=-"* 30)

#cleaned_customers_df.printSchema()
#cleaned_customers_df.show(5)
#print("=-"* 30)

#orders_df.printSchema()
#orders_df.show(5)
#print("=-"* 30)

# Converter os DataFrames do PySpark para pandas para salvar no PostgreSQL
customers_pd = customers_df.toPandas()
orders_pd = orders_df.toPandas()
cleaned_customers_df = cleaned_customers_df.toPandas()
cleaned_orders_df = orders_df.toPandas()
products_df = products_df.toPandas()
seller_df = seller_df.toPandas()
product_cat_name_pt_eng_df = product_cat_name_pt_eng_df.toPandas()
payment_df = payment_df.toPandas()



try:
    # Salvar a tabela 'customers' no banco de dados
    customers_pd.to_sql('customers', con=engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    print("Tabela 'customers' salva com sucesso no banco de dados!")

    # Salvar a tabela 'orders' no banco de dados
    orders_pd.to_sql('orders', con=engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    print("Tabela 'orders' salva com sucesso no banco de dados!")

     # Salvar a tabela 'cleaner_customers' no banco de dados
    cleaned_customers_df.to_sql('cleaned_customers', con=engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    print("Tabela 'cleaned_customers' salva com sucesso no banco de dados!")

     # Salvar a tabela 'cleaner_orders' no banco de dados
    cleaned_orders_df.to_sql('cleaned_orders', con=engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    print("Tabela 'cleaned_orders' salva com sucesso no banco de dados!")

      # Salvar a tabela 'products' no banco de dados
    products_df.to_sql('products', con=engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    print("Tabela 'products' salva com sucesso no banco de dados!")

     # Salvar a tabela 'seller' no banco de dados
    seller_df.to_sql('seller', con=engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    print("Tabela 'seller' salva com sucesso no banco de dados!")

    product_cat_name_pt_eng_df.to_sql('product_cat_name_pt_eng', con=engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    print("Tabela 'product_cat_name_pt_eng' salva com sucesso no banco de dados!")

    payment_df.to_sql('order_payment', con=engine, schema=DB_SCHEMA, if_exists='replace', index=False)
    print("Tabela 'order_payment' salva com sucesso no banco de dados!")

except ProgrammingError as e:
    print(f"Erro ao salvar no banco de dados: {e}")