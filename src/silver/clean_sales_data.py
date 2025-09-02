# Databricks notebook source
#importacao
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp


# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpeza dos dados ###

# COMMAND ----------

# Lê cada mês separadamente
df_jan = spark.read.format("delta").load("s3://bucket-taxi-project/consumption/yellow_janeiro/")
df_fev = spark.read.format("delta").load("s3://bucket-taxi-project/consumption/yellow_fevereiro/")
df_mar = spark.read.format("delta").load("s3://bucket-taxi-project/consumption/yellow_marco/")
df_abr = spark.read.format("delta").load("s3://bucket-taxi-project/consumption/yellow_abril/")
df_maio = spark.read.format("delta").load("s3://bucket-taxi-project/consumption/yellow_maio/")

# Faz o union de todos
df_all = df_jan.unionByName(df_fev)\
               .unionByName(df_mar)\
               .unionByName(df_abr)\
               .unionByName(df_maio)

df_all.createOrReplaceTempView("df_all")

# COMMAND ----------

# Realiza a limpeza excluindo a qtd de passageiros que não sejam validos
df_clean = df_all.filter(col("passenger_count") > 0)


# COMMAND ----------

# Normaliza as colunas de data e realiza a limpeza eliminando o que for Null
df_clean = df_clean.withColumn("pickup_ts", to_timestamp("tpep_pickup_datetime")) \
                   .withColumn("dropoff_ts", to_timestamp("tpep_dropoff_datetime")) \
                   .filter(col("pickup_ts").isNotNull() & col("dropoff_ts").isNotNull())
df_clean.createOrReplaceTempView("df_clean")

# COMMAND ----------

# Salva a tabela em delta no bucket s3
query_ingestao = spark.sql("""
                           select 
                           VendorID AS id,
                           pickup_ts,
                           dropoff_ts,
                           passenger_count,
                           total_amount
                           from df_clean
                           where pickup_ts between '2023-01-01 00:00:00' and '2023-05-31 23:59:59' """)

query_ingestao.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("s3a://bucket-taxi-project/consumption/taxi")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Registro no catálogo ###
# MAGIC

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS taxy_yellow
    USING DELTA
    LOCATION 's3a://bucket-taxi-project/consumption/taxi'
""")
