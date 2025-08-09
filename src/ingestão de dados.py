# Databricks notebook source
#importacao
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp


# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura dos arquivos em parquet no bucket s3 ###

# COMMAND ----------

df_maio = spark.read.parquet("s3://bucket-taxi-project/raw/yellow_tripdata_2023-05.parquet")

df_maio = df_maio.select(
    col("VendorID").cast(IntegerType()),
    col("passenger_count").cast(IntegerType()),
    col("total_amount").cast(DoubleType()),
    col("tpep_pickup_datetime").cast(TimestampType()),
    col("tpep_dropoff_datetime").cast(TimestampType())
).na.drop(subset=["VendorID", "passenger_count", "total_amount", 
                  "tpep_pickup_datetime", "tpep_dropoff_datetime"])

df_maio.write.format("delta").mode("overwrite").save("s3a://bucket-taxi-project/consumption/yellow_maio")


# COMMAND ----------

df_abril = spark.read.parquet("s3://bucket-taxi-project/raw/yellow_tripdata_2023-04.parquet")

df_abril = df_abril.select(
    col("VendorID").cast(IntegerType()),
    col("passenger_count").cast(IntegerType()),
    col("total_amount").cast(DoubleType()),
    col("tpep_pickup_datetime").cast(TimestampType()),
    col("tpep_dropoff_datetime").cast(TimestampType())
).na.drop(subset=["VendorID", "passenger_count", "total_amount", 
                  "tpep_pickup_datetime", "tpep_dropoff_datetime"])
df_abril.write.format("delta").mode("overwrite").save("s3a://bucket-taxi-project/consumption/yellow_abril")

# COMMAND ----------

df_marco = spark.read.parquet("s3://bucket-taxi-project/raw/yellow_tripdata_2023-03.parquet")

df_marco = df_marco.select(
    col("VendorID").cast(IntegerType()),
    col("passenger_count").cast(IntegerType()),
    col("total_amount").cast(DoubleType()),
    col("tpep_pickup_datetime").cast(TimestampType()),
    col("tpep_dropoff_datetime").cast(TimestampType())
).na.drop(subset=["VendorID", "passenger_count", "total_amount", 
                  "tpep_pickup_datetime", "tpep_dropoff_datetime"])
                  
df_marco.write.format("delta").mode("overwrite").save("s3a://bucket-taxi-project/consumption/yellow_marco")

# COMMAND ----------

df_fevereiro = spark.read.parquet("s3://bucket-taxi-project/raw/yellow_tripdata_2023-02.parquet")

df_fevereiro = df_fevereiro.select(
    col("VendorID").cast(IntegerType()),
    col("passenger_count").cast(IntegerType()),
    col("total_amount").cast(DoubleType()),
    col("tpep_pickup_datetime").cast(TimestampType()),
    col("tpep_dropoff_datetime").cast(TimestampType())
).na.drop(subset=["VendorID", "passenger_count", "total_amount", 
                  "tpep_pickup_datetime", "tpep_dropoff_datetime"])

df_fevereiro.write.format("delta").mode("overwrite").save("s3a://bucket-taxi-project/consumption/yellow_fevereiro")


# COMMAND ----------

df_janeiro = spark.read.parquet("s3://bucket-taxi-project/raw/yellow_tripdata_2023-01.parquet")

df_janeiro = df_janeiro.select(
    col("VendorID").cast(IntegerType()),
    col("passenger_count").cast(IntegerType()),
    col("total_amount").cast(DoubleType()),
    col("tpep_pickup_datetime").cast(TimestampType()),
    col("tpep_dropoff_datetime").cast(TimestampType())
).na.drop(subset=["VendorID", "passenger_count", "total_amount", 
                  "tpep_pickup_datetime", "tpep_dropoff_datetime"])

df_janeiro.write.format("delta").mode("overwrite").save("s3a://bucket-taxi-project/consumption/yellow_janeiro")

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
