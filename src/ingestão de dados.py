# Databricks notebook source
#importacao
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura dos arquivos em parquet no bucket s3 ###

# COMMAND ----------

df_all = spark.read.parquet("s3://bucket-taxi-project/yellow_tripdata_2023-05.parquet")
df_all = df_all.withColumn("total_amount", col("total_amount").cast(DoubleType()))
df.display()

# COMMAND ----------

df_ingestion = df_all.select(
    col("VendorID").cast(IntegerType()),
    col("passenger_count").cast(IntegerType()),
    col("total_amount").cast(DoubleType()),
    col("tpep_pickup_datetime").cast(TimestampType()),
    col("tpep_dropoff_datetime").cast(TimestampType())
).na.drop(subset=[
    "VendorID", "passenger_count", "total_amount", 
    "tpep_pickup_datetime", "tpep_dropoff_datetime"
])

df_ingestion.write.format("delta").mode("overwrite").save("s3a://bucket-taxi-project/consumption/yellow-maio")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Registro no catálogo ###
# MAGIC

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS yellow_taxi
    USING DELTA
    LOCATION 's3a://bucket-taxi-project/consumption/yellow-abr'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from yellow_taxi
# MAGIC order by tpep_pickup_datetime desc
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpeza dos dados - Jan a Maio de 2023 ###

# COMMAND ----------

# filtro para pegar o range de data solicitado
start_date = "2023-01-01"
end_date = "2023-05-31 23:59:59"

df_filtered = df_all.filter(
    (col("tpep_pickup_datetime") >= start_date) &
    (col("tpep_pickup_datetime") <= end_date)
)

df_filtered.display()

# salva em delta com a limpeza de dados fora de jan a maio
df_filtered.write.format("delta").mode("overwrite").save("s3a://bucket-taxi-project/consumption/taxi_data/")

