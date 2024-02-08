# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType
from pyspark.sql.functions import col, avg

# COMMAND ----------

spark = SparkSession.builder.appName('PySpark').getOrCreate()

# COMMAND ----------

dataframe = spark.read.format("csv").option("header", "true").load("/Volumes/kusha_solutions/default/life-exp/life_expectancy.csv")

# COMMAND ----------



# COMMAND ----------

df1=dataframe.withColumnRenamed("Female Life Expectancy","FemaleLifeExpectancy").withColumnRenamed("Male Life Expectancy","MaleLifeExpectancy")
df2=df1.withColumn("FemaleLifeExpectancy",df1.FemaleLifeExpectancy.cast(IntegerType()))
df3= df2.groupBy("Country","Year").agg(avg("FemaleLifeExpectancy").alias("FemaleLifeExpectancy"), avg("MaleLifeExpectancy").alias("MaleLifeExpectancy"))







# COMMAND ----------

df3.show(10)

# COMMAND ----------

df3.write.option(
  "mergeSchema", "true"
).saveAsTable(
  "kusha_solutions.default.expectancy",
  mode="overwrite"
)

# COMMAND ----------

# MAGIC
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %sh whoami
# MAGIC

# COMMAND ----------


