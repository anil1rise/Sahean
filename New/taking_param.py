# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType
from pyspark.sql.functions import col, avg
from delta.tables import DeltaTable

# COMMAND ----------

spark = SparkSession.builder.appName('PySpark').getOrCreate()

# COMMAND ----------

dbutils.widgets.text(name="param",defaultValue='',label='heading')

# COMMAND ----------

print(dbutils.widgets.get('param'))
df1 = spark.sql("SELECT * FROM kusha_solutions.default.expectancy WHERE Country ='Argentina'")
df1.show()

# COMMAND ----------

df = spark.sql("SELECT * FROM kusha_solutions.default.expectancy WHERE Country = '" + dbutils.widgets.get('param') + "'")
df.show(5)

# COMMAND ----------


