# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType
from pyspark.sql.functions import col, avg
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE kusha_solutions.default.expectancy SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5')
# MAGIC

# COMMAND ----------

spark = SparkSession.builder.appName('PySpark').getOrCreate()

# COMMAND ----------


dataframe = spark.read.option("header","true").format("csv").load("dbfs:/FileStore/tables/life_expectancy.csv")
display(dataframe.printSchema)
df1=dataframe.withColumnRenamed("Female Life Expectancy","FemaleLifeExpectancy").withColumnRenamed("Male Life Expectancy","MaleLifeExpectancy").withColumnRenamed("Country Code","CountryCode")



# COMMAND ----------


df2=df1.withColumn("FemaleLifeExpectancy",df1.FemaleLifeExpectancy.cast(IntegerType()))
df3= df2.groupBy("Country", "Year","CountryCode").agg(avg("FemaleLifeExpectancy").alias("FemaleLifeExpectancy"), avg("MaleLifeExpectancy").alias("MaleLifeExpectancy"))


df3.write.option(
  "mergeSchema", "true"
).option("delta.columnMapping.mode", "name").option("mergeKey", "Country,Year,CountryCode").saveAsTable(
  "kusha_solutions.default.expectancy",
  mode="overwrite"
)


# COMMAND ----------

updates_inserts = spark.createDataFrame([
    ("Afghanistan","1979","AFG",43.00,40.00),  
    ("Afghanistan","2027","AFG",49.00,50.00)        
], ["Country", "Year", "CountryCode","FemaleLifeExpectancy","MaleLifeExpectancy"])

# COMMAND ----------


deltaTable = DeltaTable.forName(spark,"kusha_solutions.default.expectancy")

deltaTable.alias("events").merge(
    updates_inserts.alias("updates"),
    "events.Country = updates.Country AND events.Year = updates.Year AND events.CountryCode = updates.CountryCode") \
  .whenMatchedUpdate(set = { "FemaleLifeExpectancy" : "updates.FemaleLifeExpectancy" } ) \
  .whenNotMatchedInsert(values =
    {
      "Country": "updates.Country",
      "Year": "updates.Year",
      "CountryCode": "updates.CountryCode",
      "FemaleLifeExpectancy":"updates.FemaleLifeExpectancy",
      "MaleLifeExpectancy":"updates.MaleLifeExpectancy"
    }
  ) \
  .execute()

# COMMAND ----------

deltaTable.toDF().write.option(
  "mergeSchema", "true"
).option("delta.columnMapping.mode", "name").saveAsTable(
  "kusha_solutions.default.expectancy",
  mode="overwrite"
)



# COMMAND ----------

print("Hello")
