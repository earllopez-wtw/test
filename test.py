# Databricks notebook source
# MAGIC %sql
# MAGIC WITH filtered_elements AS (
# MAGIC   SELECT *
# MAGIC   FROM global_temp.udm_elements
# MAGIC   WHERE SourceColumnType = 'MON'
# MAGIC )
# MAGIC SELECT m.ElementId as Id, m.DestinationColumnName as ElementName, a.DestinationColumnName as ElementCurrency
# MAGIC FROM global_temp.udm_elements a
# MAGIC INNER JOIN filtered_elements m
# MAGIC   ON a.ParentElementId = m.ElementId
# MAGIC WHERE a.RelationshipType = 'C' AND m.SourceColumnType = 'MON'

# COMMAND ----------



# COMMAND ----------

import pandas as pd

# Define the conversion ratio dictionary
currency_conversion = {'AED': 0.27, 'HKD': 0.17, 'EUR': 1.18}

# Define the DataFrame
df = pd.DataFrame({
    'currency': ['AED', 'HKD', 'AED']
})

print("Before:\n", df)

# Add the new column
df['CONVERSIONRATIO'] = df['currency'].map(currency_conversion)

print("\nAfter:\n", df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.format("delta").load("/mnt/silver_1/incumbent")
df = df.select(sorted(df.columns))
df.printSchema()

# COMMAND ----------

# Get the schema of df as a list of StructField objects
schema_list = df.schema

# Convert the list of StructField objects to a list of tuples
schema_tuples = [(field.name, str(field.dataType)) for field in schema_list]

# Convert the list of tuples to a DataFrame
df_schema = spark.createDataFrame(schema_tuples, ['Column_Name', 'Type'])
exclude_values = ["StringType()", "TimestampType()"]

display(df_schema.filter(~df_schema.Type.isin(exclude_values)))

# COMMAND ----------

df_column1 = df.columns
df_column2 = df.columns

df_column1 = spark.createDataFrame([(name,) for name in df_column1], ['Column_Names'])
df_column1 = df_column1.filter(~col('Column_Names').like('%_CURR%'))

df_column2 = spark.createDataFrame([(name,) for name in df_column2], ['Column_Names'])
df_column2 = df_column2.filter(col('Column_Names').like('%_CURR%'))
df_column2 = df_column2.withColumn('Column_Names', regexp_replace('Column_Names', '_CURR', ''))

final = df_column1.subtract(df_column2)

display(final)

# COMMAND ----------

udm = spark.sql("SELECT * FROM global_temp.udm_elements")
udm.printSchema()

# COMMAND ----------

mounts = dbutils.fs.mounts()
mounts_df = spark.createDataFrame(mounts)
display(mounts_df)

# COMMAND ----------

directory = "/mnt/silver_1"
files = dbutils.fs.ls(directory)
for file in files:
    print(file.path)

# COMMAND ----------

print("updates from remote branch")

