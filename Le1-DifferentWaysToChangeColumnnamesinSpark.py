# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType,StructType,StructField,TimestampType

# COMMAND ----------

spark = SparkSession.builder.master("local[*]").appName("changeColumnName").getOrCreate()
sc = spark.sparkContext

# COMMAND ----------

df = spark.createDataFrame([("James", 25), ("Robert", 42)],
                                  ["Name", "Age"])

# COMMAND ----------

df.show()

# COMMAND ----------

df1 = df.selectExpr("Name as Firstname1", "Age as YearsLived1")

# COMMAND ----------

df1.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df2 = df.select(col("Name").alias("Firstname2"), col("Age").alias("YearsLived2"))
df2.show()

# COMMAND ----------

df.createOrReplaceTempView("dfTable")

# COMMAND ----------

df3 = spark.sql("SELECT Name as FirstName3, Age as YearsLived3 FROM dfTable")
df3.show()

# COMMAND ----------

df4= df.withColumnRenamed("Name","FirstName4")\
       .withColumnRenamed("Age","YearsLived4")
df4.show()

# COMMAND ----------

df = spark.createDataFrame([("James", 25), ("Robert", 42)],
                                  ["First Name", "Years Lived"])

# COMMAND ----------

newColumn= list(map(lambda x: x.replace(" ", "_"), df.columns))

# COMMAND ----------

df5 = df.toDF(*newColumn)

# COMMAND ----------

df5.show()

# COMMAND ----------

def twice(x):
    return x+x
twice(9)

# COMMAND ----------

double= lambda x: x+x

# COMMAND ----------

double(9)

# COMMAND ----------

from functools import reduce

# COMMAND ----------

#reduce(function, iterable/seq, initializer=None)

# COMMAND ----------

f = lambda a,b: a if (a > b) else b
reduce(f, [7,21,442,402,453],888)

# COMMAND ----------

df = spark.createDataFrame([("James", 25), ("Robert", 42)],
                                  ["Name", "Age"])

# COMMAND ----------

oldColumns = df.schema.names

# COMMAND ----------

newColumns = ["Firstname6", "YearsLived6"]

# COMMAND ----------

df6 = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), df)

# COMMAND ----------

df6.printSchema()

# COMMAND ----------

df6.show()

# COMMAND ----------


