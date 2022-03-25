# Databricks notebook source
import os
import tarfile
import urllib
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

tgz_path = "/dbfs/FileStore/tables/housing.tgz"
housing_tgz = tarfile.open(tgz_path)
housing_tgz.extractall(path="/dbfs/FileStore/tables/")
housing_tgz.close()

# COMMAND ----------

csvFile = "/FileStore/tables/housing.csv"
rawdf = (spark.read
         .csv(csvFile, header=True, inferSchema=True)
)

# COMMAND ----------

#rawdf.printSchema()
display(rawdf)

# COMMAND ----------

rawdf.summary().show()

# COMMAND ----------

from pyspark.sql.functions import *

rawdf.select(countDistinct("ocean_proximity")).show()
rawdf.distinct().count()


# COMMAND ----------

rawdf.groupBy('ocean_proximity').count().orderBy('count').show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Creating a Test Set
# MAGIC 
# MAGIC ### Stratisfied Shuffle Split

# COMMAND ----------

from pyspark.ml.feature import Bucketizer

bucketizer = Bucketizer(splits=[ 0, 1.5, 3, 4.5, 6, float('Inf') ],inputCol="median_income", outputCol="buckets")
df_buck = bucketizer.setHandleInvalid("keep").transform(rawdf)

display(df_buck)

# COMMAND ----------

display(df_buck.groupBy('buckets').count())

# COMMAND ----------

train = df_buck.sampleBy('buckets', fractions = {0: 0.7, 1: 0.7, 2: 0.7, 3: 0.7, 4: 0.7})
test = df_buck.subtract(train)

# COMMAND ----------

# display(train.groupBy('buckets').count())
# display(test.groupBy('buckets').count())
