from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
from pyspark.dbutils import DBUtils
from datetime import date

def returnDouble(x):
    return 2*x

print(returnDouble(10))

spark = SparkSession.builder.appName('db-connect-demo').getOrCreate()

doubleUDF = udf(returnDouble, IntegerType())

spark.range(10).withColumn("twiceRange", doubleUDF(col("id"))).show()

# this is a comment