from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
from pyspark.dbutils import DBUtils
from datetime import date
from test_libraries import returnDouble

def returnDouble(x):
    return 2*x

spark = SparkSession.builder.appName('db-connect-demo').getOrCreate()

dbutils = DBUtils(spark)

print(dir(dbutils))

# spark.sparkContext.addPyFile("./test_libraries/test_module.py")
dbutils.library.installPyPI("fbprophet")

# doubleUDF = udf(returnDouble, IntegerType())

# spark.range(10).withColumn("twiceRange", doubleUDF(col("id"))).show()