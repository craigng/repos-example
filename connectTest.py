from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

spark = SparkSession.builder.appName('db-connect-demo').getOrCreate()

spark.range(10).show()