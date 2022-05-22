from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()

df =spark.read.format("org.apache.spark.sql.cassandra").option("table","emp").option("keyspace","january").load()
df.show()
