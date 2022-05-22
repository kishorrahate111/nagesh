from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import re

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()

data="E:\\bigdata\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferschema","true").load(data)
def off(st):
    if(st=="LA"):
        return "30% off";
    elif(st=="OH"):
        return "50% off";
#function convert to udf
todayof = udf(off, StringType())
spark.udf.register("test",todayof)
res=df.withColumn("todayoffer",todayof(col("state")))
res.show()
df.createOrReplaceTempView("tab")
res1=spark.sql("select * ,test(state) todayoff from tab")
res1.show()