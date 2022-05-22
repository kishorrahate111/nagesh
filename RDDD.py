from pyspark.sql import *
from pyspark.sql import functions as F
import re

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
sc=spark.sparkContext
data="E:\\bigdata\\datasets\\asl.csv"
ardd = sc.textFile(data)
skip=ardd.first() #return first line
res=ardd.filter(lambda x:x!=skip).map(lambda x:x.split(","))\
    .filter(lambda x:int(x[1])>30)
    #.filter(lambda x:"hyd" in x[2])
#select * from tab where age>30 ;
#sel city, count(*) from tab group by city;
for x in res.collect():
    print(x)