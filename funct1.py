from pyspark.sql import *
from pyspark.sql import functions as F
import re
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
data="E:\\bigdata\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferschema","true").load(data)

res=df.withColumn("age",lit(18))\
    .withColumn("phone1",regexp_replace(col("phone1"),"-",""))\
    .withColumn("rangetest", when(col("zip")<46000,"small").when((col("zip")>=46000) & ((col("zip")<90000)),"med").otherwise(lit("big")))\
    .withColumn("fullname",concat("first_name",lit(" "),"last_name"))\
    .withColumn('full',concat_ws(' ',"first_name","last_name","city",'state'))\
    .withColumn("id",monotonically_increasing_id()+1)\
    .withColumn("avg",col("zip")/col("id"))\
    .withColumn("ciel",ceil("avg"))\
    .withColumn("floor",floor("avg"))\
    .withColumn("round",(round(("avg"))))\
    .withColumn('emailonly',substring("email",0,5))\
    .withColumn("email1",substring_index("email","@",-1))
res1=res.groupBy(col("email1")).count().orderBy(col("count").asc())
res1.show()



# res=df.groupBy(col("city")).agg(count("*"),collect_list("first_name"))
res.show(truncate=False)
res.printSchema()
# with column if no column exists add one new column
#used to add one extra column with something values
#withcolumn if already exits...update that column based on that logic
 #lit will allow to enter the dummy values
 #collect_list--->will show duplicate record also
 #collect_set ___> will show only distinct record
 #ciel values--->will give round off values next
 #floor values-->will give round off values previous
 #round values---> will give round figure values