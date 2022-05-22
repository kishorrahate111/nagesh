from pyspark.sql import *
from pyspark.sql.functions import lit,desc,asc, substring,substring_index, floor, ceil, round, collect_list, regexp_replace,col ,max, when, count
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
data = "E:\\bigdata\\datasets\\us-500.csv"
df = spark.read.format("csv").option("inferSchema","true").option("header", "true").load(data)
res=df.withColumn("age",lit(18)).withColumn("phone1", regexp_replace(col("phone1"),"-","") )\
    .withColumn("rangetest", when(col("zip")<46000,"small").when((col("zip")>=46000) & ((col("zip")<90000)),"med").otherwise(lit("big")))\
    .withColumn("email1", regexp_replace(col("email"),"gmail","googlemail"))\
    .withColumn("email2", when(col("email").contains("gmail"),"googlemail").otherwise(col("email")))\
    .withColumn("fullname", concat(col("first_name"), lit(" "), col("last_name"), lit(" "), col("state")))\
    .withColumn("full", concat_ws("_", col("first_name"),col("last_name"),col("state")))\
    .withColumn("id",monotonically_increasing_id()+1)\
    .withColumn("avg",col("zip")/col("id"))\
    .withColumn("ceil",ceil(col("avg")))\
    .withColumn("floor",floor(col("avg")))\
    .withColumn("round",round(col("avg")))\
    .withColumn("emailonly",substring(col("email"),1,5))\
    .withColumn("onlyemail", substring_index(col("email"),'@',-1))
res1=res.groupBy(col("onlyemail")).count().orderBy(col("count").desc())

#res=df.groupBy(col("state")).agg(count("*"),collect_list("first_name"))



res.show(truncate=False)
res.printSchema()
res1.show()

#withColumn If no column exists add one new column with something values..
#withColumn if column already exists .. update that column based on logic
# when .. replace everyting in that column but regexreplace just replace that patren
#monotically increase id .. auto add 0,1,2,3,,4,5, like that unique number