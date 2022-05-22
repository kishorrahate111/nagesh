from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), \
              ("Michael", "Sales", 4600), \
              ("Robert", "Sales", 4100), \
              ("Maria", "Finance", 3000), \
              ("James", "Sales", 3000), \
              ("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), \
              ("Jeff", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000), \
              ("Saif", "Sales", 4100) \
              )

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
df.show(truncate=False)

windowspec=Window.partitionBy("department").orderBy("salary")
windowag=Window.partitionBy("department")
# df1=df.withColumn("row_number",row_number().over(windowspec)) \
#     .withColumn("rank",rank().over(windowspec))\
#     .withColumn("dense",dense_rank().over(windowspec)) \
#     .withColumn("lag",lag("salary",1).over(windowspec))\
#     .withColumn("lead",lead("salary",1).over(windowspec)) \
#     .show()
df.withColumn('row',row_number().over(windowspec))\
    .withColumn("avg",avg(col("salary")).over(windowag)) \
    .withColumn("sum",sum(col("salary")).over(windowag))\
    .where(col("row")==1)\
    .show()

