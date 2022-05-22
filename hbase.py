from pyspark.sql import *
from pyspark.sql import functions as F
import re

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
# File location and type
file_location = "E:\\bigdata\\datasets\\bank_full.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)