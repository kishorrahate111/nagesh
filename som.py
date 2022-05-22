from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("testing").\
    config('spark.driver.extraClassPath',"E:\\Kishor\\SPARK\\driver\\drivers-20220224T032640Z-001\\*").\
    config('spark.executor.extraClassPath',"E:\\Kishor\\SPARK\\driver\\drivers-20220224T032640Z-001\\*").getOrCreate()
#pandas_df = pd.read_csv('D:\\bigdata\\spark_project\\use.csv', names = ['tblnm','full_loadtype'])
s_df = spark.read.format("csv").option("header","true").load("E:\\Kishor\\SparkSnowflake\\spark_project\\input.csv")
s_df.show()
for dfnew in s_df.rdd.collect():
    file_name = dfnew.tblnm+".sql.txt"
    print(file_name)
    tgtTblora = open("E:\\Kishor\\SparkSnowflake\\spark_project\\sql\\" + file_name, "r").read()
    print(tgtTblora)
    # ora_url = "jdbc:oracle:thin:@//oradb.crrnweauhzur.ap-south-1.rds.amazonaws.com:1521/ORCL"
    ora_url = "jdbc:oracle:thin:@//sanjayoracle.cq8iqbvz0eol.us-east-2.rds.amazonaws.com:1521/ORCL"
    # ora_tmp = spark.read.format("jdbc").option("url", ora_url).option("user", "orauser").option\
    #     ("password","orapassword").option("dbtable", tgtTblora).option("driver", "oracle.jdbc.OracleDriver").load()
    ora_tmp = spark.read.format("jdbc").option("url", ora_url).option("user", "ouser").option \
        ("password", "opassword").option("dbtable", tgtTblora).option("driver", "oracle.jdbc.OracleDriver").load()
    ora_tmp.show()
    ora_tmp.write.format("csv").save("E:\\Kishor\\SparkSnowflake\\spark_project\\output\\" +dfnew.tblnm+'\\oracle_extract_'+dfnew.tblnm+'.csv')
    # ora_tmp.toPandas().to_csv('D:\\bigdata\\spark_project\\output2\\'+dfnew.tblnm+'\\oracle_extract_'+dfnew.tblnm+'.csv')