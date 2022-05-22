from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("testing").\
    config('spark.driver.extraClassPath',"E:\\Kishor\\SparkSnowflake\\spark_project\\drivers\\*").\
    config('spark.executor.extraClassPath',"E:\\Kishor\\SparkSnowflake\\spark_project\\drivers\\*").getOrCreate()
#pandas_df = pd.read_csv('D:\\bigdata\\spark_project\\use.csv', names = ['tblnm','full_loadtype'])
s_df = spark.read.format("csv").option("header","true").load("E:\\Kishor\\SparkSnowflake\\spark_project\\input.csv")
s_df.show()
for dfnew in s_df.rdd.collect():
    file_name = dfnew.tblnm+".sql.txt"
    tgtTblora = open("E:\\Kishor\\SparkSnowflake\\spark_project\\sql\\" + file_name, "r").read()
    # ora_url = "jdbc:oracle:thin:@//oradb.crrnweauhzur.ap-south-1.rds.amazonaws.com:1521/ORCL"
    ora_url = "jdbc:oracle:thin:@//oracle.cawztfuy7zzp.ap-south-1.rds.amazonaws.com:1521/orcl"
    # ora_tmp = spark.read.format("jdbc").option("url", ora_url).option("user", "orauser").option\
    #     ("password","orapassword").option("dbtable", tgtTblora).option("driver", "oracle.jdbc.OracleDriver").load()
    ora_tmp = spark.read.format("jdbc").option("url", ora_url).option("user", "ousername").option \
        ("password", "opassword").option("dbtable", tgtTblora).option("driver", "oracle.jdbc.OracleDriver").load()
    ora_tmp.show()
    ora_tmp.write.format("csv").save('E:\\Kishor\\SparkSnowflake\\spark_project\\outputnew\\'+dfnew.tblnm+'\\oracle_extract_'+dfnew.tblnm+'.csv')
    # ora_tmp.toPandas().to_csv('D:\\bigdata\\spark_project\\output2\\'+dfnew.tblnm+'\\oracle_extract_'+dfnew.tblnm+'.csv')