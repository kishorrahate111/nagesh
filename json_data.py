from pyspark.sql import *
from pyspark.sql import functions as F
import re

from pyspark.sql import *
from pyspark.sql.types import ArrayType, StructType
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()


def flattenDataFrame(explodeDF):
    DFSchema = explodeDF.schema
    fields = DFSchema.fields
    #print("fields: ",fields)
    fieldNames = DFSchema.fieldNames()
    #print("field names: ",fieldNames)
    fieldLength = len(fieldNames)

    for i in range(fieldLength):
        field = fields[i]
        fieldName = field.name
        fieldDataType = field.dataType

        if isinstance(fieldDataType, ArrayType):
            fieldNameExcludingArray = list(filter(lambda colName: colName != fieldName, fieldNames))
            fieldNamesAndExplode = fieldNameExcludingArray + [
                "posexplode_outer({0}) as ({1}, {2})".format(fieldName, fieldName + "_pos", fieldName)]
            arrayDF = explodeDF.selectExpr(*fieldNamesAndExplode)
            return flattenDataFrame(arrayDF)

        elif isinstance(fieldDataType, StructType):
            childFieldnames = fieldDataType.names
            structFieldNames = list(map(lambda childname: fieldName + "." + childname, childFieldnames))
            newFieldNames = list(filter(lambda colName: colName != fieldName, fieldNames)) + structFieldNames
            renamedCols = map(lambda x: x.replace(".", "_"), newFieldNames)
            zipAliasColNames = zip(newFieldNames, renamedCols)
            aliasColNames = map(lambda y: col(y[0]).alias(y[1]), zipAliasColNames)
            structDF = explodeDF.select(*aliasColNames)
            return flattenDataFrame(structDF)
    return explodeDF

data="E:\\bigdata\\datasets\\world_bank.json"
df=spark.read.format("json").load(data).drop("_id")
res=flattenDataFrame(df)
res.show()
res.printSchema()