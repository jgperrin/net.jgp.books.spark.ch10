"""
  Analyzes the records on the stream and send each record to a debugger class.

  @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging
import time

logging.debug("-> start")

# Creates a session on a local master
spark = SparkSession.builder.appName("Read lines over a file stream") \
    .master("local[*]").getOrCreate()

# The record structure must match the structure of your generated
# record
# (or your real record if you are not using generated records)
recordSchema = StructType([StructField('fname', StringType(), True),
                           StructField('mname', StringType(), True),
                           StructField('lname', StringType(), True),
                           StructField('age', IntegerType(), True),
                           StructField('ssn', StringType(), True)])

# Reading the record is always the same
df = spark.readStream.format("csv") \
    .schema(recordSchema) \
    .csv("/tmp/")
# use for windows
#   .csv("C:/tmp/")

query = df.writeStream.outputMode("append") \
    .format("memory") \
    .option("queryName", "people") \
    .start()

iterationCount = 0
start = int(round(time.time() * 1000))

while query.isActive:
    queryInMemoryDf = spark.sql("SELECT * FROM people")
    iterationCount += 1
    logging.debug("Pass #{}, dataframe contains {} records".format(iterationCount, queryInMemoryDf.count()))

    queryInMemoryDf.show()

    end = int(round(time.time() * 1000))

    if start + 60000 < end:
        query.stop()

    time.sleep(2)

logging.debug("<- end")
