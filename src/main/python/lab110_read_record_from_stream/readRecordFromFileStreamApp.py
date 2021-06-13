"""
  Reads records from a stream (files)

  @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging

logging.debug("-> start")

# Creates a session on a local master
spark = SparkSession.builder.appName("Read records from a file stream") \
    .master("local[*]").getOrCreate()

# Specify the record that will be ingested.
# Note that the schema much match the record coming from the generator
# (or source)
recordSchema = StructType([StructField('fname', StringType(), True),
                           StructField('mname', StringType(), True),
                           StructField('lname', StringType(), True),
                           StructField('age', IntegerType(), True),
                           StructField('ssn', StringType(), True)])

df = spark.readStream.format("csv") \
                     .schema(recordSchema) \
                     .load("/tmp/")
# Use below for Windows
# .load("C:/tmp/")

query = df.writeStream.outputMode("append") \
                      .format("console") \
                      .start()

query.awaitTermination()

logging.debug("<- end")