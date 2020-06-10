"""
  Saves the record in the stream in a parquet file.

  @author rambabu.posa
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
                               StringType, IntegerType)

logging.debug("-> start")

# Creates a session on a local master
spark = SparkSession.builder.appName("Read lines over a file stream") \
    .master("local[*]") \
    .getOrCreate()

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

# File output only supports append
# Format is Apache Parquet: format = parquet
# Output directory: path = /tmp/spark/parquet
# check point: checkpointLocation = /tmp/checkpoint
query = df.writeStream.outputMode("append") \
    .format("parquet")  \
    .option("path", "/tmp/spark/parquet") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()

logging.debug("<- end")