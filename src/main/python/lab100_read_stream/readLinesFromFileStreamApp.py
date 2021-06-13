"""
  Reads a stream from a stream (files)

  @author rambabu.posa
"""
from pyspark.sql import SparkSession
import logging

logging.debug("-> start")

# Creates a session on a local master
spark = SparkSession.builder.appName("Read lines from a file stream") \
    .master("local[*]").getOrCreate()


df = spark.readStream.format("text") \
    .load("/tmp/")
# Use below for Windows
# .load("C:/tmp/")

query = df.writeStream.outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 3) \
    .start()

query.awaitTermination()

logging.debug("<- end")
