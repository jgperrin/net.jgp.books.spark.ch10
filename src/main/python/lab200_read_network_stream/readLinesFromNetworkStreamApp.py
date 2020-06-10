"""
  Reads a stream from a stream (network)

  @author rambabu.posa
"""
from pyspark.sql import SparkSession
import logging

logging.debug("-> start")

spark = SparkSession.builder \
    .appName("Read lines over a network stream") \
    .master("local[*]").getOrCreate()

df = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

query = df.writeStream.outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

# Executed only after a nice kill
logging.debug("Query status: {}".format(query.status))
logging.debug("<- end")

