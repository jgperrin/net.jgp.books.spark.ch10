"""
  Saves the record in the stream in a parquet file.

  @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging

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

# // Format is Apache Kafka
query = df.writeStream.outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
    .option("topic", "updates") \
    .start()

query.awaitTermination()

spark.stop()

logging.debug("<- end")