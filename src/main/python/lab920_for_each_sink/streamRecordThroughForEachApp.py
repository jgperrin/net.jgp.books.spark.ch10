"""
  Analyzes the records on the stream and send each record to a debugger class.

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


class RecordLogDebugger:
    def open(self, partition_id, epoch_id):
        # Open connection. This method is optional in Python.
        True

    def process(self, row):
        # Write row to connection. This method is NOT optional in Python.
        logging.info('processing {}'.format(row))

    def close(self, error):
        # Close the connection. This method in optional in Python.
        pass


query = df.writeStream.outputMode("update") \
    .foreach(RecordLogDebugger()) \
    .start()

query.awaitTermination()

spark.stop()

logging.debug("<- end")