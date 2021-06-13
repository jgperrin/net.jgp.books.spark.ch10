package net.jgp.books.spark.ch10.lab910_kafka_sink

import net.jgp.books.spark.ch10.x.utils.streaming.lib.StreamingScalaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * Saves the record in the stream in a parquet file.
 *
 * @author rambabu.posa
 *
 */
class StreamRecordOutputKafkaScalaApp {
  private val log = LoggerFactory.getLogger(classOf[StreamRecordOutputKafkaScalaApp])

  def start(): Unit = {

    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read lines over a file stream")
      .master("local[*]")
      .getOrCreate

    // The record structure must match the structure of your generated
    // record
    // (or your real record if you are not using generated records)
    val recordSchema = new StructType()
      .add("fname", "string")
      .add("mname", "string")
      .add("lname", "string")
      .add("age", "integer")
      .add("ssn", "string")

    // Reading the record is always the same
    val df = spark.readStream
      .format("csv")
      .schema(recordSchema)
      .csv(StreamingScalaUtils.getInputDirectory)

    val query = df.writeStream
      .outputMode(OutputMode.Update)
      .format("kafka") // Format is Apache Kafka
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("topic", "updates")
      .start

    try {
      query.awaitTermination(60000)
    } catch {
      case e: StreamingQueryException =>
        log.error(s"Exception while waiting for query to end ${e.getMessage}.", e)
    }

    log.debug("<- start()")
  }

}

object StreamRecordOutputKafkaScalaApplication {

  def main(args: Array[String]): Unit = {
    val app = new StreamRecordOutputKafkaScalaApp
    app.start
  }

}

