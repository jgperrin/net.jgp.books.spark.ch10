package net.jgp.books.spark.ch10.lab110_read_record_from_stream

import net.jgp.books.spark.ch10.x.utils.streaming.lib.StreamingScalaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * Reads records from a stream (files)
 *
 * @author rambabu.posa
 */
class ReadRecordFromFileStreamScalaApp {

  private val log = LoggerFactory.getLogger(classOf[ReadRecordFromFileStreamScalaApp])

  def start(): Unit = {

    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read records from a file stream")
      .master("local")
      .getOrCreate

    // Specify the record that will be ingested.
    // Note that the schema much match the record coming from the generator
    // (or
    // source)
    val recordSchema = new StructType()
      .add("fname", "string")
      .add("mname", "string")
      .add("lname", "string")
      .add("age", "integer")
      .add("ssn", "string")

    val df = spark.readStream
      .format("csv")
      .schema(recordSchema)
      .load(StreamingScalaUtils.getInputDirectory)

    val query = df.writeStream
      .outputMode(OutputMode.Update)
      .format("console")
      .start

    try {
      query.awaitTermination(60000)
    } catch {
      case e: StreamingQueryException =>
        log.error(s"Exception while waiting for query to end ${e.getMessage}.")
    }

    log.debug("<- start()")
  }

}

object ReadRecordFromFileStreamScalaApplication {
  def main(args: Array[String]): Unit = {
    val app = new ReadRecordFromFileStreamScalaApp
    app.start
  }
}

