package net.jgp.books.spark.ch10.lab920_for_each_sink

import net.jgp.books.spark.ch10.x.utils.streaming.lib.StreamingScalaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * Analyzes the records on the stream and send each record to a debugger
 * class.
 *
 * @author rambabu.posa
 *
 */
class StreamRecordThroughForEachScalaApp {

  private val log = LoggerFactory.getLogger(classOf[StreamRecordThroughForEachScalaApp])

  def start(): Unit = {

    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read lines over a file stream")
      .master("local[*]")
      .getOrCreate

    val recordSchema = new StructType()
      .add("fname", "string")
      .add("mname", "string")
      .add("lname", "string")
      .add("age", "integer")
      .add("ssn", "string")

    val df = spark.readStream
      .format("csv")
      .schema(recordSchema)
      .csv(StreamingScalaUtils.getInputDirectory)

    val query = df.writeStream
      .outputMode(OutputMode.Update)
      .foreach(new RecordLogScalaDebugger)
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

object StreamRecordThroughForEachScalaApplication {

  def main(args: Array[String]): Unit = {
    val app = new StreamRecordThroughForEachScalaApp
    app.start
  }

}
