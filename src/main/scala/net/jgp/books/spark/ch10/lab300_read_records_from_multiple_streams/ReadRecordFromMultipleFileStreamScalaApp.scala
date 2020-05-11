package net.jgp.books.spark.ch10.lab300_read_records_from_multiple_streams

import net.jgp.books.spark.ch10.x.utils.streaming.lib.StreamingUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * Reads records from multiple streams (Files)
 *
 * @author rambabu.posa
 */
class ReadRecordFromMultipleFileStreamScalaApp {

  private val log = LoggerFactory.getLogger(classOf[ReadRecordFromMultipleFileStreamScalaApp])

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

    // Two directories
    val landingDirectoryStream1 = StreamingUtils.getInputDirectory
    val landingDirectoryStream2 = "/tmp/dir2" // make sure it exists

    // Two streams
    val dfStream1 = spark.readStream
      .format("csv")
      .schema(recordSchema)
      .load(landingDirectoryStream1)

    val dfStream2 = spark.readStream
      .format("csv")
      .schema(recordSchema)
      .load(landingDirectoryStream2)

    // Each stream will be processed by the same writer
    val queryStream1 = dfStream1.writeStream
      .outputMode(OutputMode.Append)
      .foreach(new AgeCheckerScala(1))
      .start

    val queryStream2 = dfStream2.writeStream
      .outputMode(OutputMode.Append)
      .foreach(new AgeCheckerScala(2))
      .start

    // Loop through the records for 1 minute
    val startProcessing = System.currentTimeMillis
    var iterationCount = 0

    while (queryStream1.isActive && queryStream2.isActive) {
      iterationCount += 1
      log.debug("Pass #{}", iterationCount)
      if (startProcessing + 60000 < System.currentTimeMillis) {
        queryStream1.stop()
        queryStream2.stop()
      }
      try {
        Thread.sleep(2000)
      } catch {
        case e: InterruptedException =>
        // Simply ignored
      }
    }

    log.debug("<- start()")

  }

}

object ReadRecordFromMultipleFileStreamScalaApplication {

  def main(args: Array[String]): Unit = {

    val app = new ReadRecordFromMultipleFileStreamScalaApp
    app.start
  }

}
