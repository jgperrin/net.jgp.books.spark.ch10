package net.jgp.books.spark.ch10.lab100_read_stream

import net.jgp.books.spark.ch10.x.utils.streaming.lib.StreamingScalaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.slf4j.LoggerFactory

/**
 * Reads a stream from a stream (files)
 *
 * @author rambabu.posa
 */
class ReadLinesFromFileStreamScalaApp {
  private val log = LoggerFactory.getLogger(classOf[ReadLinesFromFileStreamScalaApp])

  def start(): Unit = {
    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read lines from a file stream")
      .master("local[*]")
      .getOrCreate

    val df = spark.readStream.format("text")
      .load(StreamingScalaUtils.getInputDirectory)

    val query = df.writeStream
      .outputMode(OutputMode.Update)
      .format("console")
      .option("truncate", false)
      .option("numRows", 3).start

    try {
      // the query will stop in a minute
      query.awaitTermination(60000)
    } catch {
      case e: StreamingQueryException =>
        log.error("Exception while waiting for query to end {}.", e.getMessage)
    }

    log.debug("<- start()")

  }

}

object ReadLinesFromFileStreamScalaApplication {

  def main(args: Array[String]): Unit = {

    val app = new ReadLinesFromFileStreamScalaApp
    app.start

  }

}
