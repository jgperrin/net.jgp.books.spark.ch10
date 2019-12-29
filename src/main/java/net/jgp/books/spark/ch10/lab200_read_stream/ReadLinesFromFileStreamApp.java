package net.jgp.books.spark.ch10.lab200_read_stream;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch10.x.utils.streaming.lib.StreamingUtils;

/**
 * Reads a stream from a stream (files) and
 * 
 * @author jgp
 */
public class ReadLinesFromFileStreamApp {
  private static Logger log = LoggerFactory
      .getLogger(ReadLinesFromFileStreamApp.class);

  public static void main(String[] args) {
    ReadLinesFromFileStreamApp app = new ReadLinesFromFileStreamApp();
    try {
      app.start();
    } catch (TimeoutException e) {
      log.error("A timeout exception has occured: {}", e.getMessage());
    }
  }

  private void start() throws TimeoutException {
    log.debug("-> start()");

    SparkSession spark = SparkSession.builder()
        .appName("Read lines from a file stream")
        .master("local")
        .getOrCreate();

    Dataset<Row> df = spark
        .readStream()
        .format("text")
        .load(StreamingUtils.getInputDirectory());

    StreamingQuery query = df
        .writeStream()
        .outputMode(OutputMode.Append())
        .format("console")
        .option("truncate", false)
        .option("numRows", 3)
        .start();

    try {
      query.awaitTermination(60000); // the query will stop in a minute
    } catch (StreamingQueryException e) {
      log.error(
          "Exception while waiting for query to end {}.",
          e.getMessage(),
          e);
    }

    log.debug("<- start()");
  }
}
