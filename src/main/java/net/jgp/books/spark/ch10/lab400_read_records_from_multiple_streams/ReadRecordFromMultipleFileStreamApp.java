package net.jgp.books.spark.ch10.lab400_read_records_from_multiple_streams;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch10.x.utils.streaming.lib.StreamingUtils;

public class ReadRecordFromMultipleFileStreamApp {
  private static transient Logger log = LoggerFactory.getLogger(
      ReadRecordFromMultipleFileStreamApp.class);

  public static void main(String[] args) {
    ReadRecordFromMultipleFileStreamApp app =
        new ReadRecordFromMultipleFileStreamApp();
    try {
      app.start();
    } catch (TimeoutException e) {
      log.error("A timeout exception has occured: {}", e.getMessage());
    }
  }

  private void start() throws TimeoutException {
    log.debug("-> start()");

    SparkSession spark = SparkSession.builder()
        .appName("Read lines over a file stream")
        .master("local")
        .getOrCreate();

    StructType recordSchema = new StructType()
        .add("fname", "string")
        .add("mname", "string")
        .add("lname", "string")
        .add("age", "integer")
        .add("ssn", "string");

    // Two directories
    String landingDirectoryStream1 = StreamingUtils.getInputDirectory();
    String landingDirectoryStream2 = "/tmp/dir2"; // make sure it exists

    // Two streams
    Dataset<Row> dfStream1 = spark
        .readStream()
        .format("csv")
        .schema(recordSchema)
        .load(landingDirectoryStream1);

    Dataset<Row> dfStream2 = spark
        .readStream()
        .format("csv")
        .schema(recordSchema)
        .load(landingDirectoryStream2);

    // Each stream will be processed by the same writer
    StreamingQuery queryStream1 = dfStream1
        .writeStream()
        .outputMode(OutputMode.Append())
        .foreach(new AgeChecker(1))
        .start();

    StreamingQuery queryStream2 = dfStream2
        .writeStream()
        .outputMode(OutputMode.Append())
        .foreach(new AgeChecker(2))
        .start();

    // Loop through the records for 1 minute
    long startProcessing = System.currentTimeMillis();
    int iterationCount = 0;
    while (queryStream1.isActive() && queryStream2.isActive()) {
      iterationCount++;
      log.debug("Pass #{}", iterationCount);
      if (startProcessing + 60000 < System.currentTimeMillis()) {
        queryStream1.stop();
        queryStream2.stop();
      }
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        // Simply ignored
      }
    }
    log.debug("<- start()");
  }
}
