package net.jgp.books.sparkInAction.ch10.lab300ReadRecordsFromMultipleStreams;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.sparkInAction.ch10.x.utils.streaming.lib.StreamingUtils;

public class ReadRecordFromFileStreamApp {
  private static transient Logger log = LoggerFactory.getLogger(
      ReadRecordFromFileStreamApp.class);

  public static void main(String[] args) {
    ReadRecordFromFileStreamApp app = new ReadRecordFromFileStreamApp();
    app.start();
  }

  private void start() {
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

    Dataset<Row> df = spark
        .readStream()
        .format("csv")
        .schema(recordSchema)
        .csv(StreamingUtils.getInputDirectory());

    StreamingQuery query = df
        .writeStream()
        .outputMode(OutputMode.Update())
        .format("console")
        .start();

//    stopQuery(query, 5000);
//    Thread t = new Thread(() -> {
//      try {
//        Thread.sleep(5000);
//      } catch (InterruptedException e) {
//        // ignored
//      }
//      log.debug("Will stop the query now");
//      query.stop();
//      log.debug("Query has been stopped");
//      try {
//        Thread.sleep(5000);
//      } catch (InterruptedException e) {
//        // ignored
//      }
//      log.debug("Thread coming to an end");
//    });
//    t.start();
    
    try {
      query.awaitTermination(5000);
    } catch (StreamingQueryException e) {
      log.error(
          "Exception while waiting for query to end {}.",
          e.getMessage(),
          e);
    }

    // Executed only after a nice kill
    log.debug("Query status: {}", query.status());
    df.show();
    df.printSchema();
    log.debug("The dataframe contains {} record(s).", df.count());
  }
}
