package net.jgp.books.spark.ch10.lab210_read_record_from_stream;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch10.x.utils.streaming.lib.StreamingUtils;

public class ReadRecordFromFileStreamApp {
  private static Logger log =
      LoggerFactory.getLogger(ReadRecordFromFileStreamApp.class);

  public static void main(String[] args) {
    ReadRecordFromFileStreamApp app = new ReadRecordFromFileStreamApp();
    try {
      app.start();
    } catch (TimeoutException e) {
      log.error("A timeout exception has occured: {}", e.getMessage());
    }
  }

  private void start() throws TimeoutException {
    log.debug("-> start()");

    SparkSession spark = SparkSession.builder()
        .appName("Read records from a file stream")
        .master("local")
        .getOrCreate();

    // Specify the record that will be ingested.
    // Note that the schema much match the record coming from the generator
    // (or source)
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
        .load(StreamingUtils.getInputDirectory());

    StreamingQuery query = df
        .writeStream()
        .outputMode(OutputMode.Append())
        .format("console")
        .start();

    try {
      query.awaitTermination(60000);
    } catch (StreamingQueryException e) {
      log.error(
          "Exception while waiting for query to end {}.",
          e.getMessage(),
          e);
    }

    log.debug("<- start()");
  }
}
