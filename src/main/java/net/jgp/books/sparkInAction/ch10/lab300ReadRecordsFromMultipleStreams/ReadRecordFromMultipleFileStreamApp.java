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

public class ReadRecordFromMultipleFileStreamApp {
  private static transient Logger log = LoggerFactory.getLogger(
      ReadRecordFromMultipleFileStreamApp.class);

  public static void main(String[] args) {
    ReadRecordFromMultipleFileStreamApp app =
        new ReadRecordFromMultipleFileStreamApp();
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

    String landingDirectoryStream1 = StreamingUtils.getInputDirectory();
    String landingDirectoryStream2 = "/tmp/dir2"; // make sure it exists

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

    StreamingQuery queryStream1 = dfStream1
        .writeStream()
        .outputMode(OutputMode.Append())
        .format("console")
        .start();

    StreamingQuery queryStream2 = dfStream2
        .writeStream()
        .outputMode(OutputMode.Update())
        .format("console")
        .start();

    log.debug("<- start()");
  }
}
