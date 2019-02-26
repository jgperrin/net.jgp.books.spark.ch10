package net.jgp.books.spark.ch10.lab930_memory_sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch10.x.utils.streaming.lib.StreamingUtils;

/**
 * Analyzes the records on the stream and send each record to a debugger class.
 * 
 * @author jgp
 *
 */
public class StreamRecordInMemoryApp {
  private static Logger log =
      LoggerFactory.getLogger(StreamRecordInMemoryApp.class);

  public static void main(String[] args) {
    StreamRecordInMemoryApp app = new StreamRecordInMemoryApp();
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
        .outputMode(OutputMode.Append())
        .format("memory")
        .option("queryName", "people")
        .start();

    // Wait and process the incoming stream for the next minute
    Dataset<Row> queryInMemoryDf;
    int iterationCount = 0;
    long start = System.currentTimeMillis();
    while (query.isActive()) {
      queryInMemoryDf = spark.sql("SELECT * FROM people");
      iterationCount++;
      log.debug("Pass #{}, dataframe contains {} records",
          iterationCount,
          queryInMemoryDf.count());
      queryInMemoryDf.show();
      if (start + 60000 < System.currentTimeMillis()) {
        query.stop();
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
