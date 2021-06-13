package net.jgp.books.spark.ch10.x.utils.streaming.app;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch10.x.utils.streaming.lib.FieldType;
import net.jgp.books.spark.ch10.x.utils.streaming.lib.RecordGeneratorUtils;
import net.jgp.books.spark.ch10.x.utils.streaming.lib.RecordStructure;
import net.jgp.books.spark.ch10.x.utils.streaming.lib.RecordWriterUtils;
import net.jgp.books.spark.ch10.x.utils.streaming.lib.StreamingUtils;

/**
 * This application generates a series of random records at random interval
 * during one minute. All parameters are random or configurable.
 * 
 * @author jgp
 */
public class RecordsInFilesGeneratorApp {
  private static Logger log = LoggerFactory
      .getLogger(RecordsInFilesGeneratorApp.class);

  /**
   * Streaming duration in seconds.
   */
  public int streamDuration = 60;

  /**
   * Maximum number of records send at the same time.
   */
  public int batchSize = 10;

  /**
   * Wait time between two batches of records, in seconds, with an element
   * of variability. If you say 10 seconds, the system will wait between 5s
   * and 15s, if you say 20s, the system will wait between 10s and 30s, and
   * so on.
   */
  public int waitTime = 5;

  public static void main(String[] args) {
    String outputDirectory = StreamingUtils.getInputDirectory();
    if (args.length == 2
        && args[0].compareToIgnoreCase("--output-directory") == 0) {
      outputDirectory = args[1];
      File dir = new File(outputDirectory);
      dir.mkdir();
    }

    RecordStructure rs = new RecordStructure("contact")
        .add("fname", FieldType.FIRST_NAME)
        .add("mname", FieldType.FIRST_NAME)
        .add("lname", FieldType.LAST_NAME)
        .add("age", FieldType.AGE)
        .add("ssn", FieldType.SSN);

    RecordsInFilesGeneratorApp app = new RecordsInFilesGeneratorApp();
    app.start(rs, outputDirectory);
  }

  private void start(RecordStructure rs, String outputDirectory) {
    log.debug("-> start (..., {})", outputDirectory);
    long start = System.currentTimeMillis();
    while (start + streamDuration * 1000 > System.currentTimeMillis()) {
      int maxRecord = RecordGeneratorUtils.getRandomInt(batchSize) + 1;
      RecordWriterUtils.write(
          rs.getRecordName() + "_" + System.currentTimeMillis() + ".txt",
          rs.getRecords(maxRecord, false),
          outputDirectory);
      try {
        Thread.sleep(RecordGeneratorUtils.getRandomInt(waitTime * 1000)
            + waitTime * 1000 / 2);
      } catch (InterruptedException e) {
        // Simply ignore the interruption
      }
    }
  }

}
