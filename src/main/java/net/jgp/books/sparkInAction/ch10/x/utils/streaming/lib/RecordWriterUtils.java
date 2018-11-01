package net.jgp.books.sparkInAction.ch10.x.utils.streaming.lib;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes a record to an output.
 * 
 * @author jgp
 */
public abstract class RecordWriterUtils {
  private static final Logger log = LoggerFactory.getLogger(RecordWriterUtils.class);

  public static void write(String filename, StringBuilder record) {
    String fullFilename = StreamingUtils.getInputDirectory() + filename;

    log.info("Writing in: {}", fullFilename);

    // Open file
    BufferedWriter out = null;
    try {
      FileWriter fstream = new FileWriter(fullFilename, true); // true tells to
                                                               // append data.
      out = new BufferedWriter(fstream);
    } catch (IOException e) {
      log.error("Error while opening file: {}", e.getMessage());
    }

    // Write file
    try {
      out.write(record.toString());
    } catch (IOException e) {
      log.error("Error while writing: {}", e.getMessage());
    }

    // Close file
    try {
      out.close();
    } catch (IOException e) {
      log.error("Error while closing the file: {}", e.getMessage());
    }
  }
}
