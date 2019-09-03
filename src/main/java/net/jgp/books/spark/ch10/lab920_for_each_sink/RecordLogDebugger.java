package net.jgp.books.spark.ch10.lab920_for_each_sink;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Very basic logger.
 * 
 * @author jgp
 */
public class RecordLogDebugger extends ForeachWriter<Row> {
  private static final long serialVersionUID = 4137020658417523102L;
  private static Logger log =
      LoggerFactory.getLogger(RecordLogDebugger.class);
  private static int count = 0;

  /**
   * Closes the writer
   */
  @Override
  public void close(Throwable arg0) {
  }

  /**
   * Opens the writer
   */
  @Override
  public boolean open(long arg0, long arg1) {
    return true;
  }

  /**
   * Processes a row
   */
  @Override
  public void process(Row arg0) {
    count++;
    log.debug("Record #{} has {} column(s)", count, arg0.length());
    log.debug("First value: {}", arg0.get(0));
  }

}
