package net.jgp.books.spark.ch10.lab920_for_each_sink

import org.apache.spark.sql.{ForeachWriter, Row}
import org.slf4j.LoggerFactory

/**
 * Very basic logger.
 *
 * @author rambabu.posa
 */
class RecordLogScalaDebugger extends ForeachWriter[Row] {

  private val serialVersionUID = 4137020658417523102L
  private val log = LoggerFactory.getLogger(classOf[RecordLogDebugger])
  private var count = 0

  /**
   * Closes the writer
   */
  override def close(arg0: Throwable): Unit = {}

  /**
   * Opens the writer
   */
  override def open(arg0: Long, arg1: Long) = true

  /**
   * Processes a row
   */
  override def process(arg0: Row): Unit = {
    count += 1
    log.debug(s"Record #${count} has ${arg0.length} column(s)")
    log.debug(s"First value: ${arg0.get(0)}")
  }

}
