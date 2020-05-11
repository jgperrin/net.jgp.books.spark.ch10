package net.jgp.books.spark.ch10.lab300_read_records_from_multiple_streams

import org.apache.spark.sql.{ForeachWriter, Row}
import org.slf4j.LoggerFactory

class AgeCheckerScala extends ForeachWriter[Row] {

  private val serialVersionUID = 8383715100587612498L
  private val log = LoggerFactory.getLogger(classOf[AgeCheckerScala])
  private var streamId = 0

  def this(streamId: Int) {
    this()
    this.streamId = streamId
  }

  override def close(arg0: Throwable): Unit = {}

  override def open(arg0: Long, arg1: Long) = true

  def process(arg0: Row) {
    if (arg0.length != 5) return
    val age = arg0.getInt(3)

    if (age < 13)
      log.debug("On stream #{}: {} is a kid, they are {} yrs old.", streamId, arg0.getString(0), age)
    else if (age > 12 && age < 20)
      log.debug("On stream #{}: {} is a teen, they are {} yrs old.", streamId, arg0.getString(0), age)
    else if (age > 64)
      log.debug("On stream #{}: {} is a senior, they are {} yrs old.", streamId, arg0.getString(0), age)
  }

}
