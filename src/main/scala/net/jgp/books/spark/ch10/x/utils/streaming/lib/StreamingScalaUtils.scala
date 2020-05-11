package net.jgp.books.spark.ch10.x.utils.streaming.lib

/**
 * Series of utilities for streaming
 *
 * @author rambabu.posa
 */
object StreamingScalaUtils {

  def getInputDirectory: String =
    if (System.getProperty("os.name").toLowerCase.startsWith("win")) "C:\\TEMP\\"
    else System.getProperty("java.io.tmpdir")

}
