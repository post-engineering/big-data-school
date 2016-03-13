package com.griddynamics.deepdetector.opentsdb.client

import java.io._
import java.net.{HttpURLConnection, SocketTimeoutException, URL}

import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.io.Source

/**
  * TODO
  */
class TSDBClient(server: String, port: String) extends Thread with LazyLogging {

  var sessionOS: OutputStream = _
  var activeQuery: TSDBQuery = _

  override def run(): Unit = {

    val pw = new PrintWriter(new BufferedOutputStream(sessionOS))

    try {
      while (true) {
        logger.info(s"worker executes: ${activeQuery}")
        val result = executeQuery(activeQuery)

        logger.info(result)
        pw.write(s"${result}\n")

        logger.info(s"await for ${activeQuery.getIntervalInMS()} ms")
        Thread.sleep(activeQuery.getIntervalInMS())
      }
    } catch {
      case tie: InterruptedException => logger.info("agent worker has been stopped")
      case ioe: IOException => logger.error(ioe.getStackTrace.mkString("\n")) //TODO handle this
      case ste: SocketTimeoutException => logger.error(ste.getStackTrace.mkString("\n")) //TODO handle this
    }

  }

  /**
    * TODO
    * @param query
    * @return
    */
  def executeQuery(query: TSDBQuery): String = {
    get(query.create(server, port))
  }

  /**
    * Returns the text (content) from a REST URL as a String.
    * The `connectTimeout` and `readTimeout` comes from the Java URLConnection
    * class Javadoc.
    * @param url The full URL to connect to.
    * @param connectTimeout Sets a specified timeout value, in milliseconds,
    *                       to be used when opening a communications link to the resource referenced
    *                       by this URLConnection. If the timeout expires before the connection can
    *                       be established, a java.net.SocketTimeoutException
    *                       is raised. A timeout of zero is interpreted as an infinite timeout.
    *                       Defaults to 5000 ms.
    * @param readTimeout If the timeout expires before there is data available
    *                    for read, a java.net.SocketTimeoutException is raised. A timeout of zero
    *                    is interpreted as an infinite timeout. Defaults to 5000 ms.
    * @param requestMethod Defaults to "GET". (Other methods have not been tested.)

    */
  @throws(classOf[java.io.IOException])
  @throws(classOf[java.net.SocketTimeoutException])
  private def get(url: String,
                  connectTimeout: Int = 5000,
                  readTimeout: Int = 5000,
                  requestMethod: String = "GET") = {

    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val inputStream = connection.getInputStream
    val content = Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close
    content
  }

  /**
    * TODO
    */
  def streamTS(query: TSDBQuery, os: OutputStream) = {
    activeQuery = query
    sessionOS = os
    start()
  }

}
