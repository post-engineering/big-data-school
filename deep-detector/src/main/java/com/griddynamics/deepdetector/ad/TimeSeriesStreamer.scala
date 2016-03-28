package com.griddynamics.deepdetector.ad

import java.net.{HttpURLConnection, URL}
import java.util.concurrent.TransferQueue
import com.griddynamics.deepdetector.etl.jobs.ExtractTimeSeriesJob
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try

/**
  * TODO
  */
private[ad] class TimeSeriesStreamer(streamingQuery: TSDBQuery,
                                     transferQueue: TransferQueue[Seq[(Long, Double)]]) extends Runnable with LazyLogging {

  override def run(): Unit = {
    val timeline = ListBuffer[(Long, Double)]()

    streamingQuery.from(s"${streamingQuery.getInterval() / 1000L}s-ago")
    streamingQuery.to("1s-ago")
    while (true) {
      //acquire interval of time Steps
      while (!doesFitInterval(timeline, streamingQuery.getInterval())) {
        logger.info(s"worker executes: ${streamingQuery}")
        try {
          executeQuery(streamingQuery) match {
            case Some(timeline) => {
              logger.info(s"got new timeline: ${timeline}")

              if (timeline != null && timeline.size > 0) {
                timeline.++(timeline.toList)
                val nextStart = timeline.last._1 * 1000L + 1L
                streamingQuery.from(nextStart.toString)
              }
              streamingQuery.to("1s-ago")

            }
            case None => logger.info("query request has been skipped...")
          }

        } catch {
          case e: Exception => logger.error(e.getStackTrace.mkString("\n"))
        }
      }

      logger.info(s"timeline for interval of ${streamingQuery.getInterval()} ms has been acquired! Attempt transfer...")

      //TODO refactor
      val timelineToTransfer = timeline.toSeq
      if (!transferQueue.tryTransfer(timelineToTransfer)) {
        transferQueue.transfer(timelineToTransfer) //wait untill will be taken by a cinsumer
      } else {
        waitForTimeout(streamingQuery.getTimeout()) //wait for timeout
      }

      timeline.clear()
    }
  }

  private[this] def doesFitInterval(timeline: Seq[(Long, Double)], expectedInterval: Long): Boolean = {
    if (timeline.size == 0)
      return false

    val acquiredInterval = timeline.last._1 - timeline.head._1
    !(acquiredInterval < expectedInterval)
  }

  /**
    * TODO
    * @param query
    * @return
    */
  private[this] def executeQuery(query: TSDBQuery): Option[Seq[(Long, Double)]] = {
    //TODO NPE and query Validation check
    var queryResult: Option[Seq[(Long, Double)]] = None

    get(query.create()) match {
      case util.Success(tsdbResponse) => Some(ExtractTimeSeriesJob.extractTimeSeries(tsdbResponse))
      case util.Failure(error) => {
        logger.error(error.getMessage)
        None
      }
    }

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
                  connectTimeout: Int = 5000, //TODO load from config
                  readTimeout: Int = 5000,
                  requestMethod: String = "GET"): Try[String] = {

    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val inputStream = connection.getInputStream
    val content = Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close
    Try(content)
  }

  private[this] def waitForTimeout(timeoutInterval: Long): Unit = {
    if (timeoutInterval > 0L)
      this.synchronized {
        try {
          logger.info(s"wait for ${timeoutInterval} ms ...")
          this.wait(timeoutInterval)
        } catch {
          case ie: InterruptedException => "timeout waiting has been interrupted"
        }
      }
  }
}
