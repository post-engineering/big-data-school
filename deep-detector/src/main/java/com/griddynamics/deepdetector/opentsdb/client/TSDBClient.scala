package com.griddynamics.deepdetector.opentsdb.client

import java.io._
import java.net.{HttpURLConnection, Socket, SocketTimeoutException, URL}

import com.griddynamics.deepdetector.etl.ExtractTimeSeriesJob
import com.griddynamics.deepdetector.lstmnet.AnomalyDetectionNN
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer

import scala.io.Source

/**
  * TODO
  */
class TSDBClient(server: String, port: String) extends Thread with LazyLogging {

  val predictionMetricPrefix = "prediction"

  //FIXME move initialization of sc to AD, because conf requires attributes specific to dl4j
  val sc: SparkContext = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val jarsOpt = SparkContext.jarOfObject(this)

    jarsOpt match {
      case Some(ops) => conf.setJars(List(ops))
      case None => {
        conf.setMaster("local[4]") //  //spark://172.26.5.43:7077
          .set("spark.executor.memory", "1g")
          .set("spark.driver.memory", "1g")

        // conf.setJars(List("/home/ipertushin/IdeaProjects/big-data-school/spark-in-dark/target/big-deep-holms-1.0-SNAPSHOT.jar"))
      }

    }
    conf.set(SparkDl4jMultiLayer.AVERAGE_EACH_ITERATION, String.valueOf(true))
    new SparkContext(conf)
  }
  val AD = new AnomalyDetectionNN(sc, "/home/ipertushin/Documents/lstm_nn_model/")
  val openTsdbServerSessionOs = new Socket(server, port.toInt)

  //FIXME move to props or request from agent server
  var agentServerSessionOs: OutputStream = _
  var activeQuery: TSDBQuery = _

  override def run(): Unit = {

    val pw = new PrintWriter(new BufferedOutputStream(agentServerSessionOs))

    try {
      while (true) {
        logger.info(s"worker executes: ${activeQuery}")

        val rawTimetine = executeQuery(activeQuery)
        logger.info(rawTimetine)
        pw.write(s"got a new timeline: ${rawTimetine}\n")

        var timeline = ExtractTimeSeriesJob.extractTimeSeries(rawTimetine)
        if (timeline == null || timeline.size == 0) {
          timeline = Seq[(Long, Double)]((0, 0.0)) //empty timeline initialization
        }

        /*
        TODO make asynchronous call -> APPLY Scala RX
         */
        val prediction = AD.predictNextTimeLineBasedOnRecentState(timeline, 1)
        logger.info(s"Predicted timeline: ${prediction.mkString(" -> ")}")

        publishPredictions(prediction, timeline.last._1, activeQuery.getIntervalInMS())

        logger.info(s"await for ${activeQuery.getIntervalInMS()} ms before fetch next timeline")
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

  //TODO make in another thread asynchronously
  def publishPredictions(predictionsToPublish: Seq[Double],
                         fromTimeStamp: Long,
                         timeStepInterval: Long) = {


    new Thread(new Runnable {
      override def run(): Unit = {
        val ps = new PrintWriter(openTsdbServerSessionOs.getOutputStream)
        var lastTsPoint: Long = fromTimeStamp
        predictionsToPublish.foreach { p =>
          val putRequest = s"${predictionMetricPrefix}.${activeQuery.getMetric()} ${lastTsPoint} ${p} [{'host':'pornocluster'}]"
          ps.write(s"put ${putRequest}")
          logger.info(s"Predicted metric has been published: ${putRequest}")
          lastTsPoint += timeStepInterval
        }
      }
    }).start()

  }

  /**
    * TODO
    */
  def streamTS(query: TSDBQuery, os: OutputStream) = {
    activeQuery = query
    agentServerSessionOs = os
    start()
  }

}
