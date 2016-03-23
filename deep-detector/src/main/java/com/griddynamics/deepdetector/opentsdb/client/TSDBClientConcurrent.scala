package com.griddynamics.deepdetector.opentsdb.client

import java.io._
import java.net.{HttpURLConnection, Socket, SocketTimeoutException, URL}
import java.util.concurrent.{Executors, LinkedTransferQueue, TimeUnit}

import com.griddynamics.deepdetector.etl.ExtractTimeSeriesJob
import com.griddynamics.deepdetector.lstmnet.AnomalyDetectionNN
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * TODO
  */
class TSDBClientConcurrent(server: String, port: String) extends Thread with LazyLogging {


  val cachingExecutor = Executors.newCachedThreadPool()

  val transferQueue = new LinkedTransferQueue[Seq[(Long, Double)]]

  val tsStreamingWorker = new Runnable {

    def doesFitInterval(timeline: Seq[(Long, Double)], expectedInterval: Long): Boolean = {
      if (timeline.size == 0)
        return false

      val acquiredInterval = timeline.last._1 - timeline.head._1
      !(acquiredInterval < expectedInterval)
    }

    override def run(): Unit = {
      val timeline = ListBuffer[(Long, Double)]()

      while (true) {

        //acquire interval of time Steps
        while (!doesFitInterval(timeline, activeQuery.getIntervalInMS())) {
          logger.info(s"worker executes: ${activeQuery}")
          val rawTimetine = executeQuery(activeQuery)
          logger.info(rawTimetine)

          timeline.++(ExtractTimeSeriesJob.extractTimeSeries(rawTimetine))

          activeQuery.from(timeline.last._1 + 1L)
        }


        transferQueue.transfer(timeline.toSeq) // FIXME check if toSeq return a copy
        timeline.clear()
      }
    }
  }

  //even several workers can be running simultaneously trying to take from queue
  val predictingWorker = new Runnable {
    override def run(): Unit = {
      while (true) {
        val recentTimeline = transferQueue.take()

        val prediction = AD.predictNextTimeLineIntervalBasedOnRecentState(recentTimeline, activeQuery.getIntervalInMS()) //predict next timeline interval
        logger.info(s"Predicted timeline: ${prediction.mkString(" -> ")}")

        //TODO publishPredictions
      }

    }
  }


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

    activeQuery.from(System.currentTimeMillis())
    try {

      cachingExecutor.submit(tsStreamingWorker)
      cachingExecutor.submit(predictingWorker)

     // cachingExecutor.shutdown()
      cachingExecutor.awaitTermination(Long.MaxValue, TimeUnit.DAYS) //FIXME: refactor with invokeAll or CountDownLatch
      cachingExecutor.shutdownNow()
    }
    catch {
      case tie: InterruptedException => logger.info("agent worker has been stopped")
      case ioe: IOException => logger.error(ioe.getStackTrace.mkString("\n")) //TODO handle this
      case ste: SocketTimeoutException => logger.error(ste.getStackTrace.mkString("\n")) //TODO handle this
    }
  }

  def publishPredictions(predictionsToPublish: Seq[(Long, Double)]) = {


    cachingExecutor.submit(
      new Runnable {
        override def run(): Unit = {
          val ps = new PrintWriter(openTsdbServerSessionOs.getOutputStream)
          predictionsToPublish.foreach { case ((ts, m)) =>
            val putRequest = s"${predictionMetricPrefix}.${activeQuery.getMetric()} ${ts} ${m} [{\"host\":\"pornocluster\"}]"
            ps.write(s"put ${putRequest}")
            logger.info(s"Predicted metric has been published: (${ts} : ${m})")
          }
        }
      })


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
    agentServerSessionOs = os
    start()
  }

}
