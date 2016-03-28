package com.griddynamics.deepdetector.ad

import java.io._
import java.net.SocketTimeoutException
import java.util.concurrent.{Executors, LinkedTransferQueue, TimeUnit}

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer

/**
  * TODO
  */
class ADWorker(activeQuery: TSDBQuery,
               agentServerSessionOs: OutputStream) extends Thread with LazyLogging {


  private val cachingExecutor = Executors.newCachedThreadPool()
  private val transferQueue = new LinkedTransferQueue[Seq[(Long, Double)]]

  private val tsStreamingWorker = new TimeSeriesStreamer(activeQuery, transferQueue)
  //even several workers can be running concurently trying to take from queue
  private val predictingWorker = new TimeSeriesPredictor(sc, activeQuery, transferQueue)

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

  override def run(): Unit = {
    val pw = new PrintWriter(new BufferedOutputStream(agentServerSessionOs))

    activeQuery.from(System.currentTimeMillis().toString)
    try {
      cachingExecutor.submit(tsStreamingWorker)
      cachingExecutor.submit(predictingWorker)

      // cachingExecutor.shutdown()
      cachingExecutor.awaitTermination(Long.MaxValue, TimeUnit.DAYS) //FIXME: refactor with invokeAll or CountDownLatch
      cachingExecutor.shutdownNow()
    }
    catch {
      case tie: InterruptedException => {
        cachingExecutor.shutdownNow()
        logger.info("agent worker has been stopped")
      }
      case ioe: IOException => logger.error(ioe.getStackTrace.mkString("\n"))
      case ste: SocketTimeoutException => logger.error(ste.getStackTrace.mkString("\n"))
    }
  }

}
