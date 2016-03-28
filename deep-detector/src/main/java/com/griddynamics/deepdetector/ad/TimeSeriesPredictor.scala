package com.griddynamics.deepdetector.ad

import java.util.concurrent.TransferQueue

import com.griddynamics.deepdetector.lstmnet.AnomalyDetectionNNOnChars
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer
import scala.tools.nsc.io.Socket

/**
  * TODO
  */
private[ad] class TimeSeriesPredictor(sc: SparkContext,
                                      streamingQuery: TSDBQuery,
                                      streamingQueue: TransferQueue[Seq[(Long, Double)]],
                                      pathModelConfiguration: String = "/home/ipertushin/Documents/lstm_nn_model_onchar/", //FIXME
                                      doPublishingInBackgroud: Boolean = true) extends Runnable with LazyLogging {

  private[this] val AD = new AnomalyDetectionNNOnChars(sc, pathModelConfiguration)
  private[this] val predictionMetricPrefix = "prediction"
  private[this] val openTsdbSocket = Socket(streamingQuery.getServer(), streamingQuery.getPort()).either

  override def run(): Unit = {
    while (true) {
      val recentTimeline = streamingQueue.take()

      //predict next timeline interval
      val prediction = AD.predictNextTimeLineIntervalBasedOnRecentState(recentTimeline, streamingQuery.getInterval() / 1000L)
      logger.info(s"Predicted timeline: ${prediction.mkString(" -> ")}")

      //adjust timestamps for predicted timeSteps
      if (prediction != null && prediction.size > 0) {
        val timelineToPublish = ListBuffer[(Long, Double)]()
        timelineToPublish.+=((recentTimeline.last._1 + prediction(0)._1, prediction(0)._2))
        for (i <- 1 to prediction.size - 2) {
          timelineToPublish.+=((timelineToPublish(i - 1)._1 + prediction(i)._1, prediction(i)._2))


          logger.info(s"Publishing timeline: ${timelineToPublish.mkString(" -> ")}")
          if (doPublishingInBackgroud) {
            publishPredictionsAsynchronously(timelineToPublish)
          } else {
            publishPredictions(timelineToPublish)
          }

        }

      }
    }
  }

  /**
    * TODO
    * @param predictionsToPublish
    */
   private[this] def publishPredictionsAsynchronously(predictionsToPublish: Seq[(Long, Double)]) = {
    new Thread(
      new Runnable {
        override def run(): Unit = {
          publishPredictions(predictionsToPublish)
        }

      }).start()
  }

  /**
    * TODO
    * @param predictionsToPublish
    */
  private[this] def publishPredictions(predictionsToPublish: Seq[(Long, Double)]) = {

    openTsdbSocket match {
      case Left(error) => logger.error(s"can't establish connection with openTSDB server" +
        s" (${streamingQuery.getServer()}:${streamingQuery.getPort()})")

      case Right(socket) => {
        predictionsToPublish.foreach { case ((ts, m)) =>

          val putRequest = s"${predictionMetricPrefix}.${streamingQuery.getMetric()} ${ts} ${m} host=pornocluster" //

          socket.printWriter().print(s"put ${putRequest}\r\n")

          logger.info(s"publish request:  ${putRequest}")
        }
      }
    }
  }


}
