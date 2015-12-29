package com.griddynamics.bigdata.darknet.analytics.job

import com.griddynamics.bigdata.darknet.analytics.utils.AnalyticsUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans

/**
  * TODO.
  */
object BuildKMeansCluster extends SparkJob with LazyLogging {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val trainDataPath = args(0)
    val numClusters = args(1)
    val maxIterations = args(2)
    val modelOutputPath = args(3)

    val rawTrainData = sc.wholeTextFiles(trainDataPath).map { case (k, v) => v }
    val featurizedTrainData = featurizeDocuments(tokenizeDocuments(rawTrainData))
      .cache()

    val model = KMeans.train(featurizedTrainData, numClusters.toInt, maxIterations.toInt)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val wssse = model.computeCost(featurizedTrainData)
    logger.info("Within Set Sum of Squared Errors = " + wssse)

    model.save(sc, modelOutputPath)

    1
  }
}
