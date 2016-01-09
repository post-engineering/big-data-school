package com.griddynamics.analytics.darknet.job.clusterisation

import com.griddynamics.analytics.darknet.utils.AnalyticsUtils
import AnalyticsUtils._
import com.griddynamics.analytics.darknet.job.SparkJob
import com.griddynamics.analytics.darknet.utils.AnalyticsUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans

/**
  * The {@link SparkJob} job implementation builds KMeans clustering model.
  */
object BuildKMeansClusteringModel extends SparkJob with LazyLogging {

  /**
    * Executes the job
    * @param sc predefined Spark context
    * @param args required job arguments:
    *             #1: path to model train data
    *             #2: number of cluster to find
    *             #3: max number of iterations
    *             #4: path to result model
    *
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: String*): Int = {
    val trainDataPath = args(0)

    /**
    To determine the number of clusters, just try some values
      until you visually find meaningful grouping or you have a lower cost value.
      */
    val numClusters = args(1)
    val maxIterations = args(2)
    val modelOutputPath = args(3)

    val rawTrainData = sc.wholeTextFiles(trainDataPath, 20).map { case (k, v) => v }
    val featurizedTrainData = featurizeDocuments(tokenizeDocuments(rawTrainData))
      .cache()

    val model = KMeans.train(featurizedTrainData, numClusters.toInt, maxIterations.toInt)

    val cost = model.computeCost(featurizedTrainData)
    println("Cost: " + cost)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val wssse = model.computeCost(featurizedTrainData)
    logger.info("Within Set Sum of Squared Errors = " + wssse)

    model.save(sc, modelOutputPath)

    1
  }
}
