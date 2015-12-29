package com.griddynamics.bigdata.darknet.analytics.job.pdml

import com.griddynamics.bigdata.darknet.analytics.job.SparkJob
import com.griddynamics.bigdata.darknet.analytics.utils.AnalyticsUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.rdd.RDD

/**
  * Created by ipertushin on 28.12.15.
  */
object ClusterizePdmlDataJob extends SparkJob with LazyLogging {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val modelPath = args(0)
    //val testFeaturesPath = args(1)
    val rawTestDataPath = args(1)
    val outputDirPath = args(2)
    val model = KMeansModel.load(sc, modelPath)

    val rawTestData = sc.wholeTextFiles(rawTestDataPath)
    val testData = AnalyticsUtils.tokenizeDocuments(rawTestData.map { case (k, v) => v })
    val testFeaturesWithContext = AnalyticsUtils.featurizeDocumentsWithContext(testData)
    val test0 = testFeaturesWithContext.collect

    val prediction: RDD[(Int, Seq[String])] = testFeaturesWithContext.map { case (ctx, featuresVector) =>
      val clusterId = model.predict(featuresVector)
      (clusterId, ctx)
    }
    val test1 = prediction.collect


    val result = prediction.groupByKey().map {
      case (predictedClusterId, clusterData) =>
        val clusterDataFormatted = clusterData.toSeq.map {
          case (seq) =>
            val buffer = StringBuilder.newBuilder
            seq.foreach(s => buffer.append(s).append(" "))
            s"Requested page content: ${buffer.toString()}\n"
        }
        (predictedClusterId, clusterDataFormatted)
    }.collect


    for ((predictedClusterId, clusterData) <- result) {
      logger.info(s"size of cluster #$predictedClusterId : is ${clusterData.size}")
      val outputPath = outputDirPath + "/" + predictedClusterId.toString
      sc.parallelize(clusterData).saveAsTextFile(outputPath)
    }

    1
  }
}
