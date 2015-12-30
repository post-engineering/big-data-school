package com.griddynamics.bigdata.darknet.analytics.job.transformation

import com.griddynamics.bigdata.darknet.analytics.job.SparkJob
import com.griddynamics.bigdata.darknet.analytics.utils.AnalyticsUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

/**
  * TODO
  */
object ExtractFeatureVectorsJob extends SparkJob with LazyLogging {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val input = args(0)
    val output = args(1)

    val data = sc.wholeTextFiles(input).map { case (k, v) => AnalyticsUtils.tokenizeDocument(v) }
    AnalyticsUtils.featurizeDocuments(data)
      .saveAsTextFile(output)

    1
  }
}
