package com.griddynamics.bigdata.darknet.analytics.job

import com.griddynamics.bigdata.darknet.analytics.utils.AnalyticsUtils
import org.apache.spark.SparkContext

/**
  * TODO
  */
object ExtractClassifiedFeaturesJob extends SparkJob {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val input = args(0)
    val output = args(1)
    val classLabel = args(2)

    AnalyticsUtils.saveDocsAsLabeledPoints(sc, classLabel, input, output)
    1
  }
}
