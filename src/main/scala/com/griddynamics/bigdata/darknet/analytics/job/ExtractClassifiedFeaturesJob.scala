package com.griddynamics.bigdata.darknet.analytics.job

import com.griddynamics.bigdata.darknet.analytics.utils.ClassificationGroup.ClassificationGroupValue
import com.griddynamics.bigdata.darknet.analytics.utils.{AnalyticsUtils, ClassificationGroup}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

/**
  * TODO
  */
object ExtractClassifiedFeaturesJob extends SparkJob with LazyLogging {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val input = args(0)
    val output = args(1)
    val classificationGroup = ClassificationGroup.getClassificationGroupByLabel(args(2)).asInstanceOf[ClassificationGroupValue]

    AnalyticsUtils.saveDocsAsLabeledPoints(sc, classificationGroup, input, output)
    1
  }
}
