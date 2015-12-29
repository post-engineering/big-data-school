package com.griddynamics.bigdata.darknet.analytics.job

import com.griddynamics.bigdata.darknet.analytics.utils.AnalyticsUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils

/**
  * TODO
  */
object BuildAndSaveSvmModelJob extends SparkJob with LazyLogging {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val modelLPsPath = args(0)
    val modelOutputDir = args(1)
    val trainTime = args(2).toInt

    val modelLPs = MLUtils.loadLabeledPoints(sc, modelLPsPath)
    AnalyticsUtils.saveSVMModel(sc, modelLPs, trainTime, modelOutputDir)
    1
  }
}
