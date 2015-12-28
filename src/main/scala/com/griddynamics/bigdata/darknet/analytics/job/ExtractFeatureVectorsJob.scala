package com.griddynamics.bigdata.darknet.analytics.job

import com.griddynamics.bigdata.darknet.analytics.utils.PdmlPayloadExtractor
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

/**
  * TODO
  */
object ExtractFeatureVectorsJob extends SparkJob with LazyLogging {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val input = args(0)
    val output = args(1)

    PdmlPayloadExtractor.extractAndSaveAsFeatureVectors(sc, input, output)
    1
  }
}
