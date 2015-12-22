package com.griddynamics.bigdata.darknet.analytics.job

import com.griddynamics.bigdata.darknet.analytics.utils.PdmlPayloadExtractor
import org.apache.spark.SparkContext

/**
  * TODO
  */
object ExtractAndSaveDocumentsJob extends SparkJob {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val input = args(0)
    val output = args(1)

    PdmlPayloadExtractor.extractAndSaveDocuments(sc, input, output)
    1
  }
}
