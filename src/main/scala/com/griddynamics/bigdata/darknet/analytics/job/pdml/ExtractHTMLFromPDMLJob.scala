package com.griddynamics.bigdata.darknet.analytics.job.pdml

import com.griddynamics.bigdata.darknet.analytics.job.SparkJob
import com.griddynamics.bigdata.darknet.analytics.utils.PdmlPayloadExtractor
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

/**
  * TODO
  */
object ExtractHTMLFromPDMLJob extends SparkJob with LazyLogging {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val input = args(0)
    val output = args(1)

    PdmlPayloadExtractor.extractHtmlFromPDML(sc, input).saveAsTextFile(output)

    1
  }
}