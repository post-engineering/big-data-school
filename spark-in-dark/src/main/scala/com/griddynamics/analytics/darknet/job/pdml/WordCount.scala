package com.griddynamics.analytics.darknet.job.pdml

import com.griddynamics.analytics.darknet.job.SparkJob
import com.griddynamics.analytics.darknet.utils.{AnalyticsUtils, PdmlPayloadExtractor, WikiPayloadExtractor}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

/**
  * TODO
  */
object WordCount extends SparkJob with LazyLogging {
  /**
    * Executes job specific logic
    * @param sc predefined Spark context
    * @param args job arguments
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: String*): Int = {
    val input = args(0)
    val output = args(1)

    val wordCountData = sc.textFile(input,500)
      .map ( doc => doc.split(", ").toSeq)
      .flatMap(identity)
      .map(s => (s, 1))
      .reduceByKey(_ + _)

    AnalyticsUtils.visualizeWordCloud(wordCountData, output)

    1
  }


}
