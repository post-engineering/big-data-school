package com.griddynamics.analytics.darknet.job.pdml

import com.griddynamics.analytics.darknet.job.SparkJob
import com.griddynamics.analytics.darknet.utils.AnalyticsUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

/**
  * The {@link SparkJob} job implementation filters text documents by specific language.
  */
object FilterTextDocumentsByLanguage extends SparkJob with LazyLogging {
  /**
    * Executes the job
    * @param sc predefined Spark context
    * @param args required job arguments:
    *             #1: path to text documents
    *             #2: language to use as filter criteria
    *             #3: path to result data
    *
    *
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: String*): Int = {
    val input = args(0)
    val lang = args(1)
    val output = args(2)

    sc.textFile(input)
      .map(doc => doc.replace(", ", " "))
      .filter(doc => AnalyticsUtils.detectLanguage(doc).equals(lang))
      .saveAsTextFile(output)
    1
  }
}
