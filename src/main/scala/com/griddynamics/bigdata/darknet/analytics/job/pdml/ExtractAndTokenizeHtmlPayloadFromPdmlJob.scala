package com.griddynamics.bigdata.darknet.analytics.job.pdml

import com.griddynamics.bigdata.darknet.analytics.job.SparkJob
import com.griddynamics.bigdata.darknet.analytics.utils.{PdmlPayloadExtractor, WikiPayloadExtractor}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

/**
  * The {@link SparkJob} job implementation extracts and tokenizes HTML payload from PDML dump.
  */
object ExtractAndTokenizeHtmlPayloadFromPdmlJob extends SparkJob with LazyLogging {

  /**
    * Executes the job
    * @param sc predefined Spark context
    * @param args required job arguments:
    *             #1: path to PDML dump
    *             #2: path to result extracted and tokenized html payload
    *
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: String*): Int = {
    val input = args(0)
    val output = args(1)

    PdmlPayloadExtractor.extractHtmlPayloadFromPDML(sc, input)
      .map(doc => WikiPayloadExtractor.tokenizeArticleContent(doc)) //TODO implement specific tokenizer
      .saveAsTextFile(output)

    1
  }
}