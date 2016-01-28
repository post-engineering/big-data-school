package com.griddynamics.analytics.darknet.job.pdml

import com.griddynamics.analytics.darknet.job.SparkJob
import com.griddynamics.analytics.darknet.utils.{PdmlPayloadExtractor, WikiPayloadExtractor}
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

    PdmlPayloadExtractor.extractPayloadFromRawData(sc, input)
      .map(doc => WikiPayloadExtractor
        .tokenizeArticleContent(doc)
        .filter { t => t.length > 1 } //TODO filter numbers and special charecters out?
        .mkString(", ")
      ) //FIXME  implement  tokenizer specific to PDML-HTML payload
      .saveAsTextFile(output)

    1
  }
}