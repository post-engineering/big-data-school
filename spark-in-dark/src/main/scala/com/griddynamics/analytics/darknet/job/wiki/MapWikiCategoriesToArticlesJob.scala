package com.griddynamics.analytics.darknet.job.wiki

import com.griddynamics.analytics.darknet.job.SparkJob
import com.griddynamics.analytics.darknet.utils.WikiPayloadExtractor
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * The {@link SparkJob} job implementation maps Wiki categories to articles.
  */
object MapWikiCategoriesToArticlesJob extends SparkJob with LazyLogging {

  /**
    * Executes the job
    * @param sc predefined Spark context
    * @param args required job arguments:
    *             #1: path to Wiki dump
    *             #2: path to result mapping
    *
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: String*): Int = {
    val wikiDumpPath = args(0)
    val resultMappingPath = args(1)
    val wikiDump: RDD[String] = WikiPayloadExtractor.extractDocumentsFromRawData(sc, wikiDumpPath)

    val categoryToArticleMapping = wikiDump
      .filter(page => !WikiPayloadExtractor.isRedirect(page))
      .map { page =>
        val category = WikiPayloadExtractor.findCategory(page)
        val articleTokens = WikiPayloadExtractor.tokenizeArticleContent(page).distinct
        (category, articleTokens)
      }
      .filter { case (k, v) => !WikiPayloadExtractor.isUnknownCategory(k) }

    logger.info(s"Number of articles mapped to a category: ${categoryToArticleMapping.count()}")

    categoryToArticleMapping.saveAsObjectFile(resultMappingPath)
    logger.info(s"The mapping has been exported to: $resultMappingPath")
    1
  }
}
