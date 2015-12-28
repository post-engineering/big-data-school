package com.griddynamics.bigdata.darknet.analytics.job.wiki

import com.griddynamics.bigdata.darknet.analytics.job.SparkJob
import com.griddynamics.bigdata.darknet.analytics.utils.WikiPayloadExtractor
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

/**
  * TODO
  */
object ExtractWikiCategories extends SparkJob with LazyLogging {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val wikiDumpPath = args(0)
    val extractedCategoriesPath = args(1)

    val wikiDump = WikiPayloadExtractor.loadWikiArticles(sc, wikiDumpPath)
    val categories = WikiPayloadExtractor.extractCategories(sc, wikiDump)
    categories.cache()

    logger.info(s"Number of categories: ${categories.count()}")
    //on extract - 15/12/27 15:50:05 INFO ExtractWikiCategories$: Number of categories: 22227
    //ExtractWikiCategories$: Number of categories: 21948


    categories.saveAsTextFile(extractedCategoriesPath)
    1
  }
}
