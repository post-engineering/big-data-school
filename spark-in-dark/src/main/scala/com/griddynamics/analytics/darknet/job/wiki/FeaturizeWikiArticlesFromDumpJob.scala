package com.griddynamics.analytics.darknet.job.wiki

import com.griddynamics.analytics.darknet.job.SparkJob
import com.griddynamics.analytics.darknet.utils.{AnalyticsUtils, WikiPayloadExtractor}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * The {@link SparkJob} job implementation featurizes Wiki articles from raw dump data.
  */
object FeaturizeWikiArticlesFromDumpJob extends SparkJob with LazyLogging {

  /**
    * Executes the job
    * @param sc predefined Spark context
    * @param args required job arguments:
    *             #1: path to Wiki dump
    *             #2: path to target categories
    *             #3: path to result output directory
    *
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: String*): Int = {
    val wikiDumpPath = args(0)
    val targetCategoriesPath = args(1)
    val outputDirPath = args(2)

    val categoriesIndex = WikiPayloadExtractor.loadTargetCategories(sc, targetCategoriesPath)
      .distinct
      .collect
      .toSet
      .zipWithIndex
      .toMap[String, Int]

    val wikiArticles: RDD[String] = WikiPayloadExtractor.extractDocumentsFromRawData(sc, wikiDumpPath)

    val categorizedWikiArticles: RDD[(String, String)] = WikiPayloadExtractor.categorizeArticlesByTarget(
      sc,
      wikiArticles,
      categoriesIndex,
      false
    )

    val matchingCategories = categorizedWikiArticles.map { case (k, v) => k }
      .distinct
      .collect
      .toSet
      .zipWithIndex
      .toMap[String, Int]


    val classifiedWikiArticles = categorizedWikiArticles
      .map { case (k, v) =>
        (matchingCategories.get(k).get.toDouble, WikiPayloadExtractor.tokenizeArticleContent(v))
      }.cache()

    val classifiedFeatures: RDD[(Double, Vector)] = AnalyticsUtils.featurizeCategorizedDocuments(classifiedWikiArticles)

    val lps = classifiedFeatures.map { case (k, v) => LabeledPoint(k, v) }

    lps.saveAsTextFile(outputDirPath)
    1
  }


}
