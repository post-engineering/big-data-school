package com.griddynamics.bigdata.darknet.analytics.job.wiki

import com.griddynamics.bigdata.darknet.analytics.job.SparkJob
import com.griddynamics.bigdata.darknet.analytics.utils.{AnalyticsUtils, WikiPayloadExtractor}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * TODO
  */
object FeaturizeWikiArticlesJob extends SparkJob with LazyLogging {


  override def execute(sc: SparkContext, args: List[String]): Int = {
    val wikiDumpPath = args(0)
    val targetCategoriesPath = args(1)
    val outputDirPath = args(2)

    val categoriesIndex = WikiPayloadExtractor.loadTargetCategories(sc, targetCategoriesPath)
      .distinct
      .collect
      .toSet
      .zipWithIndex
      .toMap[String, Int]

    val wikiArticles: RDD[String] = WikiPayloadExtractor.loadWikiArticles(sc, wikiDumpPath)

    val categorizedWikiArticles: RDD[(String, String)] = WikiPayloadExtractor.categorizeArticlesByTarget(sc, wikiArticles, categoriesIndex)

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
