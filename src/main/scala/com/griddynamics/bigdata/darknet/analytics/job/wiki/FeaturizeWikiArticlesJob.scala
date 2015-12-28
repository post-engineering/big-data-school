package com.griddynamics.bigdata.darknet.analytics.job.wiki

import com.griddynamics.bigdata.darknet.analytics.job.SparkJob
import com.griddynamics.bigdata.darknet.analytics.utils.{TFiDFDictionary, WikiPayloadExtractor}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
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


    val targetCategoriesSet = WikiPayloadExtractor.loadTargetCategories(sc, targetCategoriesPath)
      .collect()
      .toSet

    val categoriesIndex = targetCategoriesSet.zipWithIndex.toMap

    val wikiArticles: RDD[String] = WikiPayloadExtractor.loadWikiArticles(sc, wikiDumpPath)
    // val test0 = wikiArticles.collect()

    val categorizedWikiArticles: RDD[(String, String)] = WikiPayloadExtractor.categorizeArticlesByTarget(sc, wikiArticles, categoriesIndex)
    //val test1 = categorizedWikiArticles.collect()

    val tokenizedCategorizedWikiArticles = categorizedWikiArticles
      .map { case (k, v) =>
        (categoriesIndex.getOrElse(k, 0).toDouble, WikiPayloadExtractor.tokenizeArticleContent(v))
      }


    tokenizedCategorizedWikiArticles.cache()
    //val test2 = tokenizedCategorizedWikiArticles.collect()

    val categorizedFeatures = TFiDFDictionary.featurizeCategorizedDocuments(tokenizedCategorizedWikiArticles)
    // val test3 = categorizedFeatures.collect()

    val lps = categorizedFeatures.map({ case (k, v) => LabeledPoint(k, v) })

    lps.saveAsTextFile(outputDirPath)
    1
  }


}
