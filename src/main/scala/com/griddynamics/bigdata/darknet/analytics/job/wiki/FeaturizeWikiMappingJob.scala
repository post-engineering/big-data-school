package com.griddynamics.bigdata.darknet.analytics.job.wiki

import com.griddynamics.bigdata.darknet.analytics.job.SparkJob
import com.griddynamics.bigdata.darknet.analytics.utils.AnalyticsUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * The {@link SparkJob} job implementation featurizes Wiki mapping.
  */
object FeaturizeWikiMappingJob extends SparkJob with LazyLogging {

  /**
    * Executes the job
    * @param sc predefined Spark context
    * @param args required job arguments:
    *             #1: path to Wiki Mapping
    *             #2: path to result categories index
    *             #3: path to result labeled features
    *
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args:  String*): Int = {
    val mappingPath = args(0)
    val resultCategoriesIndexPath = args(1)
    val resultLPsPath = args(2)

    val mapping = sc.objectFile[(String, Seq[String])](mappingPath)

    //all categories
    val categoriesIndex = mapping
      .map { case (k, v) => k }
      .collect
      .distinct
      .zipWithIndex
      .toMap[String, Int]

    sc.parallelize(categoriesIndex.toSeq)
      .saveAsObjectFile(resultCategoriesIndexPath)

    val indexedDocs = mapping
      .map { case (k, v) =>
        (categoriesIndex.getOrElse(k, -1).toDouble, v)
      }

    val categorizedDocs = AnalyticsUtils.featurizeCategorizedDocuments(indexedDocs)

    val lps = categorizedDocs.map { case (categoryId, features) =>
      LabeledPoint(categoryId, features)
    }

    lps.saveAsTextFile(resultLPsPath)
    1
  }

}
