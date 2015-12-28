package com.griddynamics.bigdata.darknet.analytics.job.pdml

import com.griddynamics.bigdata.darknet.analytics.job.SparkJob
import com.griddynamics.bigdata.darknet.analytics.utils.AnalyticsUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * Created by ipertushin on 28.12.15.
  */
object ClassifyPdmlDataJob extends SparkJob with LazyLogging {

  /**
    * Executes job specific logic
    * @param sc predefined Spark context
    * @param args job arguments
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: List[String]): Int = {
    val classificationModelPath = args(0)
    val targetCategoriesPath = args(1)
    val testDataPath = args(2)
    val resultDirPath = args(3)

    val model = NaiveBayesModel.load(sc, classificationModelPath)

    val targetCategoriesIndex = sc.textFile(targetCategoriesPath)
      .collect
      .toSet
      .zipWithIndex
      .toMap[String, Int]

    /**
      * classify test data
      */
    val testDataTokenized = sc.wholeTextFiles(testDataPath).map { case (k, v) => AnalyticsUtils.tokenizeDocument(v) }
    val testDataFeaturized = AnalyticsUtils.featurizeDocuments(testDataTokenized)
    testDataFeaturized.cache()

    val prediction: RDD[(Double, Vector)] = testDataFeaturized.map { docVec =>
      val predictedLabel = model.predict(docVec)
      (predictedLabel, docVec)
    }
    //TODO reduce by key and output

    //TODO map vectors to docs
    1
  }
}
