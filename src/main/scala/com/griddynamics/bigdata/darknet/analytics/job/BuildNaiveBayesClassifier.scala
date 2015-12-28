package com.griddynamics.bigdata.darknet.analytics.job

import com.griddynamics.bigdata.darknet.analytics.utils.AnalyticsUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
  * TODO
  */
object BuildNaiveBayesClassifier extends SparkJob with LazyLogging {

  /**
    * Executes job specific logic
    * @param sc predefined Spark context
    * @param args job arguments
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: List[String]): Int = {
    val lpsPath = args(0)
    val targetCategoriesPath = args(1)
    val testDataPath = args(2)
    val outputModelDirPath = args(3)


    val lps = MLUtils.loadLabeledPoints(sc, lpsPath)
      .cache()

    val targetCategoriesIndex = sc.textFile(targetCategoriesPath)
      .collect
      .toSet
      .zipWithIndex
      .toMap[String, Int]


    val model = NaiveBayes.train(lps)
    model.save(sc, outputModelDirPath)

    /**
      * test model's accuracy
      */
    val predictedExpected = lps.map { lp =>
      val predictedLabel = model.predict(lp.features)
      (predictedLabel, lp.label)

    }
    val predictedCount = predictedExpected.count()
    val hitCount = predictedExpected.filter { case (l1, l2) => l1 != l2 }.count()
    val accuracy = hitCount / (predictedCount / 100)
    logger.info(s"predicted: $predictedCount \nhit: $hitCount \naccuracy: $accuracy")

    /**
      * classify test data
      */
    val testDataTokenized = sc.wholeTextFiles(testDataPath).map { case (k, v) => AnalyticsUtils.tokenizeDocument(v) }
    val testDataFeaturized = AnalyticsUtils.featurizeDocuments(testDataTokenized)

    val prediction: RDD[(Double, Vector)] = testDataFeaturized.map { docVec =>
      val predictedLabel = model.predict(docVec)
      (predictedLabel, docVec)
    }

    //TODO map vectors to docs
    1
  }
}
