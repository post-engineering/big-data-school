package com.griddynamics.bigdata.darknet.analytics.job.wiki

import com.griddynamics.bigdata.darknet.analytics.job.SparkJob
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

/**
  * TODO
  */
object BuildClassifier extends SparkJob with LazyLogging {

  /**
    * Executes job specific logic
    * @param sc predefined Spark context
    * @param args job arguments
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: List[String]): Int = {
    val lpsPath = args(0)
    val targetCategoriesPath = args(1)
    val testVectorsPath = args(2)
    val outputModelDirPath = args(3)


    /*  val targetCategoriesSet = //
        .collect()
        .toSet

      val categoriesIndex = targetCategoriesSet.zipWithIndex.toMap*/


    /*
     lps.cache()
      val model = classification.NaiveBayes.train(lps)

      val testData = lps

      val predictedExpected = testData.map { lp =>
        val predictedLabel = model.predict(lp.features)
        (predictedLabel, lp.label)

      }

      val predictedCount = predictedExpected.count()
      val hitCount = predictedExpected.filter{case(l1,l2) => l1!=l2}.count()
      val accuracy = hitCount / ( predictedCount/100)
      logger.info(s"predicted: $predictedCount \nhit: $hitCount \naccuracy: $accuracy")
     */
    1
  }
}
