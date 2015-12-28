package com.griddynamics.bigdata.darknet.analytics.job

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.util.MLUtils

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
    val outputModelDirPath = args(1)

    val lps = MLUtils.loadLabeledPoints(sc, lpsPath)
      .cache()

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

    //TODO map vectors to docs
    1
  }
}
