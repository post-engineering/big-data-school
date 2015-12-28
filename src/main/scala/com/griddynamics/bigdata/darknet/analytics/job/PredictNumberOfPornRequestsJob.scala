package com.griddynamics.bigdata.darknet.analytics.job

import com.griddynamics.bigdata.darknet.analytics.classification.UserRequestPredictor
import com.griddynamics.bigdata.darknet.analytics.utils.ClassificationGroup
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils

/**
  * TODO
  */
object PredictNumberOfPornRequestsJob extends SparkJob with LazyLogging {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val featureVectorsPath = args(0)
    //val modelLPsPAth = args(1)
    val svmModelPath = args(1)
    val outputDir = args(2)

    val testFeatureVectors = MLUtils.loadVectors(sc, featureVectorsPath)
    val testFeatureVectorsCount = testFeatureVectors.count()

    // val modelLPs = MLUtils.loadLabeledPoints(sc, modelLPsPAth)
    val classificationGroup = ClassificationGroup.Porn
    val predictedVectorsAndLabels = UserRequestPredictor.predictForClass(sc, classificationGroup, svmModelPath, testFeatureVectors)

    predictedVectorsAndLabels.saveAsTextFile(outputDir)
    val count = predictedVectorsAndLabels.count()

    //TODO logging
    logger.info(s"Overall number of featureVectors for model test: $testFeatureVectorsCount")
    logger.info(s"Number of requests predicted for class ${classificationGroup.label} : $count")

    1
  }
}



