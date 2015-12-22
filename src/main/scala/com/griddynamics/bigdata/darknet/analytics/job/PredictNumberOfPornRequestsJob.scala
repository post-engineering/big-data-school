package com.griddynamics.bigdata.darknet.analytics.job

import com.griddynamics.bigdata.darknet.analytics.classification.UserRequestPredictor
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils

/**
  * TODO
  */
object PredictNumberOfPornRequestsJob extends SparkJob {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val featureVectorsPath = args(0)
    //val modelLPsPAth = args(1)
    val svmModelPath = args(1)

    val testFeatureVectors = MLUtils.loadVectors(sc, featureVectorsPath)
    val testFeatureVectorsCount = testFeatureVectors.count()

    // val modelLPs = MLUtils.loadLabeledPoints(sc, modelLPsPAth)
    val classLabel = "porn"
    val count = UserRequestPredictor.predictCountOfClass(sc, classLabel, svmModelPath, testFeatureVectors).size

    println(s"Overall number of featureVectors for model test: $testFeatureVectorsCount")
    println(s"Number of request for class $classLabel : $count")

    1
  }
}



