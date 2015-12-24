package com.griddynamics.bigdata.darknet.analytics.job

import com.griddynamics.bigdata.darknet.analytics.classification.UserRequestPredictor
import com.griddynamics.bigdata.darknet.analytics.utils.{AnalyticsUtils, ClassificationGroup}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * TODO
  */
object PredictPornRequestsJob extends SparkJob with LazyLogging {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val testDocsDir = args(0)
    val docsForLPsDir = args(1)
    val outputDir = args(2)

    val classificationGroup = ClassificationGroup.Porn
    val dictionaryAndModelLPs = AnalyticsUtils.buildLabeledPointsOfClassForDocs(sc, classificationGroup, docsForLPsDir)
    val lps = dictionaryAndModelLPs._2.collect()

    val dictionary = dictionaryAndModelLPs._1
    val modelLPs = dictionaryAndModelLPs._2

    val docs = sc.wholeTextFiles(testDocsDir).map(doc => doc._2)
    val testFeatureVectors = AnalyticsUtils.featurizeDocuments(docs, dictionary)
    val normalizedTestFeatureVectors = dictionary.normalizer.transform(testFeatureVectors)
    val normalizedLps = modelLPs.map(lp => LabeledPoint(lp.label, dictionary.normalizer.transform(lp.features)))

    val predictedVectorsAndLabels = UserRequestPredictor.predictForClass(sc, classificationGroup, normalizedLps, normalizedTestFeatureVectors)

    val correlatedDocs = dictionary.unfeaturizeVector(predictedVectorsAndLabels.map(v => v._1))

    logger.info(s"Overall number of featureVectors for model test: ${normalizedTestFeatureVectors.count()}")
    logger.info(s"Number of requests predicted for class ${classificationGroup.label} : ${predictedVectorsAndLabels.count()}")

    correlatedDocs.saveAsTextFile(outputDir)

    1

  }
}



