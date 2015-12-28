package com.griddynamics.bigdata.darknet.analytics.job

import com.griddynamics.bigdata.darknet.analytics.utils.{AnalyticsUtils, ClassificationGroup}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

/**
  * TODO
  */
object PredictNonPornRequestsJob extends SparkJob with LazyLogging {

  override def execute(sc: SparkContext, args: List[String]): Int = {
    val testDocsDir = args(0)
    val docsForLPsDir = args(1)
    val outputDir = args(2)

    val classificationGroup = ClassificationGroup.Porn
    val dictionaryAndModelLPs = AnalyticsUtils.buildLabeledPointsOfClassForDocs(sc, classificationGroup, docsForLPsDir)
    val lps = dictionaryAndModelLPs.collect()


    val modelLPs = dictionaryAndModelLPs

    val docs = sc.wholeTextFiles(testDocsDir)
    //FIXME
    /* val testFeatureVectors = AnalyticsUtils.featurizeDocuments(docs.map(doc => doc)

     val normalizedLps = modelLPs.map(lp => LabeledPoint(lp.label, lp.features))

     val predictedVectorsAndLabels = UserRequestPredictor.predictForClass(sc, ClassificationGroup.Unclassified, normalizedLps, normalizedTestFeatureVectors)

     val correlatedDocs = dictionary.unfeaturizeVector(predictedVectorsAndLabels.map(v => v._1))

     logger.info(s"Overall number of featureVectors for model test: ${normalizedTestFeatureVectors.count()}")
     logger.info(s"Number of requests predicted for class ${classificationGroup.label} : ${predictedVectorsAndLabels.count()}")

     correlatedDocs.saveAsTextFile(outputDir)*/
    1

  }
}



