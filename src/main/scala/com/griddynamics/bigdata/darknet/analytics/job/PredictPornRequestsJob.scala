package com.griddynamics.bigdata.darknet.analytics.job

import com.griddynamics.bigdata.darknet.analytics.classification.UserRequestPredictor
import com.griddynamics.bigdata.darknet.analytics.utils.{AnalyticsUtils, ClassificationGroup, TFiDFDictionary}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

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
    val lps = dictionaryAndModelLPs.collect()

    val modelLPs = dictionaryAndModelLPs

    val docs = sc.wholeTextFiles(testDocsDir).map(doc => doc._2)
    //todo tokenize
    val testFeatureVectors = TFiDFDictionary.featurizeDocuments(docs.map(doc => doc.split("\\s")))


    val predictedVectorsAndLabels = UserRequestPredictor.predictForClass(sc, classificationGroup, modelLPs, testFeatureVectors)


    logger.info(s"Overall number of featureVectors for model test: ${testFeatureVectors.count()}")
    logger.info(s"Number of requests predicted for class ${classificationGroup.label} : ${predictedVectorsAndLabels.count()}")

    //FIXME!!!
    //correlatedDocs.saveAsTextFile(outputDir)

    1

  }
}



