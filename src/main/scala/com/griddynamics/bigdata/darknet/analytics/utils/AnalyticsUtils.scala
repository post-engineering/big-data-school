package com.griddynamics.bigdata.darknet.analytics.utils

import com.griddynamics.bigdata.darknet.analytics.utils.ClassificationGroup.ClassificationGroupValue
import com.griddynamics.bigdata.html.JsoupExtractor
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
  * The utility class aggregates cross-cutting functions
  * that can be used for design of analytical jobs
  */
object AnalyticsUtils extends LazyLogging {

  //TODO
  def saveDocsAsFeatureVectors(docs: RDD[String],
                               outputDir: String): Unit = {
    val featureVectors = TFiDFDictionary.featurizeDocuments(docs.map(doc => doc.split("\\s").toSeq))
    featureVectors.saveAsTextFile(outputDir)
  }

  def saveDocsAsLabeledPoints(classificationGroup: ClassificationGroupValue,
                              docs: RDD[Seq[String]],
                              outputDir: String): Unit = {
    val labeledPoints = buildLabeledPointsOfClassForDocs(classificationGroup, docs)
    MLUtils.saveAsLibSVMFile(labeledPoints, outputDir)
  }

  def saveDocsAsLabeledPoints(sc: SparkContext,
                              classificationGroup: ClassificationGroupValue,
                              inputDir: String,
                              outputDir: String
                             ): Unit = {

    val labeledPoints = buildLabeledPointsOfClassForDocs(sc, classificationGroup, inputDir)
    labeledPoints.saveAsTextFile(outputDir)
  }

  def buildLabeledPointsOfClassForDocs(sc: SparkContext,
                                       classificationGroup: ClassificationGroupValue,
                                       inputDir: String
                                      ): RDD[LabeledPoint] = {
    val docs: RDD[String] = sc.wholeTextFiles(inputDir)
      .map(file => new JsoupExtractor().extractTextSafely(file._2))

    buildLabeledPointsOfClassForDocs(classificationGroup, docs.map(doc => doc.split("\\s")))
  }

  def buildLabeledPointsOfClassForDocs(classificationGroup: ClassificationGroupValue,
                                       docs: RDD[Seq[String]]): RDD[LabeledPoint] = {

    val modelLPs = TFiDFDictionary.featurizeDocuments(docs)
      .map(f => LabeledPoint(classificationGroup.classId, f))
    modelLPs
  }

  def saveSVMModel(sc: SparkContext,
                   modelFeatures: RDD[LabeledPoint],
                   numIterations: Int,
                   modelOutputDir: String): Unit = {
    val model = SVMWithSGD.train(modelFeatures, numIterations);
    model.save(sc, modelOutputDir)
  }

}
