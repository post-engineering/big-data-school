package com.griddynamics.bigdata.darknet.analytics.utils

import com.griddynamics.bigdata.darknet.analytics.utils.ClassificationGroup.ClassificationGroupValue
import com.griddynamics.bigdata.html.JsoupExtractor
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
  * The utility class aggregates cross-cutting functions
  * that can be used for design of analytical jobs
  */
object AnalyticsUtils extends LazyLogging {

  def saveDocsAsFeatureVectors(docs: RDD[String], outputDir: String): Unit = {
    featurizeDocuments(docs)
      .saveAsTextFile(outputDir)
  }

  def featurizeDocuments(docs: RDD[String]): RDD[Vector] = {
    val docWords: RDD[Seq[String]] = docs.map(doc => doc.split("\\s").toSeq) //todo apply smart filter
    val tf: RDD[Vector] = new HashingTF().transform(docWords)
    val idf = new IDF().fit(tf)
    val tf_idf: RDD[Vector] = idf.transform(tf)
    tf_idf
  }

  //TODO
  def saveDocsAsFeatureVectors(docs: RDD[String],
                               outputDir: String,
                               termDictionary: TFiDFDictionary): Unit = {
    val featureVectors = featurizeDocuments(docs, termDictionary)
    featureVectors.saveAsTextFile(outputDir)
  }

  def saveDocsAsLabeledPoints(classificationGroup: ClassificationGroupValue,
                              docs: RDD[String],
                              outputDir: String): Unit = {
    val labeledPoints = buildLabeledPointsOfClassForDocs(classificationGroup, docs)._2
    MLUtils.saveAsLibSVMFile(labeledPoints, outputDir)
  }

  def saveDocsAsLabeledPoints(sc: SparkContext,
                              classificationGroup: ClassificationGroupValue,
                              inputDir: String,
                              outputDir: String
                             ): Unit = {

    val labeledPoints = buildLabeledPointsOfClassForDocs(sc, classificationGroup, inputDir)._2
    labeledPoints.saveAsTextFile(outputDir)
  }

  def buildLabeledPointsOfClassForDocs(sc: SparkContext,
                                       classificationGroup: ClassificationGroupValue,
                                       inputDir: String
                                      ): (TFiDFDictionary, RDD[LabeledPoint]) = {
    val docs: RDD[String] = sc.wholeTextFiles(inputDir)
      .map(file => new JsoupExtractor().extractTextSafely(file._2))

    buildLabeledPointsOfClassForDocs(classificationGroup, docs)
  }

  def buildLabeledPointsOfClassForDocs(classificationGroup: ClassificationGroupValue,
                                       docs: RDD[String]): (TFiDFDictionary, RDD[LabeledPoint]) = {
    val termDictionary = new TFiDFDictionary(docs)
    val modelLPs = featurizeDocuments(docs, termDictionary)
      .map(f => LabeledPoint(classificationGroup.classId, f))
    (termDictionary, modelLPs)
  }

  //TODO
  def featurizeDocuments(docs: RDD[String], dictionary: TFiDFDictionary): RDD[Vector] = {
    dictionary.featurizeDocument(docs.map(doc => doc.split("\\s").toSeq))
  }

  def saveSVMModel(sc: SparkContext,
                   modelFeatures: RDD[LabeledPoint],
                   numIterations: Int,
                   modelOutputDir: String): Unit = {
    val model = SVMWithSGD.train(modelFeatures, numIterations);
    model.save(sc, modelOutputDir)
  }

}
