package com.griddynamics.bigdata.darknet.analytics.utils

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
    val featureVectors = featurizeDocuments(docs)
    featureVectors.saveAsTextFile(outputDir)
  }

  def featurizeDocuments(docs: RDD[String]): RDD[Vector] = {
    val docWords: RDD[Seq[String]] = docs.map(doc => doc.split("\\s").toSeq) //todo apply smart filter
    val tf: RDD[Vector] = new HashingTF().transform(docWords)
    val idf = new IDF().fit(tf)
    val tf_idf: RDD[Vector] = idf.transform(tf)
    tf_idf
  }

  def saveDocsAsLabeledPoints(classLabel: String, docs: RDD[String], outputDir: String): Unit = {
    val labeledPoints = buildLabeledPointsOfClassForDocs(classLabel, docs)
    MLUtils.saveAsLibSVMFile(labeledPoints, outputDir)
  }

  def saveDocsAsLabeledPoints(sc: SparkContext,
                              classLabel: String,
                              inputDir: String,
                              outputDir: String): Unit = {

    val labeledPoints = buildLabeledPointsOfClassForDocs(sc, classLabel, inputDir)
    labeledPoints.saveAsTextFile(outputDir)
  }

  def buildLabeledPointsOfClassForDocs(sc: SparkContext, classLabel: String, inputDir: String): RDD[LabeledPoint] = {
    val docs: RDD[String] = sc.wholeTextFiles(inputDir)
      .map(file => new JsoupExtractor().extractTextSafely(file._2))

    buildLabeledPointsOfClassForDocs(classLabel, docs)
  }

  def buildLabeledPointsOfClassForDocs(classLabel: String, docs: RDD[String]): RDD[LabeledPoint] = {
    val features: RDD[Vector] = featurizeDocuments(docs)
    features.map(f => LabeledPoint(ClassificationGroup.getLabelIdByName(classLabel), f))
  }

  def saveSVMModel(sc: SparkContext,
                   modelFeatures: RDD[LabeledPoint],
                   numIterations: Int,
                   modelOutputDir: String): Unit = {
    val model = SVMWithSGD.train(modelFeatures, numIterations);
    model.save(sc, modelOutputDir)
  }

}
