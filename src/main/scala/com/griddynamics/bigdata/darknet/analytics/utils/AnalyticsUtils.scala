package com.griddynamics.bigdata.darknet.analytics.utils

import com.griddynamics.bigdata.darknet.analytics.utils.ClassificationGroup.ClassificationGroupValue
import com.griddynamics.bigdata.html.JsoupExtractor
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
  * The utility class aggregates cross-cutting functions
  * that can be used for design of analytical jobs
  */
object AnalyticsUtils extends LazyLogging {

  //TODO implement
  def tokenizeDocuments(docs: RDD[String]): RDD[Seq[String]] = {
    throw new UnsupportedOperationException
  }

  def tokenizeDocument(doc: String): Seq[String] = {
    throw new UnsupportedOperationException
  }


  /*  def featurizeDocument(doc: Seq[String]): RDD[Vector] = {
      val tf = new HashingTF().transform(docs) //TODO tokenize?
      val idf = new IDF().fit(tf);
      val tf_idf = idf.transform(tf)

      val normalized = new Normalizer().transform(tf_idf)
      val normalizedAndScaled = new feature.StandardScaler().fit(normalized).transform(normalized)

      normalizedAndScaled
    }*/

  def featurizeCategorizedDocuments(categorizedDocs: RDD[(Double, Seq[String])]): RDD[(Double, Vector)] = {
    val tf = categorizedDocs.map { case (k, v) => (k, new HashingTF().transform(v)) } //TODO tokenize?
    val idf = new IDF().fit(tf.map { case (k, v) => v });
    val tf_idf = tf.map { case (k, v) => (k, idf.transform(v)) }

    val normalized: RDD[(Double, Vector)] = tf_idf.map { case (k, v) =>
      (k, new Normalizer().transform(v))
    }

    val scaleModel = new feature.StandardScaler().fit(normalized.map { case (k, v) => v })
    val normalizedAndScaled = normalized.map { case (k, v) => (k, scaleModel.transform(v)) }

    normalizedAndScaled
  }

  //TODO
  def saveDocsAsFeatureVectors(docs: RDD[String],
                               outputDir: String): Unit = {
    val featureVectors = featurizeDocuments(docs.map(doc => doc.split("\\s").toSeq))
    featureVectors.saveAsTextFile(outputDir)
  }

  def featurizeDocuments(docs: RDD[Seq[String]]): RDD[Vector] = {
    val tf = new HashingTF().transform(docs) //TODO tokenize?
    val idf = new IDF().fit(tf);
    val tf_idf = idf.transform(tf)

    val normalized = new Normalizer().transform(tf_idf)
    val normalizedAndScaled = new feature.StandardScaler().fit(normalized).transform(normalized)

    normalizedAndScaled
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

    val modelLPs = featurizeDocuments(docs)
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
