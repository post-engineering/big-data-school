package com.griddynamics.bigdata.darknet.analytics.utils

import com.griddynamics.bigdata.html.JsoupExtractor
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
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

  def tokenizeDocuments(docs: RDD[String]): RDD[Seq[String]] = {
    docs.map(doc => tokenizeDocument(doc))
  }

  def tokenizeDocument(doc: String): Seq[String] = {
    doc.toLowerCase.split("\\s").toSeq
  }


  def featurizeCategorizedDocuments(categorizedDocs: RDD[(Double, Seq[String])]): RDD[(Double, Vector)] = {
    val tf = categorizedDocs.map { case (k, v) => (k, new HashingTF().transform(v)) }
    val idf = new IDF().fit(tf.map { case (k, v) => v });
    val tf_idf = tf.map { case (k, v) => (k, idf.transform(v)) }

    val normalized: RDD[(Double, Vector)] = tf_idf.map { case (k, v) =>
      (k, new Normalizer().transform(v))
    }

    val scaleModel = new feature.StandardScaler().fit(normalized.map { case (k, v) => v })
    val normalizedAndScaled = normalized.map { case (k, v) => (k, scaleModel.transform(v)) }

    normalizedAndScaled
  }

  def saveDocsAsFeatureVectors(docs: RDD[String],
                               outputDir: String): Unit = {
    val featureVectors = featurizeDocuments(docs.map(doc => doc.split("\\s").toSeq))
    featureVectors.saveAsTextFile(outputDir)
  }

  def saveDocsAsLabeledPoints(classificationGroup: Double,
                              docs: RDD[Seq[String]],
                              outputDir: String): Unit = {
    val labeledPoints = buildLabeledPointsOfClassForDocs(classificationGroup, docs)
    MLUtils.saveAsLibSVMFile(labeledPoints, outputDir)
  }

  def buildLabeledPointsOfClassForDocs(classificationGroup: Double,
                                       docs: RDD[Seq[String]]): RDD[LabeledPoint] = {

    val modelLPs = featurizeDocuments(docs)
      .map(f => LabeledPoint(classificationGroup, f))
    modelLPs
  }

  def featurizeDocuments(docs: RDD[Seq[String]]): RDD[Vector] = {
    val tf = new HashingTF().transform(docs)
    val idf = new IDF().fit(tf);
    val tf_idf = idf.transform(tf)

    val normalized = new Normalizer().transform(tf_idf)
    val normalizedAndScaled = new feature.StandardScaler().fit(normalized).transform(normalized)

    normalizedAndScaled
  }

  def featurizeDocumentsWithContext(docs: RDD[Seq[String]]): RDD[(Seq[String], Vector)] = {
    val tfWithContext: RDD[(Seq[String], Vector)] = docs.map(doc => (doc, new HashingTF().transform(doc)))
    val idf = new IDF().fit(tfWithContext.map { case (k, v) => v });
    val tf_idf = tfWithContext.map { case (k, v) => (k, idf.transform(v)) }

    val normalized = tf_idf.map { case (k, v) =>
      (k, new Normalizer().transform(v))
    }

    val scaleModel = new feature.StandardScaler().fit(normalized.map { case (k, v) => v })
    val normalizedAndScaled = normalized.map { case (k, v) => (k, scaleModel.transform(v)) }

    normalizedAndScaled
  }

  def saveDocsAsLabeledPoints(sc: SparkContext,
                              classificationGroup: Double,
                              inputDir: String,
                              outputDir: String
                             ): Unit = {

    val labeledPoints = buildLabeledPointsOfClassForDocs(sc, classificationGroup, inputDir)
    labeledPoints.saveAsTextFile(outputDir)
  }

  def buildLabeledPointsOfClassForDocs(sc: SparkContext,
                                       classificationGroup: Double,
                                       inputDir: String
                                      ): RDD[LabeledPoint] = {
    val docs: RDD[String] = sc.wholeTextFiles(inputDir)
      .map(file => new JsoupExtractor().extractTextSafely(file._2))

    buildLabeledPointsOfClassForDocs(classificationGroup, docs.map(doc => doc.split("\\s")))
  }

}
