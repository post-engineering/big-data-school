package com.griddynamics.analytics.darknet.utils

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * The utility class aggregates cross-cutting functions
  * that can be used for design of analytical jobs
  */
object AnalyticsUtils extends LazyLogging {

  /**
    * Featurizes categorized documents.
    * Applies normalization and scaling to vectors.
    * @param categorizedDocs rdd of categories mapped to documents
    * @return rdd of categories mapped to feature-vectors
    */
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

  /**
    * Saves documents as feature-vectors to specified output directory.
    * @param docs
    * @param outputDir
    */
  def saveDocsAsFeatureVectors(docs: RDD[String],
                               outputDir: String): Unit = {
    val featureVectors = featurizeDocuments(tokenizeDocuments(docs))
    featureVectors.saveAsTextFile(outputDir)
  }

  /**
    * Tokenizes documents
    * @param docs rdd of docs
    * @return rdd of tokenized docs
    */
  def tokenizeDocuments(docs: RDD[String]): RDD[Seq[String]] = {
    docs.map(doc => tokenizeDocument(doc))
  }

  /**
    * Tokenizes document
    * @param doc
    * @return tokenized doc
    */
  def tokenizeDocument(doc: String): Seq[String] = {
    doc.toLowerCase.split("\\s").toSeq
  }

  /**
    * Featurizes  documents. Applies normalization and scaling to vectors.
    * @param docs rdd of documents
    * @return rdd of feature-vectors
    */
  def featurizeDocuments(docs: RDD[Seq[String]]): RDD[Vector] = {
    val tf = new HashingTF().transform(docs)
    val idf = new IDF().fit(tf);
    val tf_idf = idf.transform(tf)

    val normalized = new Normalizer().transform(tf_idf)
    val normalizedAndScaled = new feature.StandardScaler().fit(normalized).transform(normalized)

    normalizedAndScaled
  }

  /**
    * Builds labeled points from documents for specified category
    * @param classificationGroup
    * @param docs
    */
  def buildLabeledPointsOfClassForDocs(classificationGroup: Double,
                                       docs: RDD[Seq[String]]): RDD[LabeledPoint] = {

    val modelLPs = featurizeDocuments(docs)
      .map(f => LabeledPoint(classificationGroup, f))
    modelLPs
  }

  /**
    * Featurizes documents and maps the vectors to original documents.
    * Applies normalization and scaling to vectors.
    * @param docs rdd of  documents
    * @return rdd of feature-vectors mapped to original documents
    */
  def featurizeDocumentsAndMapToOriginal(docs: RDD[Seq[String]]): RDD[(Seq[String], Vector)] = {
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

}
