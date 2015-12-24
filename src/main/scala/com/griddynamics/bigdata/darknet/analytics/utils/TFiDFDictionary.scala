package com.griddynamics.bigdata.darknet.analytics.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.mllib.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * TODO.
  */
class TFiDFDictionary(terms: RDD[String]) extends Serializable with LazyLogging {

  private lazy val indexToTerms = termsToIndex.map(_.swap)
  val normalizer = new Normalizer()
  val hashingTF = new HashingTF();

  // private val scaller = new StandardScaler()
  private val termsToIndex = terms.reduce((s1, s2) => s1 + " " + s2)
    .split("\\s").toSeq.map(s => s -> hashingTF.indexOf(s)).toMap

  def featurizeDocument(docs: RDD[Seq[String]]): RDD[Vector] = {
    val tf = hashingTF.transform(docs)
    val idf = new IDF().fit(tf);
    val tf_idf = idf.transform(tf)
    tf_idf
  }

  def indexOfTermInDictionary(term: String): Int = {
    termsToIndex(term)
  }

  def unfeaturizeVector(vectors: RDD[Vector]): RDD[String] = {
    vectors.map(v => unfeaturizeVector(v))
  }

  def unfeaturizeVector(vector: Vector): String = {
    val docTerms = new StringBuilder
    vector.toArray
      .zipWithIndex
      .foreach { case (x, i) => val term = valueOf(i)
        if (term != null) {
          docTerms.++=(term).+=('\t')
        }


      }

    val result = docTerms.+=('\n').toString()
    logger.debug(result)
    result
  }

  def valueOf(index: Int): String = {
    indexToTerms.getOrElse(index, null)
  }

}
