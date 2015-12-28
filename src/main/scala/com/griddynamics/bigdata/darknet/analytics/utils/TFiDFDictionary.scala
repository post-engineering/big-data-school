package com.griddynamics.bigdata.darknet.analytics.utils

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * TODO. FIXME
  */
object TFiDFDictionary extends Serializable with LazyLogging {


  def featurizeDocuments(docs: RDD[Seq[String]]): RDD[Vector] = {
    val tf = new HashingTF().transform(docs) //TODO tokenize
    val idf = new IDF().fit(tf);
    val tf_idf = idf.transform(tf)

    val normalized = new Normalizer().transform(tf_idf)
    val normalizedAndScaled = new feature.StandardScaler().fit(normalized).transform(normalized)

    normalizedAndScaled
  }

  def featurizeCategorizedDocuments(categorizedDocs: RDD[(Double, Seq[String])]): RDD[(Double, Vector)] = {
    val tf = categorizedDocs.map { case (k, v) => (k, new HashingTF().transform(v)) } //TODO tokenize
    val idf = new IDF().fit(tf.map { case (k, v) => v });
    val tf_idf = tf.map { case (k, v) => (k, idf.transform(v)) }

    val normalized: RDD[(Double, Vector)] = tf_idf.map { case (k, v) =>
      (k, new Normalizer().transform(v))
    }

    val scaleModel = new feature.StandardScaler().fit(normalized.map { case (k, v) => v })
    val normalizedAndScaled = normalized.map { case (k, v) => (k, scaleModel.transform(v)) }

    normalizedAndScaled
  }

  /* def indexOfTermInDictionary(term: String): Int = {
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
   }*/

}
