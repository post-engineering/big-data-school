package com.griddynamics.analytics.darknet.utils

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * The class defines base payload extraction functionality
  */
abstract class PayloadExtractor extends LazyLogging {

  /**
    * Extracts documents and builds feature-vectors
    * @param sc predefined instance of { @see SparkContext}
    * @param input path to raw data
    * @return rdd of feature-vectors
    */
  def extractPayloadAndFeaturize(sc: SparkContext, input: String): RDD[Vector] = {
    val docs = extractPayloadFromRawData(sc, input)
    val tokenizedDocs = AnalyticsUtils.tokenizeDocuments(docs)
    val features = AnalyticsUtils.featurizeDocuments(tokenizedDocs)

    logger.debug(s"Total number of extracted feature-vectors from $input is: ${features.count()}")
    features
  }

  /**
    * Extracts documents from raw data
    * @param sc predefined instance of { @see SparkContext}
    * @param input path to raw data
    * @return rdd of extracted documents
    */
  def extractPayloadFromRawData(sc: SparkContext, input: String): RDD[String]
}
