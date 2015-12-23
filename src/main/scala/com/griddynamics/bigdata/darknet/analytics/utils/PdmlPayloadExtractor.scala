package com.griddynamics.bigdata.darknet.analytics.utils

import com.griddynamics.bigdata.html.JsoupExtractor
import com.griddynamics.bigdata.input.pdml.PDMLInputFormat
import com.griddynamics.bigdata.util.PDMLUtil
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * The class provides operations for PDML-payload extraction
  */
object PdmlPayloadExtractor extends LazyLogging {

  def extractAndSaveDocuments(sc: SparkContext, input: String, output: String): Unit = {
    val data = extractHtmlPayloadFromPDML(sc, input)
    data.saveAsTextFile(output);
  }

  def extractHtmlPayloadFromPDML(sc: SparkContext, input: String): RDD[String] = {
    val htmlPayload: RDD[String] = sc.newAPIHadoopFile[LongWritable, BytesWritable, PDMLInputFormat](input)
      .map(pdmlPacket => new PDMLUtil().extractHTMLPayloadFromPacket(pdmlPacket._2.getBytes, 0, pdmlPacket._2.getLength))
      .filter(html => html != null && !html.isEmpty)
      .map(html => new JsoupExtractor().extractTextSafely(html))
      .filter(htmlPayload => htmlPayload != null && !htmlPayload.isEmpty)

    htmlPayload
  }

  def extractAndSaveAsFeatureVectors(sc: SparkContext, input: String, output: String): Unit = {
    val docs = extractHtmlPayloadFromPDML(sc, input)
    AnalyticsUtils.saveDocsAsFeatureVectors(docs, output)
  }

}
