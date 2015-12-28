package com.griddynamics.bigdata.darknet.analytics.utils

import com.griddynamics.bigdata.html.JsoupExtractor
import com.griddynamics.bigdata.input.pdml.PDMLInputFormat
import com.griddynamics.bigdata.util.PDMLUtil
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * The class provides operations for PDML-payload extraction
  */
object PdmlPayloadExtractor extends LazyLogging {

  def extractAndSaveDocumentsContent(sc: SparkContext, input: String, output: String): Unit = {
    val data = extractHtmlPayloadFromPDML(sc, input)
    data.saveAsTextFile(output);
  }

  def extractAndSaveAsFeatureVectors(sc: SparkContext, input: String, output: String): Unit = {
    val docs = extractHtmlPayloadFromPDML(sc, input)
    AnalyticsUtils.saveDocsAsFeatureVectors(docs, output)
  }

  def extractHtmlPayloadFromPDML(sc: SparkContext, input: String): RDD[String] = {
    val html = extractHtmlFromPDML(sc, input)
    val htmlPayload = html.map(html => new JsoupExtractor().extractTextSafely(html))
      .filter(htmlPayload => htmlPayload != null && !htmlPayload.isEmpty)
    htmlPayload
  }

  def extractHtmlFromPDML(sc: SparkContext, input: String): RDD[String] = {
    sc.newAPIHadoopFile[LongWritable, BytesWritable, PDMLInputFormat](input)
      .map(pdmlPacket => new PDMLUtil().extractHTMLPayloadFromPacket(pdmlPacket._2.getBytes, 0, pdmlPacket._2.getLength))
      .filter(html => html != null && !html.isEmpty)

  }

}
