package com.griddynamics.bigdata.darknet.analytics.utils

import com.griddynamics.bigdata.html.JsoupExtractor
import com.griddynamics.bigdata.input.xml.XmlInputFormat
import com.griddynamics.bigdata.util.PdmlUtil
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * The class provides operations for PDML-payload extraction
  */
object PdmlPayloadExtractor extends LazyLogging {

  private val XML_START_TAG = "<packet>"
  private val XML_END_TAG = "</packet>"

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

    val hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(sc.getConf)
    hadoopConfiguration.set(XmlInputFormat.CONF_XML_NODE_START_TAG, XML_START_TAG)
    hadoopConfiguration.set(XmlInputFormat.CONF_XML_NODE_END_TAG, XML_END_TAG)
    hadoopConfiguration.set("io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization," +
        "org.apache.hadoop.io.serializer.WritableSerialization")

    sc.newAPIHadoopFile(
      input,
      ClassTag.apply(classOf[XmlInputFormat]).runtimeClass.asInstanceOf[Class[XmlInputFormat]],
      ClassTag.apply(classOf[LongWritable]).runtimeClass.asInstanceOf[Class[LongWritable]],
      ClassTag.apply(classOf[Text]).runtimeClass.asInstanceOf[Class[Text]],
      conf = hadoopConfiguration)
      .map(pdmlPacket => new PdmlUtil().extractHTMLPayloadFromPacket(pdmlPacket._2.getBytes, 0, pdmlPacket._2.getLength))
      .filter(html => html != null && !html.isEmpty)

  }

}
