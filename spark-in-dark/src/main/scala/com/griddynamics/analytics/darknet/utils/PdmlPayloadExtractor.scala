package com.griddynamics.analytics.darknet.utils

import com.griddynamics.analytics.darknet.html.JsoupExtractor
import com.griddynamics.analytics.darknet.input.xml.XmlInputFormat
import com.griddynamics.analytics.darknet.util.PdmlUtil
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * The {@link PayloadExtractor} implementation provides functionality for extraction of PDML-dump payload data.
  */
object PdmlPayloadExtractor extends PayloadExtractor with LazyLogging {

  private val XML_START_TAG = "<packet>"
  private val XML_END_TAG = "</packet>"

  override def extractDocumentsFromRawData(sc: SparkContext, input: String): RDD[String] = {
    val packets = extractPacketFromPDML(sc, input)
    val htmlPayload = packets
      .filter(packet => packet != null && !packet.isEmpty)
      .map(packet => new JsoupExtractor().extractTextSafely(packet))

    htmlPayload
  }

  def extractPacketFromPDML(sc: SparkContext, input: String): RDD[String] = {

    val hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(sc.getConf)
    hadoopConfiguration.set(XmlInputFormat.CONF_XML_NODE_START_TAG, XML_START_TAG)
    hadoopConfiguration.set(XmlInputFormat.CONF_XML_NODE_END_TAG, XML_END_TAG)
    hadoopConfiguration.set("io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization," +
        "org.apache.hadoop.io.serializer.WritableSerialization")

    val packets = sc.newAPIHadoopFile(
      input,
      ClassTag.apply(classOf[XmlInputFormat]).runtimeClass.asInstanceOf[Class[XmlInputFormat]],
      ClassTag.apply(classOf[LongWritable]).runtimeClass.asInstanceOf[Class[LongWritable]],
      ClassTag.apply(classOf[Text]).runtimeClass.asInstanceOf[Class[Text]],
      conf = hadoopConfiguration)
      .map { case (pdmlPacket) =>
        new PdmlUtil().extractHTMLPayloadFromPacket(pdmlPacket._2.getBytes, 0, pdmlPacket._2.getLength)
      }

    logger.debug(s"Total number of extracted packets from $input is: ${packets.count()}")
    packets
  }

}
