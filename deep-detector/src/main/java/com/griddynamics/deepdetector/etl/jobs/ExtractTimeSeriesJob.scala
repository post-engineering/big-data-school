package com.griddynamics.deepdetector.etl.jobs

import com.griddynamics.deepdetector.etl.SparkJob
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

/**
  * TODO
  */
object ExtractTimeSeriesJob extends SparkJob with LazyLogging {

  private[this] val TS_PATTERN = """(.*)\"dps\"\:\{(.+[^\}])\}""".r
  private[this] val DPS_PATTERN = """^\"(\d{1,})\":(\d{1,}[\.\d*]*)""".r

  /**
    * Executes job specific logic
    * @param sc predefined Spark context
    * @param args job arguments
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: String*): Int = {
    val filePath = args(0)
    loadTSFromFile(sc, filePath)
      .foreach(ts => logger.info(ts.toString()))
    1
  }

  /**
    * TODO
    * @param sc
    * @param filePath
    * @return
    */
  def loadTSFromFile(sc: SparkContext, filePath: String): Seq[(Long, Double)] = {
    sc.textFile(filePath)
      .map { s => extractTimeSeries(s) }
      .filter(s => s != null)
      .flatMap(identity)
      .sortByKey(true)
      .collect()
      .toSeq
  }


  /**
    * TODO
    * @param tsRaw
    * @return
    */
  def extractTimeSeries(tsRaw: String): Seq[(Long, Double)] = { //FIXME apply Option
    val ts = TS_PATTERN.findFirstMatchIn(tsRaw) match {
      case Some(v) => {
        v.group(2)
          .split(",")
          .map { dps =>
            DPS_PATTERN.findFirstMatchIn(dps) match {
              case Some(v) => Some((v.group(1).toLong, v.group(2).toDouble))
              case _ => None
            }
          }
         .map{case(ts) => ts.get}
         // .map { case (k, v) => if (v > 1.0) (k, 0.99) else (k, v) } //FIXME upper bound reached
          // .filter { case (k, v) => v < 1.0 } //FIXME
          .toSeq
      }
      case None => null
    }
    ts
  }
}
