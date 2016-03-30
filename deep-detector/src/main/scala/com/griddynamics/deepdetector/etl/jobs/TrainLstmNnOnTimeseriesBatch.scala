package com.griddynamics.deepdetector.etl.jobs

import com.griddynamics.deepdetector.etl.SparkJob
import com.griddynamics.deepdetector.lstmnet.AnomalyDetectionNNOnChars
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

import scala.reflect.io.File

/**
  * TODO
  */
object TrainLstmNnOnTimeseriesBatch extends SparkJob with LazyLogging {
  /**
    * Executes job specific logic
    * @param sc predefined Spark context
    * @param args job arguments
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: String*): Int = {
    val ad = new AnomalyDetectionNNOnChars(sc, null, null)
    val batchTsFilePath = args(0)
    val resultModelPath = args(1)

    File(resultModelPath).createDirectory(true)

    logger.info(s"using ${batchTsFilePath} for Lstm NN training...")

    ad.trainAD(batchTsFilePath)
      .saveModel(resultModelPath)

    logger.info(s"model has been successfully built and saved to ${resultModelPath}")
    1
  }
}
