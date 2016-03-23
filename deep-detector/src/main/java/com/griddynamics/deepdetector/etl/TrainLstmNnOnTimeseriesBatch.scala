package com.griddynamics.deepdetector.etl

import com.griddynamics.deepdetector.SparkJob
import com.griddynamics.deepdetector.lstmnet.AnomalyDetectionNN
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext

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
    val ad = new AnomalyDetectionNN(sc, null, null, 200)
    val batchTsFilePath = args(0)
    val resultModelPath = args(1)

    logger.info(s"using ${batchTsFilePath} for Lstm NN training...")

    ad.trainAD(batchTsFilePath)
      .saveModel(resultModelPath)

    logger.info(s"model has been successfully built and saved to ${resultModelPath}")
    1
  }
}
