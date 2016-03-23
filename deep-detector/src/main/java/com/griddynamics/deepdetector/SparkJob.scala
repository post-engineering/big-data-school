package com.griddynamics.deepdetector

import org.apache.spark.{SparkConf, SparkContext}
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer


/**
  * The base class for jobs running on Spark
  */
abstract class SparkJob {

  /**
    * Job entry point.
    * @param args job arguments
    */
  def main(args: Array[String]) {
    //initialize spark context
    val sc = {
      val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      val jarsOpt = SparkContext.jarOfObject(this)

      jarsOpt match {
        case Some(ops) => conf.setJars(List(ops))
        case None => {
          conf.setMaster("local[3]")//  //spark://172.26.5.43:7077
            .set("spark.executor.memory", "1g")
            .set("spark.driver.memory", "1g")
            .set(SparkDl4jMultiLayer.AVERAGE_EACH_ITERATION, String.valueOf(true))

            // conf.setJars(List("/home/ipertushin/IdeaProjects/big-data-school/spark-in-dark/target/big-deep-holms-1.0-SNAPSHOT.jar"))

        }

      }
      new SparkContext(conf)
    }

    execute(sc, args: _*)

  }

  /**
    * Executes job specific logic
    * @param sc predefined Spark context
    * @param args job arguments
    * @return status of job completion: '1' / '0' - success / failure
    */
  def execute(sc: SparkContext, args: String*): Int
}
