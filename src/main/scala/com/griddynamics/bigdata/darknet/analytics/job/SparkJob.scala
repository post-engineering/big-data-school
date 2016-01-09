package com.griddynamics.bigdata.darknet.analytics.job

import org.apache.spark.{SparkConf, SparkContext}

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
          conf.setMaster("local[5]")
            .set("spark.executor.memory", "2g")
            .set("spark.driver.memory", "2g")
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
