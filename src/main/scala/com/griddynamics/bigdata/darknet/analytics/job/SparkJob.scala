package com.griddynamics.bigdata.darknet.analytics.job

import org.apache.spark.{SparkConf, SparkContext}

/**
  * TODO
  */
abstract class SparkJob {

  def main(args: Array[String]) {
    //initialize spark context
    val sc = {
      val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      val jarsOpt = SparkContext.jarOfObject(this);

      if (jarsOpt != None) {
        conf.setJars(List(jarsOpt.get))
      } else {
        conf.setMaster("local[4]")
      }
      new SparkContext(conf)
    }

    val result = execute(sc, args.toList)

    System.exit(if (result == 1) 0 else 1)
  }

  def execute(sc: SparkContext, args: List[String]): Int
}
