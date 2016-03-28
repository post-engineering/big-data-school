package com.griddynamics.deepdetector.lstmnet.utils

import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.factory.Nd4j

import scala.collection.mutable.ListBuffer

/**
  * TODO
  */
object CharFeaturizer extends TimelineFeaturizer[String] with Serializable {


  val validChars = {
    val chars = ListBuffer[Char]()

    for (c <- '0' to '9') {
      chars.+=(c)
    }

    chars.+=('.', ',', '(', ')', ':')
    chars.toList
  }

  val charToInt = validChars.zipWithIndex.toMap[Char, Int]
  val intToChar = charToInt.map(_.swap)

  //timelines must be of equal size
  def featurizeTimeline(timeline: String): DataSet = {

    println(s"tl of size ${timeline.length}: ${timeline}")
    val features: INDArray = Nd4j.zeros(1, charToInt.size, timeline.length -1)
    val labels: INDArray = Nd4j.zeros(1, charToInt.size, timeline.length - 1)

    val f = new Array[Int](3)
    val l = new Array[Int](3)

    for (i <- 0 to timeline.length - 2) {
      f(1) = charToInt.get(timeline.charAt(i)).get
      f(2) = i

      l(1) = charToInt.get(timeline.charAt(i + 1)).get
      l(2) = i
      features.putScalar(f, 1.0)
      labels.putScalar(l, 1.0)
    }

    new DataSet(features, labels)
  }


}
