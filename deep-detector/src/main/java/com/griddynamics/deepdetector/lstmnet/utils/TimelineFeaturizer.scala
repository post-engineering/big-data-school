package com.griddynamics.deepdetector.lstmnet.utils

import org.nd4j.linalg.dataset.DataSet

/**
  * TODO
  */
trait TimelineFeaturizer[T] {

  //timelines must be of equal size
  def featurizeTimeline(timeline: T): DataSet
}
