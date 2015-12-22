package com.griddynamics.bigdata.darknet.analytics.utils

/**
  * TODO
  */
object ClassificationLabel extends Enumeration {
  type ClassificationLabel = ClassificationLabelValue

  val Unclassified = ClassificationLabelValue("unclassified", 0.0)
  val Porn = ClassificationLabelValue("porn", 1.0)
  val Terrorism = ClassificationLabelValue("terrorism", 2.0)

  def getLabelNameById(classId: Double): String = {
    values.foreach { p =>
      val pCasted = p.asInstanceOf[ClassificationLabelValue]
      if (pCasted.classId.equals(classId)) {
        return pCasted.label
      }
    }
    ClassificationLabel.Unclassified.label
  }

  def getLabelIdByName(labelName: String): Double = {
    values.foreach { p =>
      val pCasted = p.asInstanceOf[ClassificationLabelValue]
      if (pCasted.label.equals(labelName)) {
        return pCasted.classId
      }
    }
    ClassificationLabel.Unclassified.classId
  }

  case class ClassificationLabelValue(label: String, classId: Double) extends Val(label)

}
