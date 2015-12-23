package com.griddynamics.bigdata.darknet.analytics.utils

/**
  * The enumeration declares all available Classification groups
  */
object ClassificationGroup extends Enumeration {
  type ClassificationGroup = ClassificationGroupValue

  val Unclassified = ClassificationGroupValue("unclassified", 0.0)
  val Porn = ClassificationGroupValue("porn", 1.0)
  val Terrorism = ClassificationGroupValue("terrorism", 2.0)
  //TODO more?

  def getLabelNameById(classId: Double): String = {
    values.foreach { p =>
      val pCasted = p.asInstanceOf[ClassificationGroupValue]
      if (pCasted.classId.equals(classId)) {
        return pCasted.label
      }
    }
    ClassificationGroup.Unclassified.label
  }

  def getLabelIdByName(labelName: String): Double = {
    values.foreach { p =>
      val pCasted = p.asInstanceOf[ClassificationGroupValue]
      if (pCasted.label.equals(labelName)) {
        return pCasted.classId
      }
    }
    ClassificationGroup.Unclassified.classId
  }

  case class ClassificationGroupValue(label: String, classId: Double) extends Val(label)

}
