package com.griddynamics.bigdata.darknet.analytics.classification

import com.griddynamics.bigdata.darknet.analytics.utils.{AnalyticsUtils, ClassificationLabel, PdmlPayloadExtractor}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * TODO
  */
object UserRequestPredictor {

  /**
    * TODO
    *
    * @param modelTrainDataPath
    * @param testDataPath
    * @return
    */
  def predictCountOfClass(sc: SparkContext, classLabel: String, modelTrainDataPath: String, testDataPath: String): Long = {

    val classifiedLPs: RDD[LabeledPoint] = AnalyticsUtils.buildLabeledPointsOfClassForDocs(sc, classLabel, modelTrainDataPath)
    val testData: RDD[String] = PdmlPayloadExtractor.extractHtmlPayloadFromPDML(sc, testDataPath)

    //todo think of mapping between predicted vector and actual doc
    /*  val vecToDoc = testData.map { doc =>
        val docWords: Seq[String] = doc.split("\\s").toSeq
        val tf: Vector = new HashingTF().transform(docWords)

        (tf, doc)
      }.persist()*/

    val testFeatures: RDD[Vector] = AnalyticsUtils.featurizeDocuments(testData)

    val labelsAndFeatureVectors = predictCountOfClass(sc, classLabel, classifiedLPs, testFeatures)
    val count = labelsAndFeatureVectors.size

    count
  }

  /**
    * TODO
    *
    * @param modelLPs
    * @param testFeatures
    * @return
    */
  def predictCountOfClass(sc: SparkContext, classLabel: String, modelLPs: RDD[LabeledPoint], testFeatures: RDD[Vector]): Array[(Vector, Double)] = {
    val model = SVMWithSGD.train(modelLPs, 10);

    val featuresAndLabel = testFeatures.map { features =>
      val label = model.predict(features)
      (features, label)
    }.filter(p => p._2.equals(ClassificationLabel.getLabelIdByName(classLabel))).collect()

    featuresAndLabel
  }

  /**
    * TODO
    *
    * @param sc
    * @param classLabel
    * @param modelPath
    * @param testFeatures
    * @return
    */
  def predictCountOfClass(sc: SparkContext, classLabel: String, modelPath: String, testFeatures: RDD[Vector]): Array[(Vector, Double)] = {
    val model = SVMModel.load(sc, modelPath)

    val featuresAndLabel = testFeatures.map { features =>
      val label = model.predict(features)
      (features, label)
    }.filter(p => p._2.equals(ClassificationLabel.getLabelIdByName(classLabel))).collect()

    featuresAndLabel
  }
}



