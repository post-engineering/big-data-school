package com.griddynamics.bigdata.darknet.analytics.classification

import com.griddynamics.bigdata.darknet.analytics.utils.ClassificationGroup.ClassificationGroupValue
import com.griddynamics.bigdata.darknet.analytics.utils.{AnalyticsUtils, PdmlPayloadExtractor, TFiDFDictionary}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * The class defines functions available for classification  of a User request type
  */
object UserRequestPredictor extends LazyLogging {

  /**
    * Predicts vectors that fir to the specified ClassificationGroup
    * TODO
    * @param modelTrainDataPath
    * @param testDataPath
    * @return
    */
  def predictForClass(sc: SparkContext,
                      classificationGroup: ClassificationGroupValue,
                      modelTrainDataPath: String,
                      testDataPath: String): RDD[(Vector, Double)] = {

    val classifiedLPs = AnalyticsUtils.buildLabeledPointsOfClassForDocs(sc, classificationGroup, modelTrainDataPath)
    val testData: RDD[String] = PdmlPayloadExtractor.extractHtmlPayloadFromPDML(sc, testDataPath)

    //todo think of mapping between predicted vector and actual doc
    /*  val vecToDoc = testData.map { doc =>
        val docWords: Seq[String] = doc.split("\\s").toSeq
        val tf: Vector = new HashingTF().transform(docWords)

        (tf, doc)
      }.persist()*/

    val testFeatures: RDD[Vector] = TFiDFDictionary.featurizeDocuments(testData.map(doc => doc.split("\\s").toSeq))

    val labelsAndFeatureVectors = predictForClass(sc, classificationGroup, classifiedLPs, testFeatures)
    labelsAndFeatureVectors
  }

  /**
    * The class defines functions available for classification  of a User request type
    * TODO
    * @param modelLPs
    * @param testFeatures
    * @return
    */
  def predictForClass(sc: SparkContext,
                      classificationGroup: ClassificationGroupValue,
                      modelLPs: RDD[LabeledPoint],
                      testFeatures: RDD[Vector]): RDD[(Vector, Double)] = {

    val model = SVMWithSGD.train(modelLPs, 100);

    val featuresAndLabel = testFeatures.map { features =>
      val label = model.predict(features)
      (features, label)
    }
    val ff = featuresAndLabel.collect()

    val hh = featuresAndLabel.filter(p => p._2.equals(classificationGroup.classId))

    var f = hh.collect()
    hh

  }

  /**
    * The class defines functions available for classification  of a User request type
    * TODO
    * @param sc
    * @param classificationGroup
    * @param modelPath
    * @param testFeatures
    * @return
    */
  def predictForClass(sc: SparkContext,
                      classificationGroup: ClassificationGroupValue,
                      modelPath: String,
                      testFeatures: RDD[Vector]): RDD[(Vector, Double)] = {

    val model = SVMModel.load(sc, modelPath)
    testFeatures.cache()

    val featuresAndLabel = testFeatures.map { features =>
      val label = model.predict(features)
      (features, label)
    }.filter(p => p._2.equals(classificationGroup.classId))

    featuresAndLabel
  }

}



