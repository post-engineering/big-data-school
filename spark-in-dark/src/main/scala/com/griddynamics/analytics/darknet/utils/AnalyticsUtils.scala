package com.griddynamics.analytics.darknet.utils

import java.awt.Color

import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.ngram.NgramExtractors
import com.optimaize.langdetect.profiles.LanguageProfileReader
import com.optimaize.langdetect.text.CommonTextObjectFactories
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import wordcloud.bg.{Background, CircleBackground}
import wordcloud.font.scale.{FontScalar, SqrtFontScalar}
import wordcloud.nlp.FrequencyAnalyzer
import wordcloud.palette.ColorPalette
import wordcloud.{CollisionMode, WordCloud, WordFrequency}

import scala.collection.JavaConverters._

/**
  * The utility class aggregates cross-cutting functions
  * that can be used for design of analytical jobs
  */
object AnalyticsUtils extends LazyLogging {

  //load all languages:
  val languageProfiles = new LanguageProfileReader().readAll();

  //build language detector:
  val languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
    .withProfiles(languageProfiles)
    .build();

  //create a text object factory
  val textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();

  /**
    * Featurizes categorized documents.
    * Applies normalization and scaling to vectors.
    * @param categorizedDocs rdd of categories mapped to documents
    * @return rdd of categories mapped to feature-vectors
    */
  def featurizeCategorizedDocuments(categorizedDocs: RDD[(Double, Seq[String])]): RDD[(Double, Vector)] = {
    val tf = categorizedDocs.map { case (k, v) => (k, new HashingTF().transform(v)) }
    val idf = new IDF().fit(tf.map { case (k, v) => v });
    val tf_idf = tf.map { case (k, v) => (k, idf.transform(v)) }

    val normalized: RDD[(Double, Vector)] = tf_idf.map { case (k, v) =>
      (k, new Normalizer().transform(v))
    }

    val scaleModel = new feature.StandardScaler().fit(normalized.map { case (k, v) => v })
    val normalizedAndScaled = normalized.map { case (k, v) => (k, scaleModel.transform(v)) }

    normalizedAndScaled
  }

  /**
    * Saves documents as feature-vectors to specified output directory.
    * @param docs
    * @param outputDir
    */
  def saveDocsAsFeatureVectors(docs: RDD[String],
                               outputDir: String): Unit = {
    val featureVectors = featurizeDocuments(tokenizeDocuments(docs))
    featureVectors.saveAsTextFile(outputDir)
  }

  /**
    * Tokenizes documents
    * @param docs rdd of docs
    * @return rdd of tokenized docs
    */
  def tokenizeDocuments(docs: RDD[String]): RDD[Seq[String]] = {
    docs.map(doc => tokenizeDocument(doc))
  }

  /**
    * Tokenizes document
    * @param doc
    * @return tokenized doc
    */
  def tokenizeDocument(doc: String): Seq[String] = {
    doc.toLowerCase.split("\\s").toSeq
  }

  /**
    * Featurizes  documents. Applies normalization and scaling to vectors.
    * @param docs rdd of documents
    * @return rdd of feature-vectors
    */
  def featurizeDocuments(docs: RDD[Seq[String]]): RDD[Vector] = {
    val tf = new HashingTF().transform(docs)
    val idf = new IDF().fit(tf);
    val tf_idf = idf.transform(tf)

    val normalized = new Normalizer().transform(tf_idf)
    val normalizedAndScaled = new feature.StandardScaler().fit(normalized).transform(normalized)

    normalizedAndScaled
  }

  /**
    * Builds labeled points from documents for specified category
    * @param classificationGroup
    * @param docs
    */
  def buildLabeledPointsOfClassForDocs(classificationGroup: Double,
                                       docs: RDD[Seq[String]]): RDD[LabeledPoint] = {

    val modelLPs = featurizeDocuments(docs)
      .map(f => LabeledPoint(classificationGroup, f))
    modelLPs
  }

  /**
    * Featurizes documents and maps the vectors to original documents.
    * Applies normalization and scaling to vectors.
    * @param docs rdd of  documents
    * @return rdd of feature-vectors mapped to original documents
    */
  def featurizeDocumentsAndMapToOriginal(docs: RDD[Seq[String]]): RDD[(Seq[String], Vector)] = {
    val tfWithContext: RDD[(Seq[String], Vector)] = docs.map(doc => (doc, new HashingTF().transform(doc)))
    val idf = new IDF().fit(tfWithContext.map { case (k, v) => v });
    val tf_idf = tfWithContext.map { case (k, v) => (k, idf.transform(v)) }

    val normalized = tf_idf.map { case (k, v) =>
      (k, new Normalizer().transform(v))
    }

    val scaleModel = new feature.StandardScaler().fit(normalized.map { case (k, v) => v })
    val normalizedAndScaled = normalized.map { case (k, v) => (k, scaleModel.transform(v)) }

    normalizedAndScaled
  }

  /**
    * TODO
    * @param text
    * @return
    */
  def detectLanguage(text: String): String = {
    //query:
    val textObject = textObjectFactory.forText(text);
    val lang = languageDetector.detect(textObject);
    lang.or("unknown")
  }

  /**
    * TODO
    * @param wc
    * @param output
    */
  def visualizeWordCloud(wc: RDD[(String, Int)], output: String): Unit = {

    visualizeWordCloud(wc,
      new CircleBackground(500),
      new ColorPalette(new Color(0x4055F1),
        new Color(0x408DF1),
        new Color(0x40AAF1),
        new Color(0x40C5F1),
        new Color(0x40D3F1),
        new Color(0xFFFFFF)),
      new SqrtFontScalar(10, 40),
      output
    )


  }

  /**
    * TODO
    * @param wc
    * @param bkgrd
    * @param colorPalette
    * @param fontScalar
    * @param output
    * @param topN
    */
  def visualizeWordCloud(wc: RDD[(String, Int)],
                         bkgrd: Background,
                         colorPalette: ColorPalette,
                         fontScalar: FontScalar,
                         output: String,
                         topN: Int = 700): Unit = {

    val wcList: List[WordFrequency] = wc.collect()
      .toSeq
      .map { case (k, v) => new WordFrequency(k, v) }
      .toList

    val frequencyAnalyzer = new FrequencyAnalyzer()
    frequencyAnalyzer.setWordFrequencesToReturn(topN)
    frequencyAnalyzer.setMinWordLength(4)

    val wordFrequencies = frequencyAnalyzer.loadWordFrequencies(wcList.asJava)
    val wordCloud = new WordCloud(1000, 1000, CollisionMode.PIXEL_PERFECT)
    wordCloud.setPadding(2)

    wordCloud.setBackground(bkgrd)
    wordCloud.setColorPalette(colorPalette)
    wordCloud.setFontScalar(fontScalar)

    wordCloud.build(wordFrequencies)
    wordCloud.writeToFile(output)
  }

}
