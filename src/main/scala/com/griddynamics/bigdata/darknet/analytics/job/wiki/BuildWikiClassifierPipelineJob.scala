package com.griddynamics.bigdata.darknet.analytics.job.wiki

import com.griddynamics.bigdata.darknet.analytics.job.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{Row, SQLContext}


/**
  * The {@link SparkJob} job implementation provides functionality for building classifier based on Wiki mapping data
  */
object BuildWikiClassifierPipelineJob extends SparkJob {

  /**
    * Executes the job
    * @param sc predefined Spark context
    * @param args required job arguments:
    *             #1: path to Wiki Mapping
    *
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: String*): Int = {
    val mappingPath = args(0)
    //build model
    val categoryToArticleMapping = sc.objectFile[(String, Seq[String])](mappingPath)

    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val df = categoryToArticleMapping.toDF("category", "terms").cache()

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("label")

    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol("terms")
      .setOutputCol("tfFeatures")

    val idf = new IDF()
      .setInputCol("tfFeatures")
      .setOutputCol("tfidfFeatures")

    val normalizer = new Normalizer()
      .setInputCol("tfidfFeatures")
      .setOutputCol("normalizedFeatures")

    val scaller = new StandardScaler()
      .setInputCol("normalizedFeatures")
      .setOutputCol("scaledFeatures")

    val rf = new RandomForestClassifier()
      .setNumTrees(3)
      .setFeatureSubsetStrategy("auto")
      .setImpurity("gini")
      .setMaxDepth(5)
      .setMaxBins(32)
      .setMaxMemoryInMB(4000)
      .setFeaturesCol("scaledFeatures")


    val pipeline = new Pipeline()
      .setStages(Array(
        indexer,
        hashingTF,
        idf,
        normalizer,
        scaller,
        rf)
      )

    // Fit the pipeline to training documents.
    val model = pipeline.fit(df)

    val test = df.select("terms")

    // Make predictions on test documents.
    model.transform(test)
      .select("prediction")
      .collect()
      .foreach { case Row(prediction: Double) =>
        println(s"--> prediction=$prediction")
      }
    1
  }

}
