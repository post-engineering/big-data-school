package com.griddynamics.deepdetector.lstmnet

import java.io.IOException
import java.lang
import java.util.Random

import com.griddynamics.deepdetector.etl.jobs.ExtractTimeSeriesJob
import com.griddynamics.deepdetector.lstmnet.utils.{CharFeaturizer, ModelUtils}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.distribution.UniformDistribution
import org.deeplearning4j.nn.conf.layers.{GravesLSTM, RnnOutputLayer}
import org.deeplearning4j.nn.conf.{BackpropType, MultiLayerConfiguration, NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction

import scala.collection.mutable.ListBuffer
import scala.reflect.io.File

/**
  * TODO
  */
class AnomalyDetectionNNOnChars(sc: SparkContext,
                                pathToExistingModelConf: Option[String],
                                pathToExistingModelParam: Option[String]) extends LazyLogging {


  private[this] val LSTM_N_NET = startNN()
  private[this] var lastTS: Long = 0L //FIXME workaround

  def this(sc: SparkContext, modelbasePath: String) =
    this(sc,
      Option(ModelUtils.lookupAnyFileByNameEnding(modelbasePath, "conf.json").getAbsolutePath),
      Option(ModelUtils.lookupAnyFileByNameEnding(modelbasePath, ".bin").getAbsolutePath)
    )

  def startNN(): SparkDl4jMultiLayer = {

    var net: SparkDl4jMultiLayer = null

    if (pathToExistingModelConf != null && pathToExistingModelParam != null) {
      //load existing
      net = loadModel(pathToExistingModelConf.get, pathToExistingModelParam.get)
    } else {
      //build from scratch
      val lstm = setupNetwork(sc, CharFeaturizer.charToInt.size, CharFeaturizer.charToInt.size)
      net = new SparkDl4jMultiLayer(sc, lstm)
    }
    net
  }

  def loadModel(confPath: String, paramPath: String): SparkDl4jMultiLayer = {
    val lstm = ModelUtils.loadModelAndParameters(confPath, paramPath)
    lstm.setUpdater(null) //TODO adjust in case of using any updaters in the feature
    new SparkDl4jMultiLayer(sc, lstm)
  }

  /**
    * TODO move configuration parameters for layers to a json file
    * @param sc
    * @return
    */
  private def setupNetwork(sc: SparkContext,
                           nIn: Int,
                           nOut: Int) = {


    sc.getConf.set(SparkDl4jMultiLayer.AVERAGE_EACH_ITERATION, String.valueOf(true))

    val lstmLayerSize = 200

    val truncatedBPTTLength: Int = 100

    //Set up network configuration:
    val conf: MultiLayerConfiguration = new NeuralNetConfiguration.Builder()
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .iterations(1)
      .learningRate(0.1)
      .rmsDecay(0.95)
      .seed(12345)
      .regularization(true)
      .l2(0.001)
      .list(5)
      .layer(0, new GravesLSTM.Builder()
        .nIn(nIn)
        .nOut(lstmLayerSize)
        .updater(Updater.RMSPROP)
        .activation("tanh")
        .weightInit(WeightInit.DISTRIBUTION)
        .dist(new UniformDistribution(-0.08, 0.08))
        .build()
      )
      .layer(1, new GravesLSTM.Builder()
        .nIn(lstmLayerSize)
        .nOut(lstmLayerSize)
        .updater(Updater.RMSPROP)
        .activation("tanh")
        .weightInit(WeightInit.DISTRIBUTION)
        .dist(new UniformDistribution(-0.08, 0.08))
        .build()
      )
      .layer(2, new GravesLSTM.Builder()
        .nIn(lstmLayerSize)
        .nOut(lstmLayerSize)
        .updater(Updater.RMSPROP)
        .activation("tanh")
        .weightInit(WeightInit.DISTRIBUTION)
        .dist(new UniformDistribution(-0.08, 0.08))
        .build()
      )
      .layer(3, new GravesLSTM.Builder()
        .nIn(lstmLayerSize)
        .nOut(lstmLayerSize)
        .updater(Updater.RMSPROP)
        .activation("tanh")
        .weightInit(WeightInit.DISTRIBUTION)
        .dist(new UniformDistribution(-0.08, 0.08))
        .build()
      )
      .layer(4, new RnnOutputLayer.Builder(LossFunction.MCXENT)
        .activation("softmax")
        .updater(Updater.RMSPROP)
        .nIn(lstmLayerSize)
        .nOut(nOut)
        .weightInit(WeightInit.DISTRIBUTION)
        .dist(new UniformDistribution(-0.08, 0.08))
        .build()
      )
      .pretrain(false)
      .backprop(true)
      .backpropType(BackpropType.TruncatedBPTT)
      .tBPTTForwardLength(truncatedBPTTLength)
      .tBPTTBackwardLength(truncatedBPTTLength)
      .build()

    val net = new MultiLayerNetwork(conf)
    net.init()
    net.setUpdater(null) //Workaround for a minor bug in 0.4-rc3.8

    //  net.setListeners(Collections.singletonList(new ScoreIterationListener().asInstanceOf[IterationListener]))
    net
  }

  /**
    * TODO
    * @param path
    */
  def saveModel(path: String) = {
    ModelUtils.saveModelAndParameters(LSTM_N_NET.getNetwork, path)
  }

  /**
    * TODO
    * @param pathToTimeSeriesBatch
    * @return
    */
  def trainAD(pathToTimeSeriesBatch: String): AnomalyDetectionNNOnChars = {
    //val nCores = sc.getConf.get("spark.executor.cores").toInt
    val nCores = 4 //FIXME  fetch actual value from Sparkconf
    val sparkExamplesPerFit: Int = 20 * nCores

    //Length of each sequence (used in truncated BPTT)
    val timelineLength = 500


    //loadTS
    val rawTs = loadTimeSeriesFromLocalBatchFile(sc, pathToTimeSeriesBatch)
    val ts = timestempToInterval(rawTs)

    logger.info(s"total number of loaded timelines : ${ts.size}")

    val rddTs = sc.parallelize(
      timelineToString(ts)
        .grouped(timelineLength)
        .toSeq
    )

    rddTs.persist(StorageLevel.MEMORY_ONLY)

    //Do training, and then generate and print samples from network
    //for (i <- 0 to numIterations) {
    //TODO apply iterative training


    /*   Split the Time Series to relatively short timeline batches and convert them to vector representations.
         Conversion of full data set at once (and re-use it)  by
         SparkDl4jMultiLayer.fitDataSet(data : RDD[DataSet], examplesPerFit:Int) method takes much more memory.
    */
    val timelineBatches = splitTimeSriesToTimelines(rddTs, sparkExamplesPerFit)


    val featurizer = sc.broadcast(CharFeaturizer)

    val numEpochs = 5


    for (i <- 0 to numEpochs) {

      logger.info(s"epoch #${i}")

      for (batch <- timelineBatches) {
        val tsVectorized = batch.map { case (timeline) =>
          featurizer.value.featurizeTimeline(timeline)
        }

        LSTM_N_NET.fitDataSet(tsVectorized)
      }

      logger.info(s"model score: #${LSTM_N_NET.getScore}")
    }

    this
  }

  /**
    * TODO
    * @param sc
    * @param pathToTimeSeriesBatchfile
    * @throws java.io.IOException
    * @return
    */
  @throws(classOf[IOException])
  private def loadTimeSeriesFromLocalBatchFile(sc: SparkContext,
                                               pathToTimeSeriesBatchfile: String
                                              ): Seq[(Long, Double)] = {

    // val fileLocation = "/home/ipertushin/Documents/ts_proc.loadavg.1min_all" //FIXME
    //"ts_proc.loadavg.1min.txt"
    //ts_proc.loadavg.1min_host=ipetrushin

    val f = File(pathToTimeSeriesBatchfile)
    if (!f.exists) {
      logger.error(s"can't find the file for loading batch timeseries: ${pathToTimeSeriesBatchfile}")
      throw new IOException("no such file of TS")
    }
    else {
      logger.info(s"the timeseries batch file has been found: ${pathToTimeSeriesBatchfile}")
    }
    val allData = ExtractTimeSeriesJob.loadTSFromFile(sc, pathToTimeSeriesBatchfile)
    logger.info(s"total number of extracted time steps : ${allData.size}")

    allData

  }

  /**
  Splits the TimeSeries RDD to short timelines.
    * @param timeSeries
    * @param timeStepsPerTimeline
    * @return
    */
  private def splitTimeSriesToTimelines(timeSeries: RDD[String], timeStepsPerTimeline: Int): Array[RDD[String]] = {
    var nSplits = 0
    val nExamples: Long = timeSeries.count
    if (nExamples % timeStepsPerTimeline == 0) {
      nSplits = (nExamples / timeStepsPerTimeline).toInt
    }
    else {
      nSplits = (nExamples / timeStepsPerTimeline).toInt + 1
    }
    val splitWeights = new Array[Double](nSplits)

    for (i <- 0 to nSplits - 1) {
      splitWeights(i) = 1.0 / nSplits
    }

    return timeSeries.randomSplit(splitWeights)
  }

  /**
    * TODO
    * @param recentTimeline
    * @param predictionIntervalInMs
    * @return
    */
  def predictNextTimeLineIntervalBasedOnRecentState(recentTimeline: Seq[(Long, Double)],
                                                    predictionIntervalInMs: Long): Seq[(Long, Double)] = {
    val numberOfTimelinesToGenerate = 1
    val rng = new Random(12345)

    logger.info("--------------------")
    logger.info(s"Make prediction for the timeline: ${recentTimeline.mkString(" -> ")}")
    val predictedTimeline = predictNextTimeline(
      recentTimeline,
      LSTM_N_NET.getNetwork,
      rng,
      CharFeaturizer.charToInt.size,
      predictionIntervalInMs
    )

    predictedTimeline
  }

  /**
    * Generate a sample from the network, given an (optional, possibly null) initializationTimeline. Initialization
    * can be used to 'prime' the RNN with a sequence you want to extend/continue.<br>
    * Note that the initalization is used for all samples
    *
    * @param initTimeline  String, may be null. If null, select a random character as initializationTimeline for all samples
    * @param  predictionIntervalInSec interval of tsSteps to sample from network (excluding initializationTimeline)
    * @param net             MultiLayerNetwork with one or more GravesLSTM/RNN layers and a softmax output layer
    */
  private def predictNextTimeline(initTimeline: Seq[(Long, Double)],
                                  net: MultiLayerNetwork,
                                  rng: Random,
                                  featureVectorSize: Int,
                                  predictionIntervalInSec: Long
                                 ): Seq[(Long, Double)] = {

    val nSamples = 1;
    //vectorize initialization Timeline
    val initCharTimeline = timelineToString(timestempToInterval(initTimeline))
    logger.info(s"ts converted to interval: ${initCharTimeline}")

    val vectorizedInitTimeline: INDArray = Nd4j.zeros(1, featureVectorSize, initCharTimeline.length)
    for (i <- 0 to initCharTimeline.length - 1) {
      val idx: Int = CharFeaturizer.charToInt.get(initCharTimeline.charAt(i)).get
      vectorizedInitTimeline.putScalar(Array[Int](0, idx, i), 1.0f)
    }

    // net.rnnClearPreviousState //FIXME
    var output: INDArray = net.rnnTimeStep(vectorizedInitTimeline)
    output = output.tensorAlongDimension(output.size(2) - 1, 1, 0) //Gets the last time step output

    //sampling
    val acquiredTimeline = ListBuffer[(Long, Double)]()

    while (!isTimeLineAcquired(acquiredTimeline, predictionIntervalInSec)) {

      var tsIsAcquired = false
      val predictedTimeStepStr = new StringBuilder

      while (!tsIsAcquired) {
        val nextInput = Nd4j.zeros(1, featureVectorSize)

        val outputProbDistribution = new Array[Double](featureVectorSize)

        for (j <- 0 to outputProbDistribution.length - 1) {
          outputProbDistribution(j) = output.getDouble(0, j)
        }

        val sampledTsStepIdx: Int = sampleFromDistribution(outputProbDistribution, rng)
        nextInput.putScalar(Array[Int](0, sampledTsStepIdx), 1.0f)
        val sampleResult = CharFeaturizer.intToChar.get(sampledTsStepIdx).get

        predictedTimeStepStr += sampleResult

        output = net.rnnTimeStep(nextInput)

        val ts = extractTimeStep(predictedTimeStepStr.toString())
        ts match {
          case Some(ts) => {
            tsIsAcquired = true
            acquiredTimeline.+=(ts)
          }
          case None =>
        }
      }

    }

    acquiredTimeline.toSeq
  }

  /**
    * TODO
    * @param timeseries
    * @return
    */
  def timestempToInterval(timeseries: Seq[(Long, Double)]): Seq[(Long, Double)] = {
    //convert to intervals
    val buf = ListBuffer[(Long, Double)]()

    //FIXME: the ugly workaround for first timeStep interval value
    if (lastTS == 0L) {
      buf.+=((0, timeseries(0)._2))
    } else {
      buf.+=((timeseries(0)._1 - lastTS, timeseries(0)._2))
    }

    for (i <- 1 to timeseries.size - 1) {
      val interval = timeseries(i)._1 - timeseries(i - 1)._1
      buf.+=((interval, timeseries(i)._2))
    }

    lastTS = timeseries.last._1
    buf.toSeq
  }

  def timelineToString(timeline: Seq[(Long, Double)]): String = {
    timeline
      .map { case ((ts, m)) =>
        s"(${ts}:${m.formatted("%.2f")})"
      }
      .mkString(",")
  }

  private def isTimeLineAcquired(timeline: Seq[(Long, Double)], targetInterval: Long): Boolean = {
    if (timeline == null || timeline.size == 0)
      return false

    val actual = timeline.map { case (i, m) => i }.reduce((i1, i2) => i1 + i2)
    (actual >= targetInterval)
  }

  private def extractTimeStep(tsSrt: String): Option[(Long, Double)] = {
    val p = """.*\((\d{1,})\:(\d{1,}\.\d{1,}){1}\).*""".r
    p findFirstMatchIn tsSrt match {
      case Some(m) => {
        Some((lang.Long.parseLong(m.group(1)), lang.Double.parseDouble(m.group(2))))
      }
      case _ => None
    }
  }

  /**
    * Given a probability distribution over discrete classes, sample from the distribution
    * and return the generated class index.
    *
    * @param distribution Probability distribution over classes. Must sum to 1.0
    */
  private def sampleFromDistribution(distribution: Array[Double], rng: Random): Int = {
    val d = rng.nextDouble
    var sum = 0.0

    for (i <- 0 to distribution.length) {
      sum += distribution(i)
      if (d <= sum) return i
    }

    throw new IllegalArgumentException("Distribution is invalid? d=" + d + ", sum=" + sum)
  }

  def calculateIntervalValue(predictedTimeline: String): Unit = {

  }

  /**
    * TODO
    * @param sc
    * @param pathToTimeSeriesBatchfile
    * @throws java.io.IOException
    * @return
    */
  @throws(classOf[IOException])
  private def loadTimeSeriesFromBatchFile(sc: SparkContext,
                                          pathToTimeSeriesBatchfile: String
                                         ): Seq[(Long, Double)] = {


    val allData = ExtractTimeSeriesJob.loadTSFromFile(sc, pathToTimeSeriesBatchfile)
    logger.info(s"total number of extracted time steps : ${allData.size}")

    allData

  }

  /**
    * TODO
    * @param sc
    * @param pathToTimeSeriesBatchfile
    * @throws java.io.IOException
    * @return
    */
  @throws(classOf[IOException])
  private def loadTimeSeriesRDDFromBatchFile(sc: SparkContext,
                                             pathToTimeSeriesBatchfile: String
                                            ): RDD[(Long, Double)] = {

    val allData = ExtractTimeSeriesJob.loadTSRDDFromFile(sc, pathToTimeSeriesBatchfile)
    allData

  }


}

