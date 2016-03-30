package com.griddynamics.deepdetector.lstmnet

import java.io.IOException
import java.util.Random
import com.griddynamics.deepdetector.etl.jobs.ExtractTimeSeriesJob
import com.griddynamics.deepdetector.lstmnet.utils.ModelUtils
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
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction

import scala.collection.mutable.ListBuffer
import scala.reflect.io.File

@Deprecated
class AnomalyDetectionNNOnInt(sc: SparkContext,
                              pathToExistingModelConf: Option[String],
                              pathToExistingModelParam: Option[String],
                              featureVectorSize: Int = 200,
                              scaleFactor: Int = 100) extends Serializable with LazyLogging {


  private[this] val LSTM_N_NET = startNN()

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
      val lstm = setupNetwork(sc, featureVectorSize, featureVectorSize)
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
      .iterations(500)
      .learningRate(0.1)
      .rmsDecay(0.95)
      .seed(12345)
      .regularization(true)
      .l2(0.001)
      .list(3)
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
      .layer(2, new RnnOutputLayer.Builder(LossFunction.MCXENT)
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
    net.setUpdater(null)   //Workaround for a minor bug in 0.4-rc3.8

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
  def trainAD(pathToTimeSeriesBatch: String): AnomalyDetectionNNOnInt = {
    //val nCores = sc.getConf.get("spark.executor.cores").toInt
    val nCores = 4 //FIXME  fetch actual value from Sparkconf
    val sparkExamplesPerFit: Int = 20 * nCores

    //Length of each sequence (used in truncated BPTT)
    val timelineLength = 200

    val scale = scaleFactor //FIXME
    val inSize = featureVectorSize

    //loadTS
    val rawTs = getTimeSeriesAsListOfTimelines(sc, pathToTimeSeriesBatch, timelineLength)
    logger.info(s"total number of loaded timelines : ${rawTs.size}")
    val ts = sc.parallelize(rawTs)
    ts.persist(StorageLevel.MEMORY_ONLY)

    //Do training, and then generate and print samples from network
    //for (i <- 0 to numIterations) {
    //TODO apply iterative training


    /*   Split the Time Series to relatively short timeline batches and convert them to vector representations.
         Conversion of full data set at once (and re-use it)  by
         SparkDl4jMultiLayer.fitDataSet(data : RDD[DataSet], examplesPerFit:Int) method takes much more memory.
    */
    val timelineBatches = splitTimeSriesToTimelines(ts, sparkExamplesPerFit)

    for (batch <- timelineBatches) {

      val tsVectorized = ts.map { case (timeline) =>

        val features: INDArray = Nd4j.zeros(1, inSize, timeline.length - 1)
        val labels: INDArray = Nd4j.zeros(1, inSize, timeline.length - 1)

        val f = new Array[Int](3)
        val l = new Array[Int](3)

        for (i <- 0 to timeline.length - 2) {
          val tsStepValue = timeline(i)._2
          f(1) = (tsStepValue * scale).toInt //FIXME
          f(2) = i

          l(1) = (timeline(i + 1)._2 * scale).toInt //FIXME
          l(2) = i
          features.putScalar(f, 1.0)
          labels.putScalar(l, 1.0)
        }

        new DataSet(features, labels)
      }

      val net = LSTM_N_NET.fitDataSet(tsVectorized)
    }

    //LSTM_N_NET.getNetwork.setUpdater(null)
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
  private def getTimeSeriesAsListOfTimelines(sc: SparkContext,
                                             pathToTimeSeriesBatchfile: String,
                                             timeLineLength: Int): Seq[Seq[(Long, Double)]] = {

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
    logger.info(s"total number of extracted time steps : ${allData.size}. ${timeLineLength} per a timeline.")

    allData.grouped(timeLineLength).toSeq

  }

  /**
  Splits the TimeSeries RDD to short timelines.
    * @param timeSeries
    * @param timeStepsPerTimeline
    * @return
    */
  private def splitTimeSriesToTimelines(timeSeries: RDD[Seq[(Long, Double)]], timeStepsPerTimeline: Int): Array[RDD[Seq[(Long, Double)]]] = {
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
    * TODO implement
    * @param recentTimeline
    * @param predictionIntervalInMs
    * @return
    */
  def predictNextTimeLineIntervalBasedOnRecentState(recentTimeline: Seq[(Long, Double)],
                                            predictionIntervalInMs: Long): Seq[(Long,Double)] = {
    null
  }
  /**
    * TODO
    * uses recent timeline for making predictions
    * @param recentTimeline
    * @param numberOfTimeStepsToPredictPerTimeline
    * @return
    */
  def predictNextTimeLineBasedOnRecentState(recentTimeline: Seq[(Long, Double)],
                                            numberOfTimeStepsToPredictPerTimeline: Int): Seq[Double] = {
    /*  val miniBatchSize = 20
      val numIterations = 5*/
    val numberOfTimelinesToGenerate = 1
    val generationInitialization = recentTimeline.map { case (k, v) => v }
    val rng = new Random(12345)


    logger.info("--------------------")
    logger.info(s"Make prediction for the timeline: ${generationInitialization.mkString(" -> ")}")
    val predictedTimeline = predictNextTimeline(
      generationInitialization,
      LSTM_N_NET.getNetwork,
      rng,
      featureVectorSize,
      numberOfTimeStepsToPredictPerTimeline,
      numberOfTimelinesToGenerate)

    predictedTimeline
  }

  /**
    * Generate a sample from the network, given an (optional, possibly null) initializationTimeline. Initialization
    * can be used to 'prime' the RNN with a sequence you want to extend/continue.<br>
    * Note that the initalization is used for all samples
    *
    * @param initTimeline  String, may be null. If null, select a random character as initializationTimeline for all samples
    * @param numberOfTimeStepsToPredict Number of tsSteps to sample from network (excluding initializationTimeline)
    * @param net             MultiLayerNetwork with one or more GravesLSTM/RNN layers and a softmax output layer
    */
  private def predictNextTimeline(initTimeline: Seq[Double],
                                  net: MultiLayerNetwork,
                                  rng: Random,
                                  featureVectorSize: Int,
                                  numberOfTimeStepsToPredict: Int,
                                  numSamples: Int): List[Double] = {

    //vectorize initialization Timeline
    val numberOfTimeStepsInTimeline = initTimeline.length
    val vectorizedInitTimeline: INDArray = Nd4j.zeros(numSamples, featureVectorSize, numberOfTimeStepsInTimeline)
    for (i <- 0 to initTimeline.length - 1) {
      val idx: Int = (initTimeline(i) * scaleFactor).toInt
      for (j <- 0 to numSamples - 1) {
        vectorizedInitTimeline.putScalar(Array[Int](j, idx, i), 1.0f)
      }
    }

    // net.rnnClearPreviousState //FIXME

    var output: INDArray = net.rnnTimeStep(vectorizedInitTimeline)
    output = output.tensorAlongDimension(output.size(2) - 1, 1, 0) //Gets the last time step output

    val allSamplesResult = new ListBuffer[List[Double]]

    //sampling
    for (i <- 0 to numberOfTimeStepsToPredict - 1) {

      val nextInput = Nd4j.zeros(numSamples, featureVectorSize)

      val sampleResult = new ListBuffer[Double]
      for (s <- 0 to numSamples - 1) {
        val outputProbDistribution = new Array[Double](featureVectorSize)

        for (j <- 0 to outputProbDistribution.length - 1) {
          outputProbDistribution(j) = output.getDouble(s, j)

        }
        val sampledTsStepIdx: Int = sampleFromDistribution(outputProbDistribution, rng)
        nextInput.putScalar(Array[Int](s, sampledTsStepIdx), 1.0f)
        sampleResult += sampledTsStepIdx.toDouble / scaleFactor //inx = tsStepValue * 10
      }
      allSamplesResult += sampleResult.toList
      output = net.rnnTimeStep(nextInput)
    }

    allSamplesResult.toList.flatMap(identity)
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
}
