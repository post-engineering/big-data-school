package com.griddynamics.deepdetector.lstmnet

import java.io.IOException
import java.util.Random

import com.griddynamics.deepdetector.SparkJob
import com.griddynamics.deepdetector.etl.ExtractTimeSeriesJob
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

/**
  * TODO
  */
class AnomalyDetectionNet extends SparkJob with LazyLogging {

  /**
    * Executes job specific logic
    * @param sc predefined Spark context
    * @param args job arguments
    * @return status of job completion: '1' / '0' - success / failure
    */
  override def execute(sc: SparkContext, args: String*): Int = {

    val miniBatchSize = 20
    val numIterations = 5
    val nSamplesToGenerate = 4
    val nTsStepsToSample = 20
    val generationInitialization = null

    // Above is Used to 'prime' the LSTM with a character sequence to continue/complete.
    // Initialization characters must all be in CharacterIterator.getMinimalCharacterSet() by default
    val rng = new Random(12345)

    val nCores = 6
    val sparkExamplesPerFit: Int = 20 * nCores
    val tsVectorSize = 10 * 10


    val net = setupNetwork(sc, tsVectorSize, tsVectorSize)
    val sparkNetwork: SparkDl4jMultiLayer = new SparkDl4jMultiLayer(sc, net)


    //Length of each sequence (used in truncated BPTT)
    val timeSeriesIntervalLength = 20

    //loadTS
    val list = getListOfTimeSeriesIntervals(sc, timeSeriesIntervalLength)
    val tsIntervals = sc.parallelize(list)
    tsIntervals.persist(StorageLevel.MEMORY_ONLY)

    //Do training, and then generate and print samples from network
    for (i <- 0 to numIterations) {
      /*   Split the tsIntervals and convert to data.
           Note that we could of course convert the full data set at once (and re-use it) however this takes
           much more memory than the approach used here.
           In that case, we could use the SparkDl4jMultiLayer.fitDataSet(data : RDD[DataSet], examplesPerFit:Int) method
           instead of manually splitting like we do here
      */
      /*
          val tsIntervals = splitTsToIntervals(tsIntervals, sparkExamplesPerFit)

          for (interval <- tsIntervals) {
            val data = interval.map { case (i) =>

            }

            net = sparkNetwork.fitDataSet(data)
          }*/

      val data = tsIntervals.map { case (interval) =>

        val features: INDArray = Nd4j.zeros(1, tsVectorSize, interval.length - 1)
        val labels: INDArray = Nd4j.zeros(1, tsVectorSize, interval.length - 1)

        val f = new Array[Int](3)
        val l = new Array[Int](3)

        for (i <- 0 to interval.length - 2) {
          val tsStepValue = interval(i)._2
          f(1) = (tsStepValue * 10).toInt
          f(2) = i

          l(1) = (interval(i + 1)._2 * 10).toInt
          l(2) = i
          features.putScalar(f, 1.0)
          labels.putScalar(l, 1.0)
        }

        new DataSet(features, labels)
      }

      sparkNetwork.fitDataSet(data, sparkExamplesPerFit)

      logger.info("--------------------")
      logger.info(s"Completed iteration #${i}")
      //   System.out.println("Sampling characters from network given initialization \"" + (if (generationInitialization == null)  Seq[Double](0.0) else generationInitialization) + "\"")
      val samples = sampleTimeStepsFromNetwork(generationInitialization, net, rng, tsVectorSize, nTsStepsToSample, nSamplesToGenerate)
      logger.info(s"predicted samples:\n\t${samples.mkString(" ,")}")

    }
    1
  }

  /**
    * Generate a sample from the network, given an (optional, possibly null) initialization. Initialization
    * can be used to 'prime' the RNN with a sequence you want to extend/continue.<br>
    * Note that the initalization is used for all samples
    *
    * @param initialization  String, may be null. If null, select a random character as initialization for all samples
    * @param tsStepsToSample Number of tsSteps to sample from network (excluding initialization)
    * @param net             MultiLayerNetwork with one or more GravesLSTM/RNN layers and a softmax output layer
    */
  private def sampleTimeStepsFromNetwork(initialization: Seq[Double],
                                         net: MultiLayerNetwork,
                                         rng: Random,
                                         vectorSize: Int,
                                         tsStepsToSample: Int,
                                         numSamples: Int): List[List[Double]] = {
    var init = 0
    if (initialization == null) {
      init = tsStepsToSample
    } else {
      init = initialization.length
    }

    val initializationInput: INDArray = Nd4j.zeros(numSamples, vectorSize, init)

    net.rnnClearPreviousState

    var output: INDArray = net.rnnTimeStep(initializationInput)
    output = output.tensorAlongDimension(output.size(2) - 1, 1, 0) //Gets the last time step output


    val allSamplesResult = new ListBuffer[List[Double]]
    for (i <- 0 to tsStepsToSample) {

      val nextInput = Nd4j.zeros(numSamples, vectorSize)

      val sampleResult = new ListBuffer[Double]
      for (s <- 0 to numSamples - 1) {
        val outputProbDistribution = new Array[Double](vectorSize)

        for (j <- 0 to outputProbDistribution.length - 1) {
          outputProbDistribution(j) = output.getDouble(s, j)

        }
        val sampledTsStepIdx: Int = sampleFromDistribution(outputProbDistribution, rng)
        nextInput.putScalar(Array[Int](s, sampledTsStepIdx), 1.0f)
        //TODO  sb(s).append(dictionaryIntToTsStep.get(sampledTsStepIdx))
        sampleResult += sampledTsStepIdx / 10 //inx = tsStepValue * 10
      }
      allSamplesResult += sampleResult.toList
      output = net.rnnTimeStep(nextInput)
    }

    allSamplesResult.toList
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

  /**
    * TODO
    * @param sc
    * @return
    */
  def setupNetwork(sc: SparkContext,
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
    net.init
    net.setUpdater(null)

    net
  }

  /**
    * TODO
    * @param sc
    * @param sequenceLength
    * @throws java.io.IOException
    * @return
    */
  @throws(classOf[IOException])
  private def getListOfTimeSeriesIntervals(sc: SparkContext, sequenceLength: Int): Seq[Seq[(Long, Double)]] = {
    val fileLocation = "/home/ipertushin/Documents/ts_proc.loadavg.1min.txt"
    val f = File(fileLocation)
    if (!f.exists) {
      throw new IOException("no such file of TS")
    }
    else {
      System.out.println("Using existing text file at " + f.isValid)
    }
    val allData = ExtractTimeSeriesJob.loadTSFromFile(sc, fileLocation)

    allData.grouped(sequenceLength).toSeq
  }

  /**
  Splits the TimeSeries RDD to short intervals.
    * @param in
    * @param examplesPerSplit
    * @return
    */
  private def splitTsToIntervals(in: RDD[Seq[(Long, Double)]], examplesPerSplit: Int): Array[RDD[Seq[(Long, Double)]]] = {
    var nSplits: Int = 0
    val nExamples: Long = in.count
    if (nExamples % examplesPerSplit == 0) {
      nSplits = (nExamples / examplesPerSplit).toInt
    }
    else {
      nSplits = (nExamples / examplesPerSplit).toInt + 1
    }
    val splitWeights = Array[Double](nSplits)

    for (i <- 0 to nSplits) {
      splitWeights(i) = 1.0 / nSplits
    }

    return in.randomSplit(splitWeights)
  }
}
