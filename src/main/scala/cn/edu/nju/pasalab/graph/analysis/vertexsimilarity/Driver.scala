package cn.edu.nju.pasalab.graph.analysis.vertexsimilarity

import cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.framework.{DegreeInfoPreparation, NeighborhoodComputeFunction}
import com.carrotsearch.hppc.LongIntHashMap
import gnu.trove.map.hash.TLongIntHashMap
import org.apache.spark.{SparkConf, SparkContext}



/**
  * Created by wangzhaokang on 8/8/17.
  */
object SparkMRDriver {
  def parseArgs(args:Seq[String]):java.util.Properties = {
    val properties = new java.util.Properties()
    args.foreach(set => {
      val key = set.split("=")(0)
      val value = set.split("=")(1)
      properties.setProperty(key,value)
    })
    properties
  }
  def main(args: Array[String]): Unit = {
    /////////////////// Prepare ////////////////////////////////////
    val sparkConf = new SparkConf().setAppName("Pairwise Vertex Similarity Comparisons - SparkMR")
    val sc = new SparkContext(sparkConf)
    val analysisConf = parseArgs(args)
    System.out.println("==> Job configurations:\n" + analysisConf)

    ///////////////// Accumulators & Log client /////////////////////////////
    val logClient = wzk.akkalogger.client.ProcessLevelClient.client
    logClient.clearPreviousMetrics("New spark program begin...")
    val resultNumAccu = sc.longAccumulator("Result Num")
    println(s"Before run: result num = ${resultNumAccu.value}")


    ///////////////// Input //////////////////////////
    val t0 = System.currentTimeMillis()
    val inputFilePath = analysisConf.getProperty(VertexSimilarityConfiguration.CONF_JOB_INPUT_FILE_PATH)
    val outputFilePath = analysisConf.getProperty(VertexSimilarityConfiguration.CONF_JOB_OUTPUT_FILE_PATH)
    val inputFileRDD = sc.textFile(inputFilePath)
    val vertexRDD = inputFileRDD.map(line => {
      val fields = line.split(" ")
      val vid = fields(0).toLong
      if (fields.length < 2) throw new Exception(s"too small line:${line}")
      val neighborhood = (for (i <- 1 until fields.length) yield fields(i).toLong).toArray.sorted
      (vid, neighborhood)
    })

    val similarityFunctionName = analysisConf.getProperty(VertexSimilarityConfiguration.CONF_JOB_SIMILARITY_FUNCTION_NAME,"CN")
    val ignoreDegreeTable = analysisConf.getProperty(VertexSimilarityConfiguration.CONF_IGNORE_DEGREE_TABLE, "false").toBoolean
    var degreeInfoMap = new LongIntHashMap()
    if (!ignoreDegreeTable) {
      degreeInfoMap = DegreeInfoPreparation.prepareDegreeInfo(vertexRDD)
    }
    val degreeInfo = sc.broadcast(degreeInfoMap)
    val t1 = System.currentTimeMillis()

    val similarityFunction = analysisConf.getProperty(VertexSimilarityConfiguration.CONF_JOB_SIMLARIFY_FUNCTION_CLASS)
    framework.SparkMRFramework.analysis(vertexRDD, similarityFunction, degreeInfo,
      outputFilePath, analysisConf)

    val t2 = System.currentTimeMillis()

    println(
      s"""
         |--> Task related accumulators:
         |    a. Result Pair Num: ${resultNumAccu.value}
         |--> Performance metrics:
         |    a. Wall-clock total execution time:${t2 - t0} ms
         |    a. Wall-clock degree preparation time:${t1 - t0} ms
         |    a. Wall-clock framework time:${t2 - t1} ms
       """.stripMargin)

    sc.stop()

    System.exit(0)
  }

}


object InMemoryDriver {
  def parseArgs(args:Seq[String]):java.util.Properties = {
    val properties = new java.util.Properties()
    args.foreach(set => {
      val key = set.split("=")(0)
      val value = set.split("=")(1)
      properties.setProperty(key,value)
    })
    properties
  }
  def main(args: Array[String]): Unit = {
    /////////////////// Prepare ////////////////////////////////////
    val sparkConf = new SparkConf().setAppName("Pairwise Vertex Similarity Comparisons - In Memory")
    val sc = new SparkContext(sparkConf)
    val analysisConf = parseArgs(args)
    System.out.println("==> Job configurations:\n" + analysisConf)

    ///////////////// Accumulators & Log client /////////////////////////////
    val logClient = wzk.akkalogger.client.ProcessLevelClient.client
    logClient.clearPreviousMetrics("New spark program begin...")
    val resultNumAccu = sc.longAccumulator("Result Num")
    println(s"Before run: result num = ${resultNumAccu.value}")


    ///////////////// Input //////////////////////////
    val t0 = System.currentTimeMillis()
    val inputFilePath = analysisConf.getProperty(VertexSimilarityConfiguration.CONF_JOB_INPUT_FILE_PATH)
    val outputFilePath = analysisConf.getProperty(VertexSimilarityConfiguration.CONF_JOB_OUTPUT_FILE_PATH)
    val inputFileRDD = sc.textFile(inputFilePath)
    val vertexRDD = inputFileRDD.map(line => {
      val fields = line.split(" ")
      val vid = fields(0).toLong
      if (fields.length < 2) throw new Exception(s"too small line:${line}")
      val neighborhood = (for (i <- 1 until fields.length) yield fields(i).toLong).toArray.sorted
      (vid, neighborhood)
    })

    val similarityFunctionName = analysisConf.getProperty(VertexSimilarityConfiguration.CONF_JOB_SIMILARITY_FUNCTION_NAME,"CN")
    val ignoreDegreeTable = analysisConf.getProperty(VertexSimilarityConfiguration.CONF_IGNORE_DEGREE_TABLE, "false").toBoolean
    var degreeInfoMap = new LongIntHashMap()
    if (!ignoreDegreeTable) {
      degreeInfoMap = DegreeInfoPreparation.prepareDegreeInfo(vertexRDD)
    }
    val degreeInfo = sc.broadcast(degreeInfoMap)
    val t1 = System.currentTimeMillis()

    val similarityFunction = analysisConf.getProperty(VertexSimilarityConfiguration.CONF_JOB_SIMLARIFY_FUNCTION_CLASS)
    framework.InMemoryFramework.analysis(vertexRDD, similarityFunction, degreeInfo,
      outputFilePath, analysisConf)

    val t2 = System.currentTimeMillis()

    println(
      s"""
         |--> Task related accumulators:
         |    a. Result Pair Num: ${resultNumAccu.value}
         |--> Performance metrics:
         |    a. Wall-clock total execution time:${t2 - t0} ms
         |    a. Wall-clock degree preparation time:${t1 - t0} ms
         |    a. Wall-clock framework time:${t2 - t1} ms
       """.stripMargin)

    logClient.writeMetricToFile("inmemory.log")
    Thread.sleep(1000,0)
    sc.stop()

    System.exit(0)
  }

}
