package cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.framework

import java.io.OutputStream
import java.{lang, util}
import java.util.Properties

import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.rdd.RDD
import cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.VertexSimilarityConfiguration._
import cn.edu.nju.pasalab.graph.util.{MyHDSFUtils, ScalableSingleNodeGraphAdjStorage}
import cn.edu.nju.pasalab.graph.util.cache.{CaffeineProcessLevelCache, KVCacheSystem, NullCache}
import cn.edu.nju.pasalab.graph.util.db.batchaccess.BatchAdjListAccess
import cn.edu.nju.pasalab.graph.util.db.redis.{FeedDataIntoRedis, RedisBatchConnector}
import gnu.trove.set.hash.TLongHashSet
import org.apache.hadoop.fs.Path
import wzk.akkalogger.client.AkkaLoggerClient

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * Created by wangzhaokang on 8/9/17.
  */
object InMemoryFramework {
  /**
    *
    * @param inputGraph the vertex and its adjacency list. Vertices in the adjacency list is sorted by vertex id.
    * @param computeFunctionClassName
    * @param context
    * @param outputDirPath
    * @param conf
    */
  def analysis(inputGraph: RDD[(Long, Array[Long])], computeFunctionClassName: String,
               context: Object,
               outputDirPath: String = null,
               conf: Properties = new Properties()): Unit = {
    ///////// Configuration
    val batchDBQuerySize = conf.getProperty(CONF_JOB_DB_BATCH_QUERY_SIZE, "1000").toInt
    val t0 = System.currentTimeMillis()
    //////////// Prepare
    if (outputDirPath != null && outputDirPath != "null") {
      MyHDSFUtils.deleteDir(outputDirPath)
      MyHDSFUtils.getFileSystem().mkdirs(new Path(outputDirPath))
    }
    val coarsedInputGraph = inputGraph//inputGraph.coalesce(conf.getProperty(CONF_PARTITIONER_PARTITION_NUMBER, "8").toInt, false)
    //////////// Store graph into redis
    System.out.println("Start to load graph into redis... " + new java.util.Date())
    val javaGraphRDD = coarsedInputGraph.toJavaRDD().mapToPair(new PairFunction[(Long, Array[Long]), java.lang.Long, Array[Long]] {
      override def call(t: (Long, Array[Long])): (lang.Long, Array[Long]) = {
        (new lang.Long(t._1), t._2)
      }
    })
    FeedDataIntoRedis.feedGraphToRedis(javaGraphRDD, batchDBQuerySize)
    val t1 = System.currentTimeMillis()
    System.out.println(s"Done(${t1 - t0} ms)... " + new java.util.Date())

    //////////// Execute analysis
    val executeRDD = coarsedInputGraph.mapPartitionsWithIndex {
      case (partitionID, partitionIterator) => {
        val t0 = System.currentTimeMillis()
        var taskCount = 0L
        System.err.println(s"Start processing partition ${partitionID}...")
        // Prepare partition execution environment
        val tasks = new ArrayBuffer[(Long, Array[Long])]()
        var outputFileStream: OutputStream = null
        if (outputDirPath != "null") {
          outputFileStream = MyHDSFUtils.getFileSystem().create(new Path(outputDirPath + s"/part-${partitionID}"))
        }
        val computeFunction = Class.forName(computeFunctionClassName).newInstance().asInstanceOf[NeighborhoodComputeFunction]
        computeFunction.setup(outputFileStream, context, conf)
        // Execute in batch
        while (partitionIterator.hasNext) {
          tasks.append(partitionIterator.next())
          if (tasks.length >= batchDBQuerySize) {
            runBatchVertexAnalysis(tasks, computeFunction, context, conf, outputFileStream)
            tasks.clear()
          }
          taskCount += 1L
        }
        if (tasks.length > 0) {
          runBatchVertexAnalysis(tasks, computeFunction, context, conf, outputFileStream)
        }
        // Clean up
        computeFunction.cleanup()
        if (outputFileStream != null) outputFileStream.close()
        System.err.println("Done!")
        val t1 = System.currentTimeMillis()
        Iterator((taskCount, t1 - t0))
      }
    }
    val (taskCount, totalWallClockCPUTime) = executeRDD.reduce((a,b) => (a._1 + b._1, a._2+b._2))
    val t2 = System.currentTimeMillis()
    val configurationDetail = (for (key <- conf.keysIterator) yield s" ${key}=${conf.getProperty(key)}").mkString("\n")
    val frameworkExecutionReport =
      s"""
        |Framework analysis done!
        |- Configuration ===>
        |${configurationDetail}
        |- Task statics ===>
        | a. Task count: ${taskCount} ;
        | b. Total wall-clock time: ${totalWallClockCPUTime} ms ;
        | c. Average wall-clock time per task: ${totalWallClockCPUTime / taskCount.toDouble} ms;
        |- Framework wall-clock performance metrics ===>
        | a. Store graph to redis time: ${t1 - t0} ms;
        | b. Execute analysis time: ${t2 - t1} ms;
        | c. Framework total execution time: ${t2 - t0} ms;
      """.stripMargin
    System.out.println(frameworkExecutionReport)
  }


  private def getBatchAdjAccessClient(): BatchAdjListAccess = {
    val processLevelCaffeineCache = CaffeineProcessLevelCache.getInstance()
      .asInstanceOf[KVCacheSystem[lang.Long, Array[Long]]]
    val capacity = processLevelCaffeineCache.getCapacity()
    if (capacity == 0) {
      System.err.println("[Info] We choose null cache")
      new BatchAdjListAccess(new RedisBatchConnector(), new NullCache[lang.Long, Array[Long]]())
    } else {
      new BatchAdjListAccess(new RedisBatchConnector(), processLevelCaffeineCache)
    }
  }


  private def runBatchVertexAnalysis(tasks: Seq[(Long, Array[Long])],
                                     computeFunction: NeighborhoodComputeFunction, context: Object, conf: Properties,
                                     out: OutputStream) {
    val t0 = System.currentTimeMillis()
    // execute query
    val dbClient = getBatchAdjAccessClient()
    var totalRequestNum = 0L
    val requestVidSet = new TLongHashSet()
    for ((vid, adj) <- tasks)
      for (nvid <- adj) {
        requestVidSet.add(nvid)
        dbClient.addRequest(nvid)
        totalRequestNum += 1
      }
    val batchCompressionRatio = requestVidSet.size().toDouble / totalRequestNum
    dbClient.exec()
    // conduct neighborhood computation
    val t1 = System.currentTimeMillis()
    for ((vid, adj) <- tasks) {
      val localGraphScope = ScalableSingleNodeGraphAdjStorage.create(conf)
      for (nvid <- adj) {
        localGraphScope.addAdjList(nvid, dbClient.getAdjList(nvid))
      }
      computeFunction.compute(vid, adj, localGraphScope)
    }
    dbClient.cleanup()
    val t2 = System.currentTimeMillis()

    val metrics = Map(("Framework-DB query time (ms)", t1 - t0), ("Framework-Compute time (ms)", t2 - t1),
                      ("Framework-Total time (ms)", t2 - t0), ("Framework-Batch compression ratio", (batchCompressionRatio * 100).toLong))
    val logClient = AkkaLoggerClient.getProcessLevelClient
    logClient.logMetrics(metrics)
  }


}
