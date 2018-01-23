package cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.framework

import java.util.Properties

import cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.VertexSimilarityConfiguration._
import cn.edu.nju.pasalab.graph.util.ScalableSingleNodeGraphAdjStorage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray

/**
  * Created by wangzhaokang on 6/28/17.
  */
object SparkMRFramework {

  /**
    *
    * @param inputGraph the vertex and its adjacency list. Vertices in the adjacency list is sorted by vertex id.
    * @param computeFunctionClassName
    * @param context
    * @param outputDirPath
    * @param conf
    */
  def analysis(inputGraph:RDD[(Long, Array[Long])], computeFunctionClassName: String,
               context:Object,
               outputDirPath:String = null,
               conf:Properties = new Properties()): Unit = {


    ////////// CONF & Execution related accumulators
    val sc = inputGraph.context
    val vertexRDD = inputGraph
    val taskPartitioner = Partitioner.createPartitioner(conf)
    val partitionNum = conf.getProperty(CONF_PARTITIONER_PARTITION_NUMBER,"8").toInt
    val considerOrder = conf.getProperty(CONF_JOB_CONSIDER_VERTEX_ORDER, "true").toBoolean
    val partitionVertexNumAccu = sc.longAccumulator("Partition Vertex Num")
    val partitionEdgeNumAccu = sc.longAccumulator("Partition Edge Num")

    ////////// Output
    if (outputDirPath != null && outputDirPath != "null") {
      val outputPath = new Path(outputDirPath)
      FileSystem.get(new org.apache.hadoop.conf.Configuration()).delete(outputPath, true)
      FileSystem.get(new org.apache.hadoop.conf.Configuration()).mkdirs(outputPath)
    }

    ////////// Execution
    val t0 = System.currentTimeMillis()
    val messageRDD = vertexRDD.flatMap {
      case (vid, neighborhood) => {
        val resultBuffer = new ArrayBuffer[(Int, (Boolean, Long, Array[Long]))]()
        // A. adj itself
        resultBuffer.append((taskPartitioner.getPartition(vid), (true, vid, neighborhood)))
        // B. send to others
        if (considerOrder) {
          // calculate where to start send
          val dstPartIDToStartPos = new mutable.HashMap[Int, Int]()
          var j = 0
          while (j < neighborhood.length && dstPartIDToStartPos.size < taskPartitioner.getNumPartitions()) {
            val dstPartID = taskPartitioner.getPartition(neighborhood(j))
            if (!dstPartIDToStartPos.contains(dstPartID)) {
              dstPartIDToStartPos.put(dstPartID, j)
            }
            j = j + 1
          }
          for ((dstPartID, startPos) <- dstPartIDToStartPos) {
            val subNeighborhoodArray = java.util.Arrays.copyOfRange(neighborhood, startPos, neighborhood.length)
            resultBuffer.append((dstPartID, (false, vid, subNeighborhoodArray)))
          }
        } else {
          // send the whole adj list to related partitions
          val destinationPartitions = neighborhood.map(taskPartitioner.getPartition(_)).distinct
          for (dstPartitionID <- destinationPartitions) {
            resultBuffer.append((dstPartitionID, (false, vid, neighborhood)))
          }
        }
        resultBuffer.iterator
      }
    }

    val partitionRDD = messageRDD.groupByKey(partitionNum)
    partitionRDD.foreach {
      case (partitionID, messageIter) => {
        System.err.println(s"Start processing partition ${partitionID}...")
        System.err.println(s"Start storing message to scalable graph format...")
        val (graphStore, parVertexRDDIter) = prepareRelatedDataStructure(messageIter, conf)

        // store messages to a compact graph format
        var partitionVertexNum: Long = graphStore.numVertices
        var partitionEdgeNum: Long = graphStore.numEdges
        partitionVertexNumAccu.add(partitionVertexNum)
        partitionEdgeNumAccu.add(partitionEdgeNum)
        System.err.println(s"Finish storing messages. Vid Num = ${partitionVertexNum}, Edge Num = $partitionEdgeNum.")
        System.err.println(s"Start computing mutual friends for each vertex...")
        // counting mutual friend by sequential algorithm
        val outputStreamForThisPartition = if (outputDirPath != null && outputDirPath != "null") {
          FileSystem.get(new Configuration()).create(new Path(outputDirPath, s"part-${partitionID}"))
        } else null

        val computeFunctionClass = Class.forName(computeFunctionClassName).asSubclass(classOf[NeighborhoodComputeFunction])
        val computeFunction:NeighborhoodComputeFunction = computeFunctionClass.newInstance()

        computeFunction.setup(outputStreamForThisPartition, context, conf)
        parVertexRDDIter.foreach {
          case (vid, adj) => {
            computeFunction.compute(vid, adj, graphStore)
            }
          } // end of case
        computeFunction.cleanup()

        if (outputStreamForThisPartition != null) outputStreamForThisPartition.close()
        graphStore.close()
        System.err.println(s"Finish analysis in partition ${partitionID}.")

      }
    } // end of foreach of PartitionRDD

    System.out.println(
      s"""
         |==== Analysis Done ====
         |1. Partition Info:
         |    a. Partition Vertex Num: Sum ${partitionVertexNumAccu.value}, Avg ${partitionVertexNumAccu.avg}
         |    b. Partition Edge Num: Sum ${partitionEdgeNumAccu.value}, Avg ${partitionEdgeNumAccu.avg}
         |==== END ====
       """.stripMargin)

  }
  /*
    Implementation 2: Cogroup
    val partedVertexRDD = vertexRDD.map {
      case (vid, neighborhood) => (taskPartitioner.getPartition(vid), (vid, neighborhood))
    }.partitionBy(taskPartitioner)
    val messageRDD = vertexRDD.flatMap {
      case (vid, neighborhood) => {
        // send to who
        val dstPartIDToStartPos = new com.carrotsearch.hppc.IntIntHashMap()
        var j = 0
        while (j < neighborhood.length && dstPartIDToStartPos.size() < taskPartitioner.numPartitions) {
          val dstPartID = taskPartitioner.getPartition(neighborhood(j))
          if (!dstPartIDToStartPos.containsKey(dstPartID)) {
            dstPartIDToStartPos.put(dstPartID, j)
          }
          j = j + 1
        }
        for (dstPartID <- dstPartIDToStartPos.keys) yield {
          val startPos = dstPartIDToStartPos.get(dstPartID)
          val subNeighborhoodArray = java.util.Arrays.copyOfRange(neighborhood, startPos, neighborhood.length)
          (dstPartID, (vid, subNeighborhoodArray))
        }
      }
    }.partitionBy(taskPartitioner)

    val coRDD = partedVertexRDD.cogroup(messageRDD)


    coRDD.foreach {
      case (partitionID, (vertexRDDIter, messageRDDIter)) => {
        val logClient = wzk.akkalogger.client.ProcessLevelClient.client
        System.err.println(s"Start processing partition ${partitionID}...")
        // store messages to a compact graph format
        System.err.println(s"Start storing message to compact graph format...")
        var partitionVertexNum = 0L
        var partitionEdgeNum = 0L
        var taskVertexNum = 0L
        var resultPairNum = 0L
        val graphStore = new CompactGraphAdjStorage()
        for ((vid, adj) <- messageRDDIter) {
          graphStore.addAdjList(vid, adj)
          partitionVertexNum = partitionVertexNum + 1
          partitionEdgeNum = partitionEdgeNum + adj.length
        }
        partitionVertexNumAccu.add(partitionVertexNum)
        partitionEdgeNumAccu.add(partitionEdgeNum)
        System.err.println(s"Finish storing messages. Vid Num = ${partitionVertexNum}, Edge Num = $partitionEdgeNum.")
        System.err.println(s"Start computing mutual friends for each vertex...")
        // counting mutual friend by sequential algorithm
        val outputStreamWriter = if (outputPath != null) {
          new PrintWriter(FileSystem.get(
            new Configuration())
            .create(new Path(outputPath, s"part-${partitionID}")))
        } else null

        val parVertexRDDIter = vertexRDDIter.toParArray
        parVertexRDDIter.foreach {
          case (vid, adj) => {
            val countMap = new com.carrotsearch.hppc.LongIntHashMap(adj.length)
            for (nvid <- adj) {
              val (startPos, endPos) = graphStore.queryAdjList(nvid)
              if (startPos >= 0) {
                var i = startPos
                while (i < endPos) {
                  val nnvid = graphStore.arrayBuffer.get(i)
                  if (nnvid > vid)
                    countMap.addTo(nnvid, 1)
                  i = i + 1
                } // end of while
              } // end of if
            } // end of for nvid
            if (outputStreamWriter != null) {
              synchronized {
                for (nnvid <- countMap.keys) {
                  outputStreamWriter.println(s"$vid $nnvid ${countMap.get(nnvid)}")
                }
              }
            } // end of output
            synchronized {
              resultPairNum = resultPairNum + countMap.size()
              taskVertexNum = taskVertexNum + 1
              resultPairNumAccu.add(countMap.size())
              taskVertexNumAccu.add(1)
            }
          } // end of case
        }// end of vertexRDD Iter for

        val metrics = Map("Partition Vertex Num" -> partitionEdgeNum,
                          "Partition Edge Num" -> partitionEdgeNum,
          "Task Vertex Num" -> taskVertexNum,
          "Result Pair Num" -> resultPairNum)
        logClient.logMetrics(metrics)

        if (outputStreamWriter != null) outputStreamWriter.close()
        System.err.println(s"Finish mutual friend count calculation in partition ${partitionID}.")
      } // end of case
    } // end of for-each
    */

  private def prepareRelatedDataStructure(messageIter: Iterable[(Boolean, Long, Array[Long])], conf:Properties)
  : (ScalableSingleNodeGraphAdjStorage, ParArray[(Long, Array[Long])]) = {
    val graphStorage = ScalableSingleNodeGraphAdjStorage.create(conf)
    val taskGraph = new ArrayBuffer[(Long, Array[Long])]()
    for ((fromOriginGraph, vid, adj) <- messageIter) {
      if (fromOriginGraph) {
        taskGraph.append((vid, adj))
      } else {
        graphStorage.addAdjList(vid, adj)
      }
    }
    (graphStorage, taskGraph.toParArray)
  }

}
