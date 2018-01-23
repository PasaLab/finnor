package cn.edu.nju.pasalab.graph.analysis.vertexsimilarity

import java.io.{OutputStream, PrintWriter}
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.framework.NeighborhoodComputeFunction
import VertexSimilarityConfiguration._
import cn.edu.nju.pasalab.graph.util.ScalableSingleNodeGraphAdjStorage
import com.carrotsearch.hppc.LongIntHashMap
import gnu.trove.map.hash.TLongIntHashMap
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable

class JaccardLikeIndexComputingFunction extends NeighborhoodComputeFunction {
  var outputWriter: PrintWriter = null
  private val log = wzk.akkalogger.client.ProcessLevelClient.client
  private var resultPairNum = new AtomicLong(0L)
  private var degrees:LongIntHashMap = null
  private var K:Int = 5
  private var similarityFunctionName:String = "CN"

  override def setup(out: OutputStream, context: Object, conf: Properties): Unit = {
    if (out != null)
      outputWriter = new PrintWriter(out)
    degrees = context.asInstanceOf[Broadcast[LongIntHashMap]].value
    K = conf.getProperty(CONF_JOB_TOP_K, "5").toInt
    similarityFunctionName = conf.getProperty(CONF_JOB_SIMILARITY_FUNCTION_NAME,"CN")
  }

  def getSimilarityComputeFunction():((Int,Long,Long) => Double) = {
    similarityFunctionName match {
      case "CN" => (x:Int,_,_) => x
      case "jaccard" => (overlap,u,v) => overlap.toDouble/(degrees.get(u) + degrees.get(v) - overlap)
      case "salton" => (overlap,u,v) => overlap.toDouble/(Math.sqrt(degrees.get(u).toDouble * degrees.get(v)))
    }
  }

  override def compute(vid: Long, adj: Array[Long], graphStore: ScalableSingleNodeGraphAdjStorage): Unit = {
    assert(adj.length >= 1)
    val countMap = new mutable.HashMap[Long, Int]()
    for (nvid <- adj) {
      val nadj = graphStore.queryAdjList(nvid)
      assert(nadj.length >= 1)
      for (nnvid <- nadj if nnvid > vid) {
        countMap(nnvid) = countMap.getOrElse(nnvid, 0) + 1
      }
    } // end of for nvid
    val similarityFunction = getSimilarityComputeFunction()
    val similaritiesScores = for ((nnvid, overlap) <- countMap) yield similarityFunction(overlap,nnvid,vid)

    /**
    val topKSimilarityScores = similaritiesScores.toSeq.sorted.reverse.slice(0,Math.min(K, similaritiesScores.size))
    if (topKSimilarityScores.last < 1e-6) {
      throw new Exception(s"${similarityFunctionName} == 0.0 for ${vid}, co: ${(for (nnvid <- countMap.keys) yield countMap.get(nnvid)).mkString(",")}")
    }
      */
    if (outputWriter != null) {
      synchronized {
          //outputWriter.println(s"$vid ${topKSimilarityScores.last}")
        val scoreIterator = similaritiesScores.iterator
        for ((uvid,overlap) <- countMap) {
          if (vid < uvid)
            outputWriter.println(s"$vid,$uvid ${scoreIterator.next()}")
          else
            outputWriter.println(s"$uvid,$vid ${scoreIterator.next()}")
        }
      }
    } // end of output
    resultPairNum.addAndGet(similaritiesScores.toSeq.size)
  }

  override def cleanup(): Unit = {
    if (outputWriter != null) outputWriter.close()
    val metrics = Map(
      "Result Pair Num" -> resultPairNum.get())
    log.logMetrics(metrics)
  }
}

class RALikeIndexComputingFunction extends NeighborhoodComputeFunction {
  var outputWriter: PrintWriter = null
  private val log = wzk.akkalogger.client.ProcessLevelClient.client
  private var resultPairNum = new AtomicLong(0L)
  private var degrees:LongIntHashMap = null
  private var K:Int = 5
  private var similarityFunctionName:String = "RA"
  private var isRA:Boolean = false

  override def setup(out: OutputStream, context: Object, conf: Properties): Unit = {
    if (out != null)
      outputWriter = new PrintWriter(out)
    degrees = context.asInstanceOf[Broadcast[LongIntHashMap]].value
    K = conf.getProperty(CONF_JOB_TOP_K, "5").toInt
    similarityFunctionName = conf.getProperty(CONF_JOB_SIMILARITY_FUNCTION_NAME,"RA")
    if (similarityFunctionName == "RA") isRA = true
  }

  override def compute(vid: Long, adj: Array[Long], graphStore: ScalableSingleNodeGraphAdjStorage): Unit = {
    assert(adj.length >= 1)
    val countMap = new mutable.HashMap[Long, Float]()
    for (nvid <- adj) {
      val nadj = graphStore.queryAdjList(nvid)
      assert(nadj.length >= 1)
      for (nnvid <- nadj if nnvid > vid) {
        val overlapUnit = if (isRA) 1.0/degrees.get(nvid)
                          else if (degrees.get(nvid) != 1) 1.0/math.log(degrees.get(nvid)).toFloat
                          else 0.0
        countMap(nnvid) = countMap.getOrElse(nnvid, 0.0f) + overlapUnit.toFloat
      }
    } // end of for nvid

    /**
    val topKSimilarityScores = similaritiesScores.toSeq.sorted.reverse.slice(0,Math.min(K, similaritiesScores.size))
    if (topKSimilarityScores.last < 1e-6) {
      throw new Exception(s"${similarityFunctionName} == 0.0 for ${vid}, co: ${(for (nnvid <- countMap.keys) yield countMap.get(nnvid)).mkString(",")}")
    }
      */
    if (outputWriter != null) {
      synchronized {
          //outputWriter.println(s"$vid ${topKSimilarityScores.last}")
        for (uvid <- countMap.keySet) {
          if (vid < uvid)
            outputWriter.println(s"$vid,$uvid ${countMap(uvid)}")
          else
            outputWriter.println(s"$uvid,$vid ${countMap(uvid)}")
        }
      }
    } // end of output
    resultPairNum.addAndGet(countMap.size)
  }

  override def cleanup(): Unit = {
    if (outputWriter != null) outputWriter.close()
    val metrics = Map(
      "Result Pair Num" -> resultPairNum.get())
    log.logMetrics(metrics)
  }
}
