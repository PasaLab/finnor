package cn.edu.nju.pasalab.graph.analysis.vertexsimilarity

import java.io.{OutputStream, PrintWriter}
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.VertexSimilarityConfiguration._
import cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.framework.NeighborhoodComputeFunction
import cn.edu.nju.pasalab.graph.util.ScalableSingleNodeGraphAdjStorage
import gnu.trove.map.hash.TLongIntHashMap
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable


