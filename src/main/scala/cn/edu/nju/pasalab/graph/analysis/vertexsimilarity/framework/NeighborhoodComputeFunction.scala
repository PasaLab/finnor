package cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.framework

import java.io.OutputStream
import java.util.Properties

import cn.edu.nju.pasalab.graph.util.ScalableSingleNodeGraphAdjStorage

/**
  * Created by wangzhaokang on 6/28/17.
  */
abstract class NeighborhoodComputeFunction extends Serializable {
  def setup(out:OutputStream, context: Object, conf:Properties): Unit = {
    setup(out, context)
  }
  def setup(out:OutputStream, context:Object):Unit = {}
  def compute(vid:Long, adj:Array[Long], graphStore:ScalableSingleNodeGraphAdjStorage):Unit
  def cleanup():Unit = {}
}
