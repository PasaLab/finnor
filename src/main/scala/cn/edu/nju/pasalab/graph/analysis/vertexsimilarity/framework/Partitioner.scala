package cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.framework

import java.util.Properties

import cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.VertexSimilarityConfiguration._
import cn.edu.nju.pasalab.graph.util.MyHDSFUtils
import gnu.trove.map.hash.TLongIntHashMap
import org.apache.hadoop.fs.Path

import scala.io.Source


abstract class Partitioner() extends Serializable {

  protected var numPartitions:Int = 1

  /**
    * Setup num partitions and other configurations
    * @param conf
    */
  def setup(conf:Object):Unit = {}

  def setup(conf:Properties):Unit = {
    numPartitions = conf.getProperty(CONF_PARTITIONER_PARTITION_NUMBER, "1").toInt
  }

  def getPartition(key: Long):Int

  def getNumPartitions():Int = {numPartitions}
}

object Partitioner {

  def createPartitioner(conf:Properties):Partitioner = {
    val partitionerClassName = conf.getProperty(CONF_PARTITIONER_CLASS_NAME, classOf[ModPartitioner].getCanonicalName)
    val partitionerInstance:Partitioner = Class.forName(partitionerClassName).newInstance().asInstanceOf[Partitioner]
    partitionerInstance.setup(conf)
    partitionerInstance
  }

}

/**
  * Created by wangzhaokang on 6/28/17.
  */
class ModPartitioner extends Partitioner {
  override def getPartition(key: Long): Int = {
    (key % numPartitions).toInt
  }
}


/**
  * Read partition strategy from a partitioner file
  */
class PartitionFilePartitioner extends Partitioner {
  var partitionMap:TLongIntHashMap = null

  override def setup(conf: Properties): Unit = {
    super.setup(conf)
    val graphFilePath = conf.getProperty(CONF_JOB_INPUT_FILE_PATH)
    val parentDirPath = new Path(graphFilePath).getParent
    val partitionStrategyFilePath = new Path(parentDirPath, "partitionPlay")
    partitionMap = readPartitionStrategy(partitionStrategyFilePath.toString)
    println("Init partition strategy finishes!")
  }
  override def getPartition(key: Long): Int = {
    if (!partitionMap.containsKey(key)) throw new Exception(s"Unknown vertex id ${key} for partitioner")
    partitionMap.get(key)
  }

  private def readPartitionStrategy(dirPath:String):TLongIntHashMap = {
    val partitionMap = new TLongIntHashMap()
    var maxiumPartNum = -1
    MyHDSFUtils.listFiles(dirPath).foreach(file => {
      val inputStream = MyHDSFUtils.getFileSystem().open(file)
      val source = Source.fromInputStream(inputStream)
      for (line <- source.getLines()) {
        val vid = line.split("-")(0).toLong
        val partID = line.split("-")(1).toInt
        if (partID > maxiumPartNum) maxiumPartNum = partID
        partitionMap.put(vid, partID)
      }
      source.close()
      inputStream.close()
    })
    numPartitions = maxiumPartNum + 1
    partitionMap
  }
}
