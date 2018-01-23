package cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.framework

import java.io.{DataInput, DataOutput}
import java.lang
import java.util.Properties

import cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.VertexSimilarityConfiguration._
import cn.edu.nju.pasalab.graph.util.ScalableSingleNodeGraphAdjStorage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, NullOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray


/**
  * Created by wangzhaokang on 6/29/17.
  */
object HadoopNoNAnalysisFramework {

  val INPUT_NULL_FILE = "null"
  val CONF_OUTPUT_DIR_PATH = "pasa.output.dir"
  val CONF_COMPUTE_FUNCTION_CLASSNAME = "pasa.compute.function.class"
  val CONF_PARTITION_NUM = "pasa.partition.num"
  val CONF_NODE_NUM = "pasa.node.num"
  val CONF_IN_MEMORY_THRESHOLD = ScalableSingleNodeGraphAdjStorage.CONF_IN_MEMORY_GRAPH_STORE_THRESHOLD_MB
  val CONF_CONSIDER_ORDER = CONF_JOB_CONSIDER_VERTEX_ORDER

  def analysis(inputTextGraphFilePath: String, computeFunctionClass: Class[_ <: NeighborhoodComputeFunction],
               mrConf: Configuration,
               conf: Properties = new Properties()): Unit = {
    // Setup user defined properties
    for (keyObject <- conf.keySet()) {
      val key = keyObject.toString
      val value = conf.getProperty(key)
      mrConf.set(key, value)
    }
    // Setup framework configurations
    mrConf.set(CONF_OUTPUT_DIR_PATH, conf.getProperty(CONF_OUTPUT_DIR_PATH))
    mrConf.set(CONF_COMPUTE_FUNCTION_CLASSNAME, computeFunctionClass.getName)
    mrConf.setInt(CONF_PARTITION_NUM, conf.getProperty(CONF_PARTITION_NUM, "8").toInt)
    mrConf.setInt(CONF_NODE_NUM, conf.getProperty(CONF_NODE_NUM, "8").toInt)

    // Create hadoop job
    val job = Job.getInstance(mrConf, "NoN Analysis:" + computeFunctionClass.getName)
    job.setJarByClass(HadoopNoNAnalysisFramework.getClass)
    val mapperInstance = new NoNMapper()
    val reducerInstance = new NoNReducer()
    job.setMapperClass(mapperInstance.getClass)
    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[AdjListWritable])
    job.setReducerClass(reducerInstance.getClass)
    job.setNumReduceTasks(mrConf.getInt(CONF_NODE_NUM, 8))
    job.setOutputKeyClass(NullWritable.get().getClass)
    job.setOutputValueClass(NullWritable.get().getClass)
    FileInputFormat.setInputPaths(job, new Path(inputTextGraphFilePath))
    val splitSize = determineSplitSize(mrConf, inputTextGraphFilePath, job.getNumReduceTasks)
    FileInputFormat.setMaxInputSplitSize(job, splitSize + 1)
    FileInputFormat.setMinInputSplitSize(job, splitSize)
    job.setOutputFormatClass(classOf[NullOutputFormat[NullWritable, NullWritable]])
    FileOutputFormat.setOutputPath(job, new Path("null"))
    // Run
    job.waitForCompletion(true)
    println(
      s"""
         |Analysis done!
       """.stripMargin)
  }

  private def determineSplitSize(mrConf: Configuration, inputDir: String, partitionNum: Int): Long = {
    val fs = FileSystem.get(mrConf)
    val path = new Path(inputDir)
    val status = fs.getContentSummary(path)
    val totalFileSize = status.getLength
    status.getLength / partitionNum
  }
}

class AdjListWritable() extends Writable {
  var belongToPartition: Boolean = false
  var vid: Long = -1
  var adj: Array[Long] = null

  def this(belong: Boolean, v: Long, a: Array[Long]) {
    this()
    belongToPartition = belong
    vid = v
    adj = a
  }

  override def readFields(dataInput: DataInput): Unit = {
    belongToPartition = dataInput.readBoolean()
    vid = dataInput.readLong()
    val length = dataInput.readInt()
    adj = new Array[Long](length)
    var i = 0
    while (i < length) {
      adj(i) = dataInput.readLong()
      i += 1
    }
  }

  override def write(dataOutput: DataOutput): Unit = {
    dataOutput.writeBoolean(belongToPartition)
    dataOutput.writeLong(vid)
    dataOutput.writeInt(adj.length)
    var i = 0
    while (i < adj.length) {
      dataOutput.writeLong(adj(i))
      i += 1
    }
  }
}

/**
  * Output: KEY - Partition ID, VALUE - Adj list message
  */
class NoNMapper extends Mapper[LongWritable, Text, IntWritable, AdjListWritable] {

  var taskPartitioner: Partitioner = null
  var considerOrder: Boolean = true

  override def setup(context: Mapper[LongWritable, Text, IntWritable, AdjListWritable]#Context): Unit = {
    val conf = context.getConfiguration()
    val partitionNum = conf.getInt(HadoopNoNAnalysisFramework.CONF_PARTITION_NUM, 32)
    taskPartitioner = new ModPartitioner()
    val properties = new java.util.Properties()
    properties.setProperty(CONF_PARTITIONER_PARTITION_NUMBER, partitionNum.toString)
    taskPartitioner.setup(properties)
    considerOrder = conf.getBoolean(HadoopNoNAnalysisFramework.CONF_CONSIDER_ORDER, true)
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntWritable, AdjListWritable]#Context) {
    val line = value.toString
    val fields = line.split(" ")
    val vid = fields(0).toLong
    if (fields.length < 2) throw new Exception(s"too small line:${line}")
    val neighborhood = (for (i <- 1 until fields.length) yield fields(i).toLong).toArray.sorted
    // a. send to myself
    val myPartitionID = taskPartitioner.getPartition(vid)
    context.write(new IntWritable(myPartitionID), new AdjListWritable(true, vid, neighborhood))
    // b. send to others
    if (considerOrder) {
      val dstPartIDToStartPos = new mutable.HashMap[Int, Int]()
      var j = 0
      while (j < neighborhood.length && dstPartIDToStartPos.size < taskPartitioner.getNumPartitions() - 1) {
        val dstPartID = taskPartitioner.getPartition(neighborhood(j))
        if (!dstPartIDToStartPos.contains(dstPartID) && dstPartID != myPartitionID) {
          dstPartIDToStartPos.put(dstPartID, j)
        }
        j = j + 1
      }
      for ((dstPartID, startPos) <- dstPartIDToStartPos) {
        val subNeighborhoodArray = java.util.Arrays.copyOfRange(neighborhood, startPos, neighborhood.length)
        context.write(new IntWritable(dstPartID), new AdjListWritable(false, vid, subNeighborhoodArray))
      }
    } else {
      // send to all related partitions
      val dstPartitions = neighborhood.map(taskPartitioner.getPartition(_)).distinct
      for (dstPartID <- dstPartitions) {
        context.write(new IntWritable(dstPartID), new AdjListWritable(false, vid, neighborhood))
      }
    }
  }

}


class NoNReducer extends Reducer[IntWritable, AdjListWritable, NullWritable, NullWritable] {

  var inMemoryGraphStoreThreshold: Int = 4

  override def setup(context: Reducer[IntWritable, AdjListWritable, NullWritable, NullWritable]#Context): Unit = {
    inMemoryGraphStoreThreshold = context.getConfiguration.getInt(HadoopNoNAnalysisFramework.CONF_IN_MEMORY_THRESHOLD, 4)
  }

  override def reduce(key: IntWritable, values: lang.Iterable[AdjListWritable],
                      context: Reducer[IntWritable, AdjListWritable, NullWritable, NullWritable]#Context): Unit = {
    val partitionID = key.get()
    System.err.println(s"Start processing partition ${partitionID}...")
    System.err.println(s"Start storing message to scalable graph format...")
    val (graphStore, parVertexRDDIter) = prepareRelatedDataStructure(values, inMemoryGraphStoreThreshold)

    val logClient = wzk.akkalogger.client.ProcessLevelClient.client
    // store messages to a compact graph format
    var partitionVertexNum: Long = graphStore.numVertices
    var partitionEdgeNum: Long = graphStore.numEdges
    val partitionVertexNumCounter = context.getCounter("PASA", "Partition Vertex Num")
    val partitionEdgeNumCounter = context.getCounter("PASA", "Partition Edge Num")
    partitionVertexNumCounter.increment(partitionVertexNum)
    partitionEdgeNumCounter.increment(partitionEdgeNum)
    System.err.println(s"Finish storing messages. Vid Num = ${partitionVertexNum}, Edge Num = $partitionEdgeNum.")
    System.err.println(s"Start computing mutual friends for each vertex...")
    // counting mutual friend by sequential algorithm
    val outputDirPath = context.getConfiguration.get(HadoopNoNAnalysisFramework.CONF_OUTPUT_DIR_PATH, HadoopNoNAnalysisFramework.INPUT_NULL_FILE)
    val outputStreamForThisPartition = if (outputDirPath != HadoopNoNAnalysisFramework.INPUT_NULL_FILE) {
      FileSystem.get(new Configuration()).create(new Path(outputDirPath, s"part-${partitionID}"))
    } else null

    val computeFunctionClassName = context.getConfiguration.get(HadoopNoNAnalysisFramework.CONF_COMPUTE_FUNCTION_CLASSNAME)

    val computeFunction: NeighborhoodComputeFunction = Class.forName(computeFunctionClassName).newInstance()
      .asInstanceOf[NeighborhoodComputeFunction]

    computeFunction.setup(outputStreamForThisPartition, context)
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

  private def prepareRelatedDataStructure(messages: lang.Iterable[AdjListWritable], inMemoryThreshold: Int)
  : (ScalableSingleNodeGraphAdjStorage, ParArray[(Long, Array[Long])]) = {
    val properties = new java.util.Properties()
    properties.setProperty(ScalableSingleNodeGraphAdjStorage.CONF_IN_MEMORY_GRAPH_STORE_THRESHOLD_MB, inMemoryThreshold.toString)
    properties.setProperty(ScalableSingleNodeGraphAdjStorage.CONF_IN_MEMORY_COMPACT_STORE, "false")
    val graphStorage = ScalableSingleNodeGraphAdjStorage.create(properties)
    val taskGraph = new ArrayBuffer[(Long, Array[Long])]()
    var messageIterator = messages.iterator()
    while (messageIterator.hasNext) {
      val adjInfo = messageIterator.next()
      if (adjInfo.belongToPartition) {
        taskGraph.append((adjInfo.vid, adjInfo.adj))
        graphStorage.addAdjList(adjInfo.vid, adjInfo.adj)
      } else {
        graphStorage.addAdjList(adjInfo.vid, adjInfo.adj)
      }

    }
    (graphStorage, taskGraph.toParArray)
  }

}
