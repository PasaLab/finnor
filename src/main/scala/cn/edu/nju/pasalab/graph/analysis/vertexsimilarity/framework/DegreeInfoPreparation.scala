package cn.edu.nju.pasalab.graph.analysis.vertexsimilarity.framework

import cn.edu.nju.pasalab.graph.util.MyHDSFUtils
import gnu.trove.map.hash.TLongIntHashMap
import com.carrotsearch.hppc.LongIntHashMap
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.io.Source

/**
  * Created by wangzhaokang on 8/8/17.
  */
object DegreeInfoPreparation {
  def prepareDegreeInfo(graph: RDD[(Long, Array[Long])]): LongIntHashMap = {
    MyHDSFUtils.deleteDir("DegreeInfo")
    graph.mapValues(_.length).map(x => s"${x._1} ${x._2}").saveAsTextFile("DegreeInfo")
    val degreeMap = new LongIntHashMap()
    MyHDSFUtils.listFiles("DegreeInfo").foreach(file => {
      val inputStream = MyHDSFUtils.getFileSystem().open(file)
      val source = Source.fromInputStream(inputStream)
      for (line <- source.getLines()) {
        val vid = line.split(" ")(0).toLong
        val degree = line.split(" ")(1).toInt
        degreeMap.put(vid, degree)
      }
      source.close()
      inputStream.close()
    })
    MyHDSFUtils.deleteDir("DegreeInfo")
    degreeMap
  }

  def main(args:Array[String]) = {
    val sc = new SparkContext(new SparkConf().setAppName("Degree file generation"))
    val graph = args(0)
    val adjFile = s"$graph/resort"
    val degreeFile = s"$graph/degree"

    val degreeRDD = sc.textFile(adjFile).map(line => {
      val fields = line.split(" ")
      s"${fields(0).toLong} ${fields.length - 1}"
    })
    degreeRDD.saveAsTextFile(degreeFile)
  }
  def getDegreeInfoFromDegreeFile(graphPath:String):TLongIntHashMap = {
    val degreeMap = new TLongIntHashMap()
    MyHDSFUtils.listFiles(s"$graphPath/degree").foreach(file => {
      val inputStream = MyHDSFUtils.getFileSystem().open(file)
      val source = Source.fromInputStream(inputStream)
      for (line <- source.getLines()) {
        val vid = line.split(" ")(0).toLong
        val degree = line.split(" ")(1).toInt
        degreeMap.put(vid, degree)
      }
      source.close()
      inputStream.close()
    })
    MyHDSFUtils.deleteDir("DegreeInfo")
    degreeMap
  }

}
