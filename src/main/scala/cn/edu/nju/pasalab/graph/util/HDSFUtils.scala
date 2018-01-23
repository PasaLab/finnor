package cn.edu.nju.pasalab.graph.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by wangzhaokang on 8/8/17.
  */
object MyHDSFUtils {

  def getFileSystem() = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    fs
  }

  def deleteDir(path:String): Unit = {
   getFileSystem().delete(new Path(path), true)
  }

  def listFiles(path:String):Seq[Path] = {
    val fs = getFileSystem()
    val statuses = fs.listStatus(new Path(path))
    statuses.map(_.getPath)
  }

}
