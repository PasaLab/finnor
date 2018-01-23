package cn.edu.nju.pasalab.graph.util

import java.io.File
import java.util.Properties

import org.apache.commons.io.FileUtils

/**
  * Created by wangzhaokang on 6/28/17.
  */

/**
  * Scalable local graph adjacency list storage.
  * It tries to store all adjacency lists in main memory if the memory usage is less than the threshold
  * maxSizeInMemoryGB (GiB). If the memory usage exceeds the threshold, the class will store adjacency lists in
  * disk-based RocksDB.
  *
  * The class supports:
  *    - Adjacency list insert (NOT thread-safe)
  *    - Concurrent adjacency list get (thread-safe)
  * It does not support adjacency list deletion.
  *
  * @param maxSizeInMemory The threshold for in-memory adjacency list storage. Measured in Megabytes.
  */
// Read is thread-safe but write is not
class ScalableSingleNodeGraphAdjStorage() {

  import org.rocksdb.{RocksDB, Options => RocksOptions}

  /////////// Configuration
  private var useCompactStore = false
  private var maxSizeInMemory:Int = 5
  /////////// In-memory compact storage
  private var adjStorageBuffer = new com.carrotsearch.hppc.LongArrayList()
  private var startPositionIndex = new com.carrotsearch.hppc.LongIntHashMap()
  private var adjListLengthIndex = new com.carrotsearch.hppc.LongIntHashMap()
  //////////  In-memory easy-to-use storage
  private var adjLists =new com.carrotsearch.hppc.LongObjectHashMap[Array[Long]]()
  ////////// RocksDB storage
  private var useRocksDB = false
  private var rocksDBClient: RocksDB = null
  private var rocksOptions: RocksOptions = null
  private val dbFileName: String = s"adjDB-t${Thread.currentThread().getId}"
  private val FLAG_STORED_IN_ROCKSDB = -1
  ////////// Storage statistics
  var numEdges: Long = 0
  var numVertices: Long = 0

  def setup(conf:Properties) {
    useCompactStore = conf.getProperty(ScalableSingleNodeGraphAdjStorage.CONF_IN_MEMORY_COMPACT_STORE, "false").toBoolean
    maxSizeInMemory = conf.getProperty(ScalableSingleNodeGraphAdjStorage.CONF_IN_MEMORY_GRAPH_STORE_THRESHOLD_MB, "5").toInt
  }


  def addAdjList(vid: Long, adj: Array[Long]): Unit = {
    if (useRocksDB) {
      startPositionIndex.put(vid, FLAG_STORED_IN_ROCKSDB)
      rocksDBClient.put(SerializeHelper.longToBytes(vid), SerializeHelper.longsToBytes(adj))
    } else {
      if (useCompactStore) {
        startPositionIndex.put(vid, adjStorageBuffer.size)
        adjListLengthIndex.put(vid, adj.length)
        adjStorageBuffer.add(adj, 0, adj.length)
      } else {
        adjLists.put(vid, adj)
      }
      if (needToSwitchToRocksDB()) {
        System.err.println("[Info]Need to switch to rocksDB.")
        switchToRocksDB()
      }
    }
    numEdges += adj.length
    numVertices += 1
  }

  /**
    * Get adj list
    * @param vid
    * @return the adj list. null if it does not find the adj list.
    */
  def queryAdjList(vid: Long): Array[Long] = {
    if (useRocksDB) {
      val bytes = rocksDBClient.get(SerializeHelper.longToBytes(vid))
      if (bytes == null) null
      else SerializeHelper.bytesToLongs(bytes)
    } else {
      if (useCompactStore) {
        val startPos = startPositionIndex.getOrDefault(vid, -1)
        if (startPos == -1) null
        else {
          val endPos = startPos + adjListLengthIndex.getOrDefault(vid, -1)
          java.util.Arrays.copyOfRange(adjStorageBuffer.buffer, startPos, endPos)
        }
      } else {
        adjLists.getOrDefault(vid, null)
      } // use compact store
    } // use rocks db
  }

  private def needToSwitchToRocksDB(): Boolean = {
    val memoryLimitInBytes = maxSizeInMemory.toLong * (1L << 20L)
    numEdges * 8 > memoryLimitInBytes
  }

  private def switchToRocksDB() = {
    System.err.println("[Info]Start switching to rocksDB...")
    val t1 = System.currentTimeMillis()
    RocksDB.loadLibrary()
    rocksOptions = new RocksOptions().setCreateIfMissing(true)
    rocksDBClient = RocksDB.open(rocksOptions, dbFileName)
    useRocksDB = true
    if (useCompactStore) {
      // save current in-memory adj lists to db
      val posIndexIter = startPositionIndex.iterator()
      while (posIndexIter.hasNext) {
        val posInfo = posIndexIter.next()
        val vid = posInfo.key
        val pos = posInfo.value
        val length = adjListLengthIndex.get(vid)
        val adjList = java.util.Arrays.copyOfRange(adjStorageBuffer.buffer, pos, pos + length)
        rocksDBClient.put(SerializeHelper.longToBytes(vid), SerializeHelper.longsToBytes(adjList))
      }
      // clear current in-memory store
      adjStorageBuffer.clear()
      adjStorageBuffer = null
      adjListLengthIndex = null
    } else {
      // save current in-memory adj lists to db
      val iter = adjLists.iterator()
      while (iter.hasNext) {
        val cursor = iter.next()
        val vid = cursor.key
        val adj = cursor.value
        rocksDBClient.put(SerializeHelper.longToBytes(vid), SerializeHelper.longsToBytes(adj))
      }
      adjLists.clear()
    }
    val t2 = System.currentTimeMillis()
    System.err.println(s"[Info]Switching to rocksDB has finished. Elapsed time:${(t2 - t1)/1000.0}s")
  }

  def close(): Unit = {
    if (useRocksDB) {
      rocksDBClient.close()
      rocksOptions.close()
      FileUtils.deleteDirectory(new File(dbFileName))
    }
    adjStorageBuffer = null
    startPositionIndex = null
    adjListLengthIndex = null
    adjLists = null
  }

}

object ScalableSingleNodeGraphAdjStorage {
  val CONF_IN_MEMORY_COMPACT_STORE = "graphadjstore.inmemory.compactstore"
  val CONF_IN_MEMORY_GRAPH_STORE_THRESHOLD_MB = "graphadjstore.inmemory.memory.threshold.mb"

  def create(conf:Properties):ScalableSingleNodeGraphAdjStorage = {
    val store = new ScalableSingleNodeGraphAdjStorage()
    if (conf != null)
      store.setup(conf)
    store
  }
}
