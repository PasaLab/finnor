import cn.edu.nju.pasalab.graph.util.ScalableSingleNodeGraphAdjStorage

/**
  * Created by wangzhaokang on 6/28/17.
  */

object TestScalableGraphAdjStorage {
  def test(dbSize:Int, end:Long, degree:Int): Unit = {
    val storage = new ScalableSingleNodeGraphAdjStorage(dbSize)
    val start = 1L
    System.err.println("Start insert...")
    println(start + "," + end)
    for (i <- start to end) {
      val adj = new Array[Long](degree)
      for (j <- 0 until degree) adj(j) = i + j
      storage.addAdjList(i, adj)
    }
    System.err.println("Start check...")
    for (i <- end.to(start, -1L)) {
      val adj = storage.queryAdjList(i)
      assert(adj.length == degree)
      for (j <- 0 until degree)
        assert(adj(j) == i + j)
    }
    val adj = storage.queryAdjList(end+1)
    assert(adj == null)
    System.err.println("Pass!")

    storage.close()
  }
}
