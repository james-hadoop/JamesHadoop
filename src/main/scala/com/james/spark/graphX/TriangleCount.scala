package com.james.spark.graphX

import org.apache.spark.graphx.{Edge, Graph, GraphLoader, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}

// Listing 5.2 Triangle Counts on Slashdot friend and foe data
object TriangleCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TriangleCount").setMaster("local")
    val sc = new SparkContext(conf)

    val g = GraphLoader.edgeListFile(sc, "soc-Slashdot0811.txt").cache
    val g2 = Graph(g.vertices, g.edges.map(e =>
      if (e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr))).
      partitionBy(PartitionStrategy.RandomVertexCut)
    (0 to 6).map(i => g2.subgraph(vpred =
      (vid, _) => vid >= i * 10000 && vid < (i + 1) * 10000).
      triangleCount.vertices.map(_._2).reduce(_ + _)).foreach(println)
  }
}