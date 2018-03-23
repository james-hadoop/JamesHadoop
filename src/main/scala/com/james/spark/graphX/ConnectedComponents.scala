package com.james.spark.graphX

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object ConnectedComponents {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConnectedComponents").setMaster("local")
    val sc = new SparkContext(conf)

    // Listing 5.4 Invoking connectedComponents()
    val g = Graph(sc.makeRDD((1L to 7L).map((_, ""))),
      sc.makeRDD(Array(Edge(2L, 5L, ""), Edge(5L, 3L, ""), Edge(3L, 2L, ""),
        Edge(4L, 5L, ""), Edge(6L, 7L, "")))).cache
    g.connectedComponents.vertices.map(_.swap).groupByKey.map(_._2).collect.foreach(println)
    // CompactBuffer(1)
    // CompactBuffer(6, 7)
    // CompactBuffer(4, 3, 5, 2)
  }
}
