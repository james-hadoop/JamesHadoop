package com.james.spark.graphX

import org.apache.spark.graphx.{Edge, Graph, lib}
import org.apache.spark.{SparkConf, SparkContext}

object ShortestPaths {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PageRank1").setMaster("local")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)
    myGraph.vertices.collect.foreach(println)
    myGraph.edges.collect.foreach(println)

    lib.ShortestPaths.run(myGraph, Seq(3)).vertices.collect.foreach(println)
  }
}
