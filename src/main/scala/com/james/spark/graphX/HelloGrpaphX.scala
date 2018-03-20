package com.james.spark.graphX

import org.apache.spark.graphx.{Edge, Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

object HelloGrpaphX {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HelloGrpaphX").setMaster("local")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
    val myGraph = Graph(myVertices, myEdges)
    myGraph.vertices.collect.foreach(print)

    print(myGraph.numVertices);
    print(myGraph.numEdges)


    Thread.sleep(1000 * 60)
    sc.stop()
  }
}
