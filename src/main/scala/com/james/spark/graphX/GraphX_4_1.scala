package com.james.spark.graphX

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object GraphX_4_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphX_4_1").setMaster("local")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)
    myGraph.vertices.collect.foreach(println)
    myGraph.edges.collect.foreach(println)

    // triplets
    myGraph.triplets.collect.foreach(println)

    // mapTriplets
    myGraph.mapTriplets(t => (t.attr, t.attr == "is-friends-with" &&
      t.srcAttr.toLowerCase.contains("a"))).triplets.collect.foreach(println)

    // mapTriplets
    myGraph.mapTriplets((t => (t.attr, t.attr == "is-friends-with" &&
      t.srcAttr.toLowerCase.contains("a"))):
      (EdgeTriplet[String, String] => Tuple2[String, Boolean]))
      .triplets.collect.foreach(println)

    // aggregateMessages
    myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).collect.foreach(println)

    // aggregateMessages
    myGraph.aggregateMessages[Int](_.sendToSrc(1),
      _ + _).join(myGraph.vertices).collect.foreach(println)
  }
}
