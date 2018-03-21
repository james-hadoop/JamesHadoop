package com.james.spark.graphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}

object Serialization {
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


    // Listing 4.11 Round-trip persisting to and reading from file
    //myGraph.vertices.saveAsObjectFile("data/myGraphVertices")
    //myGraph.edges.saveAsObjectFile("data/myGraphEdges")
    val myGraph2 = Graph(
      sc.objectFile[Tuple2[VertexId, String]]("data/myGraphVertices"), sc.objectFile[Edge[String]]("data/myGraphEdges"))
    myGraph2.vertices.collect.foreach(println)
    myGraph2.edges.collect.foreach(println)

    myGraph2.vertices.saveAsObjectFile("hdfs://localhost:9000/data/spark/graphX/myGraphVertices")
  }
}
