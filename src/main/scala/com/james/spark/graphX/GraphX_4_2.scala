package com.james.spark.graphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeContext, Graph, VertexId}

object GraphX_4_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphX_4_2").setMaster("local")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)
    myGraph.vertices.collect.foreach(println)
    myGraph.edges.collect.foreach(println)

    // propagateEdgeCount
    val initialGraph = myGraph.mapVertices((_, _) => 0)
    propagateEdgeCount(initialGraph).vertices.collect.foreach(println)
  }

  // sendMsg function that will be given to aggregateMessages. // Remember this function will be called for each edge in the // graph. Here it simply passes on an incremented counter.
  def sendMsg(ec: EdgeContext[Int, String, Int]): Unit = {
    ec.sendToDst(ec.srcAttr + 1)
  }

  // Here we define a mergeMsg function that will be called
  // repeatedl for all messages delivered to a vertex. The end
  // result is the vertex will contain the highest value, or
  // distance, over all the messages
  def mergeMsg(a: Int, b: Int): Int = {
    math.max(a, b)
  }

  def propagateEdgeCount(g: Graph[Int, String]): Graph[Int, String] = {
    val verts = g.aggregateMessages[Int](sendMsg, mergeMsg)
    val g2 = Graph(verts, g.edges)
    // Let’s see whether the updated graph has any new information
    // by joining the two sets of vertices together – this results
    // in Tuple2[vertexId, Tuple2[old vertex data, new vertex data]]
    val check = g2.vertices.join(g.vertices).
      map(x => x._2._1 - x._2._2).
      reduce(_ + _)
    if (check > 0)
      propagateEdgeCount(g2)
    else
      g
  }


}
