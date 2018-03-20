package com.james.spark.graphX

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

object GraphX1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphX1").setMaster("local")
    val sc = new SparkContext(conf)

    val vertices = sc.makeRDD(Array((1L, ("SFO")), (2L, ("ORD")), (3L, ("DFW"))))
    val edges = sc.makeRDD(Array(Edge(1L, 2L, 1800), Edge(2L, 3L, 800), Edge(3L, 1L, 1400)))

    val nowhere = "nowhere"
    val graph = Graph(vertices, edges, nowhere)
    graph.vertices.take(3).foreach(println)

    val numairports = graph.numVertices
    val numroutes = graph.numEdges
    println("numairports=" + numairports)
    println("numroutes=" + numroutes)

    // routes > 1000 miles distance?
    val routes = graph.edges.filter { case (Edge(org_id, dest_id, distance)) => distance > 1000 }.take(3)
    println("routes: " + routes)

    // Triplets add source and destination properties to Edges
    graph.triplets.take(3).foreach(println)

    // print out longest routes
    graph.triplets.sortBy(_.attr, ascending = false).map(triplet => "Distance" + triplet.attr.toString + "from" + triplet.srcAttr + "to" + triplet.dstAttr).collect.foreach(println)
  }
}
