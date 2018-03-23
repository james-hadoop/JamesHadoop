package com.james.spark.graphX

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

// Listing 6.5 Travelling Salesman greedy algorithm
object TravellingSalesman {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TravellingSalesman").setMaster("local")
    val sc = new SparkContext(conf)

    def greedy[VD](g: Graph[VD, Double], origin: VertexId) = {
      var g2 = g.mapVertices((vid, vd) => vid == origin)
        .mapTriplets(et => (et.attr, false))
      var nextVertexId = origin
      var edgesAreAvailable = true
      do {
        type tripletType = EdgeTriplet[Boolean, Tuple2[Double, Boolean]]
        val availableEdges =
          g2.triplets
            .filter(et => !et.attr._2
              && (et.srcId == nextVertexId && !et.dstAttr
              || et.dstId == nextVertexId && !et.srcAttr))
        edgesAreAvailable = availableEdges.count > 0
        if (edgesAreAvailable) {
          val smallestEdge = availableEdges
            .min()(new Ordering[tripletType]() {
              override def compare(a: tripletType, b: tripletType) = {
                Ordering[Double].compare(a.attr._1, b.attr._1)
              }
            })
          nextVertexId = Seq(smallestEdge.srcId, smallestEdge.dstId)
            .filter(_ != nextVertexId)(0)
          g2 = g2.mapVertices((vid, vd) => vd || vid == nextVertexId)
            .mapTriplets(et => (et.attr._1,
              et.attr._2 ||
                (et.srcId == smallestEdge.srcId
                  && et.dstId == smallestEdge.dstId)))
        }
      }
      while (edgesAreAvailable)
      g2
    }

    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
    val myGraph = Graph(myVertices, myEdges)

    greedy(myGraph, 1L).triplets.filter(_.attr._2).map(et => (et.srcId, et.dstId)).collect.foreach(println)
    //    (1,4)
    //    (2,3)
    //    (3,5)
    //    (4,6)
    //    (5,6)
  }
}
