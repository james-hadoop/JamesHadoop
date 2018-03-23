package com.james.spark.graphX

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

// Listing 6.6 Minimum Spanning Tree
object MinimumSpanningTree {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MinimumSpanningTree").setMaster("local")
    val sc = new SparkContext(conf)

    def minSpanningTree[VD: scala.reflect.ClassTag](g: Graph[VD, Double]) = {
      var g2 = g.mapEdges(e => (e.attr, false))
      for (i <- 1L to g.vertices.count - 1) {
        val unavailableEdges =
          g2.outerJoinVertices(g2.subgraph(_.attr._2)
            .connectedComponents
            .vertices)((vid, vd, cid) => (vd, cid))
            .subgraph(et => et.srcAttr._2.getOrElse(-1) ==
              et.dstAttr._2.getOrElse(-2))
            .edges
            .map(e => ((e.srcId, e.dstId), e.attr))
        type edgeType = Tuple2[Tuple2[VertexId, VertexId], Double]
        val smallestEdge =
          g2.edges
            .map(e => ((e.srcId, e.dstId), e.attr))
            .leftOuterJoin(unavailableEdges)
            .filter(x => !x._2._1._2 && x._2._2.isEmpty)
            .map(x => (x._1, x._2._1._1))
            .min()(new Ordering[edgeType]() {
              override def compare(a: edgeType, b: edgeType) = {
                val r = Ordering[Double].compare(a._2, b._2)
                if (r == 0)
                  Ordering[Long].compare(a._1._1, b._1._1)
                else
                  r
              }
            })
        g2 = g2.mapTriplets(et =>
          (et.attr._1, et.attr._2 || (et.srcId == smallestEdge._1._1
            && et.dstId == smallestEdge._1._2)))
      }
      g2.subgraph(_.attr._2).mapEdges(_.attr._1)
    }

    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
    val myGraph = Graph(myVertices, myEdges)

    minSpanningTree(myGraph).triplets.map(et =>
      (et.srcAttr, et.dstAttr)).collect.foreach(println)
    //    (A,B)
    //    (A,D)
    //    (B,E)
    //    (C,E)
    //    (D,F)
    //    (E,G)
  }
}
