package com.james.spark.graphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}

object GraphX2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphX2").setMaster("local")
    val sc = new SparkContext(conf)

    val vertices = sc.makeRDD(Array((1L, ("SFO")), (2L, ("ORD")), (3L, ("DFW"))))
    val edges = sc.makeRDD(Array(Edge(1L, 2L, 1800), Edge(2L, 3L, 800), Edge(3L, 1L, 1400)))

    val nowhere = "nowhere"
    val graph = Graph(vertices, edges, nowhere)

    // Define a function to compute the highest degree vertex
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    // Which Airport has the most incoming flights?
    val maxInDegree: (VertexId, Int) = graph.inDegrees.reduce(max)
    println("maxInDegree=" + maxInDegree)

    // starting vertex
    val sourceId: VertexId = 13024
    // a graph with edges containing airfare cost calculation
    val gg = graph.mapEdges(e => 50.toDouble + e.attr.toDouble / 20)
    // initialize graph, all vertices except source have distance infinity
    val initialGraph = gg.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)

    // call pregel on graph
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      // Vertex Program
      (id, distCost, newDistCost) => math.min(distCost, newDistCost),
      triplet => {
        // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      // Merge Message
      (a, b) => math.min(a, b)
    )

    // routes , lowest flight cost
    println(sssp.edges.take(4).mkString("\n"))
  }
}
