package com.james.spark.graphX

import org.apache.spark.graphx.{Edge, Graph, lib}
import org.apache.spark.{SparkConf, SparkContext}

// Listing 5.7 Invoking LabelPropagation
object LabelPropagation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LabelPropagation").setMaster("local")
    val sc = new SparkContext(conf)

    val v = sc.makeRDD(Array((1L, ""), (2L, ""), (3L, ""), (4L, ""), (5L, ""),
      (6L, ""), (7L, ""), (8L, "")))
    val e = sc.makeRDD(Array(Edge(1L, 2L, ""), Edge(2L, 3L, ""), Edge(3L, 4L, ""), Edge(4L, 1L, ""), Edge(1L, 3L, ""), Edge(2L, 4L, ""), Edge(4L, 5L, ""), Edge(5L, 6L, ""), Edge(6L, 7L, ""), Edge(7L, 8L, ""), Edge(8L, 5L, ""), Edge(5L, 7L, ""), Edge(6L, 8L, "")))
    lib.LabelPropagation.run(Graph(v, e), 5).vertices.collect.
      sortWith(_._1 < _._1).foreach(println)
    //    (1,2)
    //    (2,1)
    //    (3,1)
    //    (4,2)
    //    (5,4)
    //    (6,5)
    //    (7,5)
    //    (8,4)
  }
}
