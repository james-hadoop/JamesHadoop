package com.james.spark.graphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

object PageRank1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PageRank1").setMaster("local")
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, "cit-HepTh.txt")
    graph.inDegrees.reduce((a, b) => if (a._2 > b._2) a else b)

    val v = graph.pageRank(0.001).vertices

    v.take(10).foreach(println)

    println(v.reduce((a, b) => if (a._2 > b._2) a else b))
  }
}
