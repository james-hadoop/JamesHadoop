package com.james.spark.graphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

object PageRank2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PageRank1").setMaster("local")
    val sc = new SparkContext(conf)

    val g = GraphLoader.edgeListFile(sc, "cit-HepTh.txt")

    val result = g.personalizedPageRank(9207016, 0.001)
      .vertices
      .filter(_._1 != 9207016)
      .reduce((a, b) => if (a._2 > b._2) a else b)

    // (9201015,0.09211875000000003)
    println(result)
  }
}
