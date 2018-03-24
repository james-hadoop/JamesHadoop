package com.james.spark.graphX

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import java.awt.Color
import scala.util.Random

// Listing 7.15 Execute K-Nearest Neighbors on the example data and export to .gexf
object KNearestNeighbors {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KNearestNeighbors").setMaster("local")
    val sc = new SparkContext(conf)

    case class knnVertex(classNum: Option[Int],
                         pos: Array[Double]) extends Serializable {
      def dist(that: knnVertex) = math.sqrt(
        pos.zip(that.pos).map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _))
    }

    def knnGraph(a: Seq[knnVertex], k: Int) = {
      val a2 = a.zipWithIndex.map(x => (x._2.toLong, x._1)).toArray
      val v = sc.makeRDD(a2)
      val e = v.map(v1 => (v1._1, a2.map(v2 => (v2._1, v1._2.dist(v2._2)))
        .sortWith((e, f) => e._2 < f._2)
        .slice(1, k + 1)
        .map(_._1)))
        .flatMap(x => x._2.map(vid2 =>
          Edge(x._1, vid2,
            1 / (1 + a2(vid2.toInt)._2.dist(a2(x._1.toInt)._2)))))
      Graph(v, e)
    }

    def toGexfWithViz(g: Graph[knnVertex, Double], scale: Double) = {
      val colors = Array(Color.red, Color.blue, Color.yellow, Color.pink,
        Color.magenta, Color.green, Color.darkGray)
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<gexf xmlns=\"http://www.gexf.net/1.2draft\" " +
        "xmlns:viz=\"http://www.gexf.net/1.1draft/viz\" " +
        "version=\"1.2\">\n" +
        "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
        "    <nodes>\n" +
        g.vertices.map(v =>
          "      <node id=\"" + v._1 + "\" label=\"" + v._1 + "\">\n" +
            "        <viz:position x=\"" + v._2.pos(0) * scale +
            "\" y=\"" + v._2.pos(1) * scale + "\" />\n" +
            (if (v._2.classNum.isDefined)
              "        <viz:color r=\"" + colors(v._2.classNum.get).getRed +
                "\" g=\"" + colors(v._2.classNum.get).getGreen +
                "\" b=\"" + colors(v._2.classNum.get).getBlue + "\" />\n"
            else "") +
            "    </node>\n").collect.mkString +
        "    </nodes>\n" +
        "    <edges>\n" +
        g.edges.map(e => "      <edge source=\"" + e.srcId +
          "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
          "\" />\n").collect.mkString +
        "    </edges>\n" +
        "  </graph>\n" +
        "</gexf>"
    }

    Random.setSeed(17L)
    val n = 10
    val a = (1 to n * 2).map(i => {
      val x = Random.nextDouble;
      if (i <= n)
        knnVertex(if (i % n == 0) Some(0) else None, Array(x * 50,
          20 + (math.sin(x * math.Pi) + Random.nextDouble / 2) * 25))
      else
        knnVertex(if (i % n == 0) Some(1) else None, Array(x * 50 + 25,
          30 - (math.sin(x * math.Pi) + Random.nextDouble / 2) * 25))
    })

    val g = knnGraph(a, 4)
    val pw = new java.io.PrintWriter("data/knn.gexf")
    pw.write(toGexfWithViz(g, 10))
    pw.close
  }
}
