package com.james.spark.graphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object GraphGenerator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("data/graphGenerator").setMaster("local")
    val sc = new SparkContext(conf)

    // Listing 4.17 Generate a grid graph
    var pw = new java.io.PrintWriter("data/gridGraph.gexf")
    pw.write("")
    pw.write(toGexf(util.GraphGenerators.gridGraph(sc, 4, 4)))
    pw.close()

    // Listing 4.18 Generate a star graph
    pw = new java.io.PrintWriter("data/starGraph.gexf")
    pw.write(toGexf(util.GraphGenerators.starGraph(sc, 8)))
    pw.close

    // Listing 4.19 Generate a log normal graph
    val logNormalGraph = util.GraphGenerators.logNormalGraph(sc, 15)
    pw = new java.io.PrintWriter("data/logNormalGraph.gexf")
    pw.write(toGexf(logNormalGraph))
    pw.close
    logNormalGraph.aggregateMessages[Int](
      _.sendToSrc(1), _ + _).map(_._2).collect.sorted.foreach(println)
  }

  def toGexf[VD, ED](g: Graph[VD, ED]) =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges.map(e => "      <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"
}
