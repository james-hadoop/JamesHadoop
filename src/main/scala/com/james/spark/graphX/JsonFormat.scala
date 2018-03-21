package com.james.spark.graphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonFormat {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JsonFormat").setMaster("local")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)
    myGraph.vertices.collect.foreach(println)
    myGraph.edges.collect.foreach(println)

    myGraph.vertices.map(x => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(
        com.fasterxml.jackson.module.scala.DefaultScalaModule)
      val writer = new java.io.StringWriter()
      mapper.writeValue(writer, x)
      writer.toString
    }).coalesce(1, true).saveAsTextFile("data/myGraphVerticesInJson")

    // Listing 4.15 Better performing way to serialize/deserialize to/from JSON
    myGraph.vertices.map(x => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(
        com.fasterxml.jackson.module.scala.DefaultScalaModule)
      val writer = new java.io.StringWriter()
      mapper.writeValue(writer, x)
      writer.toString
    }).coalesce(1, true).saveAsTextFile("data/myGraphVerticesA")

    myGraph.vertices.mapPartitions(vertices => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val writer = new java.io.StringWriter()
      vertices.map(v => {
        writer.getBuffer.setLength(0)
        mapper.writeValue(writer, v)
        writer.toString
      })
    }).coalesce(1, true).saveAsTextFile("data/myGraphVerticesB")
    myGraph.edges.mapPartitions(edges => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper();
      mapper.registerModule(DefaultScalaModule)
      val writer = new java.io.StringWriter()
      edges.map(e => {
        writer.getBuffer.setLength(0)
        mapper.writeValue(writer, e)
        writer.toString
      })
    }).coalesce(1, true).saveAsTextFile("data/myGraphEdgesB")
    val myGraph2 = Graph(
      sc.textFile("data/myGraphVerticesB").mapPartitions(vertices => {
        val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        vertices.map(v => {
          val r = mapper.readValue[Tuple2[Integer, String]](v, new TypeReference[Tuple2[Integer, String]] {})
          (r._1.toLong, r._2)
        })
      }),
      sc.textFile("data/myGraphEdgesB").mapPartitions(edges => {
        val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        edges.map(e => mapper.readValue[Edge[String]](e,
          new TypeReference[Edge[String]] {}))
      }))
  }
}
