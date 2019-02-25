package com.james.spark.elasticsearch

import java.util

import org.apache.spark.sql.{SparkSession}

object EsReadDemo3 extends App {
  val spark = SparkSession
    .builder()
    .appName("EsReadDemo3").master("local").getOrCreate()

  val esOptions = new util.HashMap[String, String]()
  esOptions.put("es.nodes", "localhost")
  esOptions.put("es.port", "9200")
  esOptions.put("es.index.auto.create", "true")

  val orderDF = spark.read.format("org.elasticsearch.spark.sql").options(esOptions).load("spark3/json-trips")
  orderDF.createTempView("orderDF")

  spark.sql("select * from orderDF").show()
}
