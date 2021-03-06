package com.james.spark.elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._


object EsWriteDemo2_4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("EsWriteDemo2").setMaster("local")
    conf.set("es.index.auto.create", "true")
    conf.set("spark.driver.allowMultipleContexts", "true")

    val json1 = """{"airport" : "SFO","city":"Shanghai"}"""
    val json2 = """{"airport" : "OTP","city":"Beijing"}"""
    val json3 = """{"airport" : "ABC","city":"Tianjin"}"""

    new SparkContext(conf).makeRDD(Seq(json1, json2,json3))
      .saveJsonToEs("city3/json-trips")

    val json4 = """{"reason" : "business", "participants" :3,"airport" : "SFO"}"""
    val json5 = """{"reason" : "personal","participants" :2, "airport" : "OTP"}"""

    new SparkContext(conf).makeRDD(Seq(json4, json5))
      .saveJsonToEs("spark3/json-trips")
  }
}
