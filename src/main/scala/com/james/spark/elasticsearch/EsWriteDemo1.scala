package com.james.spark.elasticsearch

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.elasticsearch.spark._


object EsWriteDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("EsWriteDemo1").setMaster("local")
        conf.set("es.index.auto.create", "true")

        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        case class Artist(name: String, albums: Int)

        val u2 = Artist("U2", 12)
        val bh = Map("name" -> "Buckethead", "albums" -> 95, "age" -> 45)
        sc.makeRDD(Seq(u2, bh)).saveToEs("radio/artists")
    }
}
