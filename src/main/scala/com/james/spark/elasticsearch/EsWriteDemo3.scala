package com.james.spark.elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._


object EsWriteDemo3 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("EsWriteDemo3").setMaster("local")
        conf.set("es.index.auto.create", "true")
        conf.set("spark.driver.allowMultipleContexts", "true")

        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
        val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
        val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

        sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection-{media_type}/doc")
    }
}
