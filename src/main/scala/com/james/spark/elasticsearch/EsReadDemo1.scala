package com.james.spark.elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._


object EsReadDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("EsReadDemo1").setMaster("local")
        conf.set("es.index.auto.create", "true")

        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        println("--------println begin--------")
        sc.esRDD("radio/artists", "?q=e*").collect().foreach(println)
        println("--------println end--------")
    }
}
