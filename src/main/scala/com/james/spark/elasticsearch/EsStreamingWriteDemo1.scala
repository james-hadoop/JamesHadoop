package com.james.spark.elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.elasticsearch.spark.streaming._

import scala.collection.mutable


object EsStreamingWriteDemo1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("EsStreamingWriteDemo1").setMaster("local")
        conf.set("es.index.auto.create", "true")

        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val ssc = new StreamingContext(sc, Seconds(1))

        val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
        val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

        val rdd = sc.makeRDD(Seq(numbers, airports))
        val microbatches = mutable.Queue(rdd)

        ssc.queueStream(microbatches).saveToEs("spark-streaming/docs")

        ssc.start()
        ssc.awaitTermination()
    }
}
