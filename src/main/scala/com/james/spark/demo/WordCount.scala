package com.james.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by james on 16-9-3.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WorldCount").setMaster("local")
    val sc = new SparkContext(conf)
    val line = sc.textFile("/home/james/install/spark/README.md")
    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

    Thread.sleep(1000 * 30)
    sc.stop()
  }
}