package com.james.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by james on 16-9-10.
  */
object TopK {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("TopK").setMaster("local");
        val sc = new SparkContext(conf)

        val count = sc.textFile("/Users/qjiang/install/spark/README.md").flatMap(line =>
            line.split(" ")).map(word =>
            (word, 1)).reduceByKey(_ + _)

        val topK = count.mapPartitions(iter => {
            while (iter.hasNext) {
                putToHeap(iter.next())
            }
            getHeap(10).iterator
        }
        ).collect()

        print(topK)
    }

    def putToHeap(iter: (String, Int)): Unit = {

    }

    def getHeap(k: Int): Array[(String, Int)] = {
        val a = new Array[(String, Int)](k)
        return a
    }
}