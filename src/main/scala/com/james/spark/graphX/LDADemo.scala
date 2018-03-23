package com.james.spark.graphX

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LDADemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LDADemo").setMaster("local")
    val sc = new SparkContext(conf)

    def bagsFromDocumentPerLine(filename:String) = sc.textFile(filename)
      .map(_.split(" ")
        .filter(x => x.length > 5 && x.toLowerCase != "reuter")
        .map(_.toLowerCase)
        .groupBy(x => x)
        .toList
        .map(x => (x._1, x._2.size)))
    val rddBags:RDD[List[Tuple2[String,Int]]] = bagsFromDocumentPerLine("rcorpus")
    val vocab:Array[Tuple2[String,Long]] = rddBags.flatMap(x => x)
      .reduceByKey(_ + _)
      .map(_._1)
      .zipWithIndex
      .collect
    def codeBags(rddBags:RDD[List[Tuple2[String,Int]]]) = rddBags.map(x => (x ++ vocab).groupBy(_._1)
      .filter(_._2.size > 1)
      .map(x => (x._2(1)._2.asInstanceOf[Long]
        .toInt,
        x._2(0)._2.asInstanceOf[Int]
          .toDouble))
      .toList)
      .zipWithIndex.map(x => (x._2, new SparseVector(
      vocab.size,
      x._1.map(_._1).toArray,
      x._1.map(_._2).toArray)
      .asInstanceOf[Vector]))

//    val model = new LDA().setK(5).run(codeBags(rddBags))

  }
}
