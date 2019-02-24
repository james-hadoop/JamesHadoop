package com.james.spark.elasticsearch

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark._

object EsReadDF3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("EsReadDF").master("local").getOrCreate()

    val conf = new SparkConf().setAppName("EsReadDF").setMaster("local")
    conf.set("es.index.auto.create", "true")

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    println("--------println begin--------")
    val df3 = map2DF(spark, sc.esRDD("spark3/json-trips", "?q=*").values)
    df3.createTempView("df3")
    spark.sql("select * from df3").show

    println("--------println end--------")
  }

  def map2DF(spark: SparkSession, rdd: RDD[scala.collection.Map[String, AnyRef]]): DataFrame = {
    val resRDD = rdd.filter(_.nonEmpty).map { m =>
      val seq = m.values.toSeq
      Row.fromSeq(seq)
    }

    val schema = StructType(List(
      StructField("reason", StringType, true),
      StructField("participants", LongType, true),
      StructField("airport", StringType, true)
    ))

    spark.createDataFrame(resRDD, schema)
  }
}
