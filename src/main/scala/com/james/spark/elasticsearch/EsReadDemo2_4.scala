package com.james.spark.elasticsearch

import com.james.spark.elasticsearch.EsReadDF3.map2DF
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark._

object EsReadDemo2_4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("EsReadDF").master("local").getOrCreate()

    val conf = new SparkConf().setAppName("EsReadDF").setMaster("local")
    conf.set("es.index.auto.create", "true")

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    println("--------println begin--------")

    val airportDF = map2DF(spark, sc.esRDD("spark3/json-trips", "?q=*").values)
    airportDF.createTempView("airportDF")
    println("\t**** to print ****")
    spark.sql("select * from airportDF").show

    val cityDF = map2DF2(spark, sc.esRDD("city3/json-trips", "?q=*").values)
    cityDF.createTempView("cityDF")
    println("\t**** to print ****")
    spark.sql("select * from cityDF").show

    println("\t**** to print ****")
    spark.sql("select * from airportDF a left join cityDF c on a.airport =c.airport").show

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

  def map2DF2(spark: SparkSession, rdd: RDD[scala.collection.Map[String, AnyRef]]): DataFrame = {
    val resRDD = rdd.filter(_.nonEmpty).map { m =>
      val seq = m.values.toSeq
      Row.fromSeq(seq)
    }

    val schema2 = StructType(List(
      StructField("airport", StringType, true),
      StructField("city", StringType, true)
    ))

    spark.createDataFrame(resRDD, schema2)
  }
}
