package com.james.spark.elasticsearch

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark._

object EsReadDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("EsReadDF").master("local").getOrCreate()

    val conf = new SparkConf().setAppName("EsReadDF").setMaster("local")
    conf.set("es.index.auto.create", "true")

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    println("--------println begin--------")

    import spark.implicits._

//    sc.esRDD("spark/json-trips", "?q=*").collect().foreach(println)
//    sc.esJsonRDD("spark/json-trips", "?q=*").collect().foreach(println)
//
//    val df = sc.esJsonRDD("spark/json-trips", "?q=*").toDF("key", "value")
//    df.printSchema()
//    df.createGlobalTempView("airports")
//    spark.sql("SELECT value FROM global_temp.airports").show()
//    spark.sql("SELECT value FROM global_temp.airports").toJSON.collect().foreach(println)


    //      sc.esRDD("spark/json-trips", "?q=*").keys.map(_.toSeq).foreach(println)
    //      sc.esRDD("spark/json-trips", "?q=*").values.collect().foreach(println)
    sc.esRDD("spark/json-trips", "?q=*").values.map{m=>val seq = m.values.toSeq
      Row.fromSeq(seq)}.collect().foreach(println)

    val schema = StructType(List(
      StructField("reason", StringType, true),
      StructField("participants", StringType, false),
      StructField("airport", StringType, true)
    ))

    val df3=map2DF(spark,sc.esRDD("spark2/json-trips", "?q=*").values)
    df3.createTempView("df3")
    spark.sql("select * from df3").show

    println("--------println end--------")
  }

  def map2DF(spark: SparkSession, rdd: RDD[scala.collection.Map[String, AnyRef]]): DataFrame = {
    val cols = rdd.take(1).flatMap(_.keys)
    val resRDD = rdd.filter(_.nonEmpty).map { m =>
      val seq = m.values.toSeq
      Row.fromSeq(seq)
    }

    val fields = cols.map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    spark.createDataFrame(resRDD, schema)
  }
}
