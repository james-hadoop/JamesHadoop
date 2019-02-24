package com.james.spark.elasticsearch

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark._

object EsReadDF2 {
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

    sc.esRDD("spark/json-trips", "?q=*").collect().foreach(println)
    sc.esJsonRDD("spark/json-trips", "?q=*").collect().foreach(println)

    val schema = StructType(List(
      StructField("_id", StringType, true),
      StructField("reason", StringType, true),
      StructField("participants", IntegerType, false),
      StructField("airport", StringType, true)
    ))

    val df=sc.esJsonRDD("spark/json-trips", "?q=*").toDF("key","value")
    df.printSchema()
    df.createGlobalTempView("airports")
    spark.sql("SELECT value FROM global_temp.airports").show()
    spark.sql("SELECT value FROM global_temp.airports").toJSON.collect().foreach(println)

    val df2=sc.esJsonRDD("spark/json-trips", "?q=*").toDF
    df2.printSchema()
//   val colsRdd=spark.sparkContext.esRDD()
//
//    val df1=map2DF(spark,colsRdd)
//    df1.printSchema()
//    df1.createGlobalTempView("df1")
//    spark.sql("select * from global_temp.df1").show()


    println("--------println end--------")
  }

  def map2DF(spark: SparkSession, rdd: RDD[Map[String, String]]): DataFrame = {
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
