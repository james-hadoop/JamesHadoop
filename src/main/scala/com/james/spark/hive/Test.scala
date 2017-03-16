package com.james.spark.hive

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by james on 17-3-6.
  */
object Test extends App {
    System.setProperty("hive.metastore.uris", "thrift://localhost:9083")

    val conf = new SparkConf().setAppName("Test")

    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val hivesc = new HiveContext(sc)

    val df = hivesc.sql("select * from mydb.u").toDF()

    df.printSchema()


//    val spark = SparkSession
//      .builder()
//      .appName("Spark Hive Example")
//      //.config("spark.sql.warehouse.dir", warehouseLocation)
//      .enableHiveSupport()
//      .getOrCreate()
//
//    import spark.sql
//
//    sql("select * from mydb.u").show()
}
