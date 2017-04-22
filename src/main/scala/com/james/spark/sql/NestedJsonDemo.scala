package com.james.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by james on 17-4-22.
  */
object NestedJsonDemo extends App {
    //    System.setProperty("hive.metastore.uris", "thrift://localhost:9083")
    //    System.setProperty("driver-class-path", "/home/james/install/spark/jars/mysql-connector-java-5.1.38-bin.jar")

    println("NestedJsonDemo start...")

    val spark = SparkSession
      .builder()
      .appName("NestedJsonDemo")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.read.json("nested.json")
    df.show()
    df.printSchema()
    df.select("name").show()

    println("temporary\n")
    df.createOrReplaceTempView("nested")

    val sqlDF = spark.sql("SELECT * FROM nested")
    sqlDF.show()

    println("save...")
    //df.toJSON.write.save("nested.json.save")
    df.write.json("nested.json.sav")
}
