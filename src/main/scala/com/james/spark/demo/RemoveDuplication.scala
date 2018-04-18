package com.james.spark.demo

import org.apache.spark.sql.{Row, SparkSession}

object RemoveDuplication {
  def main(args: Array[String]): Unit = {
    System.setProperty("hive.metastore.uris", "thrift://localhost:9083")
    System.setProperty("driver-class-path", "/home/james/install/spark/jars/mysql-connector-java-5.1.38-bin.jar")

    println("SparkSqlDemo start...")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example").master("local")
      .config("spark.some.config.option", "some-value")
      .config("driver-class-path", "/Users/qjiang/software/mysql-connector-java-5.1.44/mysql-connector-java-5.1.44-bin.jar")
      .getOrCreate()


    // This import is needed to use the $-notation
    import spark.implicits._

    val usersDF1 = spark.read.load("users.parquet").show()
    println("-----------------------------------------------------------------")
    val usersDF = spark.read.load("users.parquet").dropDuplicates("name").show()

    println("-----------------------------------------------------------------")
    val df = spark.read.json("people.json")
    df.show()
    println("-----------------------------------------------------------------")
    df.dropDuplicates("age").show()

  }
}
