package com.james.elasticsearch.spark.core

import java.io.{FileNotFoundException, IOException}

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

import scala.io.Source

/**
  * Created by Tinkpad on 2017/2/3.
  */
object Demo1 {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: Demo1 <running_machine_ip_address>")
      System.exit(1)
    }

    // get arguments from application
    val Array(runningMachineIpAddress) = args
    println(s"runningMachineIpAddress=$runningMachineIpAddress")


    val filename = "name.txt"

    try {
      for (line <- Source.fromFile(filename).getLines) {
        println(line);
      }
    } catch {
      case e: FileNotFoundException => println("Couldn't find that file.")
      case e: IOException => println("Got an IOException!")
    }

    // system configuration
    System.setProperty("user.name", "jiangqian")
    System.setProperty("HADOOP_USER_NAME", "jiangqian")
    System.setProperty("hive.metastore.uris", "thrift://w5-hadoop.esf.fdd:9083")

    // SparkConf
    val conf = new SparkConf().set("es.nodes", runningMachineIpAddress)
      .set("es.port", "9200").setAppName("ElasticSearchDemo1")
    conf.set("spark.driver.extraJavaOptions", "-Dhdp.version=2.3.4.0-3485")
    conf.set("spark.yarn.am.extraJavaOptions", "-Dhdp.version=2.3.4.0-3485")

    //conf.set("hive.metastore.uris", "thrift://w5-hadoop.esf.fdd:9083,thrift://w9-hadoop.esf.fdd:9083")
    conf.set("spark.app.id", "ElasticSearchDemo1")
    conf.set("spark.yarn.jar", "hdfs://m-hadoop.esf.fdd:8020/tmp/spark-assembly-1.5.2.2.3.4.0-3485-hadoop2.7.1.2.3.4.0-3485.jar")
    conf.set("spark.hadoop.dfs.nameservices", "m-hadoop.esf.fdd:8020")
    conf.set("spark.driver.host", "10.50.23.211")
    conf.set("spark.hadoop.fs.defaultFS", "hdfs://m-hadoop.esf.fdd:8020/user/jiangqian/")
    conf.set("spark.hadoop.yarn.resourcemanager.hostname", "m-hadoop.esf.fdd")
    conf.set("spark.hadoop.yarn.resourcemanager.address", "m-hadoop.esf.fdd:8050")
    conf.set("spark.yarn.am.cores", "2")
    conf.set("spark.executor.instances", "10")
    conf.set("spark.sql.tungsten.enabled", "false")
    conf.set("spark.sql.shuffle.partitions", "1")
    conf.set("spark.default.parallelism", "1")
    conf.set("es.index.auto.create", "true")

    // add jars
    conf.setMaster("yarn-client")
    conf.setJars(List("spark_lab.jar", "elasticsearch-hadoop-5.0.0.jar"))

    // SparkContext
    val sc = new SparkContext(conf)
    sc.addJar("hdfs://m-hadoop.esf.fdd:8020/user/oozie/share/lib/lib_20151229093730/sqoop/mysql-connector-java-5.1.36.jar")

    /*
     * write indices
     */
    // example1
    case class Trip(departure: String, arrival: String)

    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    EsSpark.saveToEs(rdd, "example1/docs")

    // example2
    val json1 =
    """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""

    sc.makeRDD(Seq(json1, json2)).saveJsonToEs("example2/json-trips")

    // example3
    val game = Map("media_type" -> "game", "title" -> "FF VI", "year" -> "1994")
    val book = Map("media_type" -> "book", "title" -> "Harry Potter", "year" -> "2010")
    val cd = Map("media_type" -> "music", "title" -> "Surfing With The Alien")

    sc.makeRDD(Seq(game, book, cd)).saveToEs("my-example3/{media_type}")

    // example4
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
    airportsRDD.saveToEsWithMeta("example4/2015")

    // example5
    val otp5 = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc5 = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo5 = Map("iata" -> "SFO", "name" -> "San Fran")

    // metadata for each document
    // note it's not required for them to have the same structure
    val otpMeta = Map("ID" -> 1, "TTL" -> "3h")
    val mucMeta = Map("ID" -> 2, "VERSION" -> "23")
    val sfoMeta = Map("ID" -> 3)

    val airportsRDD5 = sc.makeRDD(Seq((otpMeta, otp5), (mucMeta, muc5), (sfoMeta, sfo5)))
    airportsRDD5.saveToEsWithMeta("example5/2015")

    /*
     * read indices
     */
    val RDD1 = sc.esRDD("example1/docs")
    val RDD2 = sc.esRDD("example2/json-trips", "?q=*pants")

    RDD1.saveAsTextFile("example1/docs")
    RDD2.saveAsTextFile("example2/json-trips")
  }
}
