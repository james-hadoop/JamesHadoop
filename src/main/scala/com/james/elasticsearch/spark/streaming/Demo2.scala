package com.james.elasticsearch.spark.streaming

import java.io.{FileNotFoundException, IOException}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import org.elasticsearch.spark.streaming._

import scala.collection.mutable
import scala.io.Source

/**
  * Created by Tinkpad on 2017/2/3.
  */
object Demo2 {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: Demo2 <running_machine_ip_address>")
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
    // demo2_1
    val ssc = new StreamingContext(sc, Seconds(1))

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    val rdd = sc.makeRDD(Seq(numbers, airports))
    val microbatches = mutable.Queue(rdd)

    ssc.queueStream(microbatches).saveToEs("demo2_1/docs")

    ssc.start()
    ssc.awaitTermination()
  }
}
