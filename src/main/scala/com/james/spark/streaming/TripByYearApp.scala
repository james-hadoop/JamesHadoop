package com.james.spark.streaming

import java.util.Calendar

import kafka.utils.Logging
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by james on 17-1-2.
  */
object TripByYearApp extends Logging {
    def main(args: Array[String]) {
        if (args.length != 3) {
            System.err.println(
                "Usage: TripByYearApp <appname> <hostname> <port>")
            System.exit(1)
        }
        val Seq(appName, hostname, port) = args.toSeq

        val conf = new SparkConf()
          .setAppName(appName)
          .setJars(SparkContext.jarOfClass(this.getClass).toSeq).setMaster("local")

        val ssc = new StreamingContext(conf, Seconds(10))

        ssc.socketTextStream(hostname, port.toInt)
          .flatMap(rec =>
              rec.split(" ")
          ).map((_, 1)).reduceByKey(_ + _)
          .saveAsTextFiles("/home/james/data/spark/TripByYear/result")

        ssc.start()
        ssc.awaitTermination()
    }

    def normalizeYear(s: String): String = {
        try {
            (Calendar.getInstance().get(Calendar.YEAR) - s.toInt).toString
        } catch {
            case e: Exception => {
                println(e.getMessage)
                s
            }
        }
    }
}