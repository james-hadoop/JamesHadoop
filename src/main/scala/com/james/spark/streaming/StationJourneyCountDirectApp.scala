package com.james.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object StationJourneyCountDirectApp {

    def main(args: Array[String]) {
        if (args.length != 7) {
            System.err.println(
                "Usage: StationJourneyCountApp <appname> <brokerUrl> <topic> <consumerGroupId> <zkQuorum> <checkpointDir> <outputPath>")
            System.exit(1)
        }

        val Seq(appName, brokerUrl, topic, consumerGroupId, zkQuorum, checkpointDir, outputPath) = args.toSeq

        val conf = new SparkConf()
            .setAppName(appName)
            .setJars(SparkContext.jarOfClass(this.getClass).toSeq)

        val ssc = new StreamingContext(conf, Seconds(10))
        ssc.checkpoint(checkpointDir)

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "james_group",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array("test0")

        KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)).map(_.value())
            .map(rec => rec.split(","))
            .map(rec => ((rec(3), rec(7)), 1))
            .reduceByKey(_ + _)
            .repartition(1)
            .map(rec => (rec._2, rec._1))
            .transform(rdd => rdd.sortByKey(ascending = false))
            .saveAsTextFiles(outputPath)

        ssc.start()
        ssc.awaitTermination()
    }

}
