package com.james.spark.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by james on 16-12-13.
  */
object KafkaWordCount {
    def main(args: Array[String]) {
        if (args.length < 4) {
            System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
            System.exit(1)
        }

        val Array(zkQuorum, group, topics, numThreads) = args
        val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local")
        val ssc = new StreamingContext(sparkConf, Seconds(2))
        ssc.checkpoint("checkpoint")

        val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092", "group.id" -> "test0")


        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

        val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("test0"))
        println(lines)
        lines.saveAsTextFiles("/home/james/data/spark/KafkaWordCount/spark-streaming")

        //        val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
        //        println(lines)
        //        val words = lines.flatMap(_.split(" "))
        //        val wordCounts = words.map(x => (x, 1L))
        //          .reduceByKeyAndWindow(_ + _, _ - _, Seconds(10), Seconds(2), 2)
        //        wordCounts.print()
        //        wordCounts.saveAsTextFiles("/home/james/data/spark/KafkaWordCount/spark-streaming");


        ssc.start()
        ssc.awaitTermination()
    }
}
