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
        val ssc = new StreamingContext(sparkConf, Seconds(20))
        ssc.checkpoint("checkpoint")

        val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092", "group.id" -> "test0")

        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("test0"))

        // Get the lines, split them into words, count the words and print
        val lines = messages.map(_._2)
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1L))
          .reduceByKeyAndWindow(_ + _, _ - _, Seconds(40), Seconds(40), 2)

        wordCounts.saveAsTextFiles("/home/james/data/spark/KafkaWordCount/wordCounts")

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
