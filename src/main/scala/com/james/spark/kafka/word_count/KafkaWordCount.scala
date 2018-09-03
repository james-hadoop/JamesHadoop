package com.james.spark.kafka.word_count

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
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

        val Array(zkQuorum, group, dd, numThreads) = args
        val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local")
        val ssc = new StreamingContext(sparkConf, Seconds(20))

        ssc.checkpoint("checkpoint")

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "james_group",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array("test0")

        val messages = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

        // Get the lines, split them into words, count the words and print

        val lines = messages.map(_.value())
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1L))
            .reduceByKeyAndWindow(_ + _, _ - _, Seconds(40), Seconds(40), 2)

        wordCounts.saveAsTextFiles("/Users/qjiang/data/spark/KafkaWordCount/wordCounts")

        ssc.start()
        ssc.awaitTermination()
    }
}
