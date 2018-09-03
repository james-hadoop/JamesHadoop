package com.james.spark.kafka.tndpp

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object KafkaTndppConsumer {
    def main(args: Array[String]) {
        //        if (args.length < 4) {
        //            System.err.println("Usage: KafkaWordCountConsumer <metadataBrokerList> <topic> " +
        //                "<messagesPerSec> <wordsPerMessage>")
        //            System.exit(1)
        //        }

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "james_group_4",
            "enable.auto.commit" -> (true: java.lang.Boolean),
            "auto.offset.reset" -> "earliest"
        )

        val topics = Array("tndpp")

        val conf = new SparkConf()
            .setAppName("KafkaWordCountConsumer")
            .setJars(SparkContext.jarOfClass(this.getClass).toSeq).setMaster("local")

        val streamingContext = new StreamingContext(conf, Seconds(2))

        streamingContext.checkpoint("/Users/qjiang/data/kafka/checkpoint/KafkaWordCountConsumer4")

        val stream = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
        KafkaUtils
        // stream.map(record => record.value).print()
        stream.map(record => record.value).print()

        streamingContext.start()

        streamingContext.awaitTermination()
    }
}
