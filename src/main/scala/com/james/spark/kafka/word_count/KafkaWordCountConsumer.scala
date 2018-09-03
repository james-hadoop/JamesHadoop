package com.james.spark.kafka.word_count

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object KafkaWordCountConsumer {
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
            "group.id" -> "james_group_1",
            "enable.auto.commit" -> (true: java.lang.Boolean),
            "auto.offset.reset" -> "earliest"
        )

        val topics = Array("test0")

        val conf = new SparkConf()
            .setAppName("KafkaWordCountConsumer")
            .setJars(SparkContext.jarOfClass(this.getClass).toSeq).setMaster("local")

        val streamingContext = new StreamingContext(conf, Seconds(2))

        streamingContext.checkpoint("/Users/qjiang/data/kafka/checkpoint/KafkaWordCountConsumer")

        val stream = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

        // stream.map(record => record.value).print()
        stream.map(record => record.key).print()

        streamingContext.start()

        streamingContext.awaitTermination()
    }
}
