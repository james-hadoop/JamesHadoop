package com.james.spark.kafka

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

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

        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
        val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
        println(lines)
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1L))
          .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
        wordCounts.print()
        wordCounts.saveAsTextFiles("/home/james/data/spark/KafkaWordCount/spark-streaming");

        ssc.start()
        ssc.awaitTermination()
    }
}

// Produces some random words between 1 and 100.
object KafkaWordCountProducer {

    def main(args: Array[String]) {
        if (args.length < 4) {
            System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
              "<messagesPerSec> <wordsPerMessage>")
            System.exit(1)
        }

        val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

        // Zookeeper connection properties
        val props = new util.HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)

        // Send some messages
        while (true) {
            (1 to messagesPerSec.toInt).foreach { messageNum =>
                val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
                  .mkString(" ")

                val message = new ProducerRecord[String, String](topic, null, str)
                producer.send(message)
            }

            Thread.sleep(1000)
        }
    }
}
