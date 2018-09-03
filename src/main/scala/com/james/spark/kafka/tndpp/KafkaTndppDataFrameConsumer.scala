package com.james.spark.kafka.tndpp

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}


object KafkaTndppDataFrameConsumer {
    def main(args: Array[String]) {
        if (args.length != 2) {
            println("Error: invalid number of arguments")
            println("Usage: spark-submit <isLocal> <storageLevel>")
            return
        }

        val Array(isLocal, storageLevel) = args
        println(s"isLocal=$isLocal\nstorageLevel=$storageLevel")

        println("KafkaTndppDataFrameConsumer begin...")

        execKafkaTndppDataFrameConsumer(isLocal.toBoolean, storageLevel)

        println("KafkaTndppDataFrameConsumer end...")


    }

    def execKafkaTndppDataFrameConsumer(isLocal: Boolean, storageLevel: String): Unit = {
        val sparkSessionBuilder = SparkSession.builder().appName("KafkaTndppDataFrameConsumer")
        if (isLocal) {
            sparkSessionBuilder.master("local")
        }

        val spark = sparkSessionBuilder.getOrCreate()
        spark.conf.set("spark.sql.streaming.checkpointLocation", "KafkaTndppDataFrameConsumerCheckPoint1")
        spark.sparkContext.setLogLevel("WARN")


        val ds1 = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "tndpp1")
            .option("startingOffsets", "{\"tndpp1\":{\"0\":160}}")
            .option("group.id", "james_group_1")
            .load()

        import spark.implicits._

        val df = ds1.selectExpr("CAST (value AS STRING)")
            .as[(String)]

        df.createOrReplaceTempView("df");
        val records = spark.sql("SELECT value from df")

        val query = records.select("value").writeStream.format("text").start("KafkaTndppDataFrameConsumerPath1b")
        //val query = records.writeStream.format("console").start()
        //val query= records.writeStream.outputMode("complete").format("console").start()

        query.awaitTermination()
        spark.stop()
    }
}
