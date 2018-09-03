package com.james.spark.elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._


object EsWriteDemo2 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("EsWriteDemo2").setMaster("local")
        conf.set("es.index.auto.create", "true")
        conf.set("spark.driver.allowMultipleContexts", "true")

        val json1 = """{"reason" : "business", "airport" : "SFO"}"""
        val json2 = """{"participants" : 5, "airport" : "OTP"}"""

        new SparkContext(conf).makeRDD(Seq(json1, json2))
            .saveJsonToEs("spark/json-trips")
    }
}
