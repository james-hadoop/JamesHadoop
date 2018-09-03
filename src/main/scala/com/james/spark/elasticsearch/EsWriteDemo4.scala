package com.james.spark.elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._


object EsWriteDemo4 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("EsWriteDemo4").setMaster("local")
        conf.set("es.index.auto.create", "true")
        conf.set("spark.driver.allowMultipleContexts", "true")

        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
        val muc = Map("iata" -> "MUC", "name" -> "Munich")
        val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

        val airportsRDD = sc.makeRDD(Seq((1, otp), (2, muc), (3, sfo)))
        airportsRDD.saveToEsWithMeta("airports/2015")
    }
}
