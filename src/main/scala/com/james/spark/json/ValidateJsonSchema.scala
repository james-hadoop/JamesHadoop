package com.james.spark.json

import com.james.util.LogUtil
import org.apache.spark.{SparkConf, SparkContext}

object ValidateJsonSchema {
    def main(args: Array[String]): Unit = {
        if (args.length != 1) {
            println("Error: invalid number of arguments")
            return
        }

        //val fileName = args(0)
        //println("filename: " + fileName)

        println("ValidateJsonSchema begin...")


        val conf = new SparkConf().setAppName("ValidateJsonSchema").setMaster("local")
        val sc = new SparkContext(conf)

        val fileName="/Users/qjiang/Desktop/ValidateRawLog/log_hour=18/logshed_denali_usage_logs_2018-04-23-18-00-GMT_ec2-logshedcollectorexternal-11-90000"
        //val fileName = "/Users/qjiang/Desktop/ValidateRawLog/log_hour=18/logshed_denali_usage_logs_2018-04-23-18-10-GMT_ec1-logshedcollector-10-40000"
        // val fileName = "/Users/qjiang/Desktop/ValidateRawLog/raw_log/20180501/log_hour=00"

        val line = sc.textFile(fileName)
        //line.foreach(println)

        //    line.map(x => (x, com.james.util.LogUtil.validateLine(x))).filter(_._2.isSuccess == true).map(y => y._1).saveAsTextFile("log_true_dir")
        //    line.map(x => (x, com.james.util.LogUtil.validateLine(x))).filter(_._2.isSuccess == false).map(y => y._1).saveAsTextFile("log_false_dir")
        line.map(x => (x, com.james.util.LogUtil.validateLine(x))).filter(_._2.isSuccess == false).map(y => (y._1, y._2.getErrorMessage)).saveAsTextFile("log_false_dir_2")

        //    Thread.sleep(1000 * 60)
        sc.stop()

        println("ValidateJsonSchema end...")
    }

    def validateString(text: String): Boolean = {
        return com.james.util.LogUtil.validateString(com.james.util.LogUtil.parsePayload(text))
        //    return true
    }
}
