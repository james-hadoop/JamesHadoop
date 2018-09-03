package com.james.spark.elasticsearch

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.streaming._

import scala.collection.mutable

object EsJsonWriteDemo1 {

    def main(args: Array[String]): Unit = {
        val json1 = "{\"payload\":{\"travelled_edge_id_list\":[{\"actual_speed\":0,\"distance\":64,\"traffic_speed\":11.020627975463867,\"edge_id\":\"753543935\",\"assumed_speed\":11.020627975463867,\"mercator_coord_y\":49658316,\"mercator_coord_x\":36149874}],\"log_context\":{\"log_id\":\"11\",\"current_lat\":42.329645,\"app_version\":\"1.0.12304\",\"utc_timestamp\":1533004810564,\"visitor_id\":\"15434caf-1c29-4644-97ba-a07c1dab0a07\",\"time_zone\":\"Asia/Shanghai\",\"car_id\":\"30:a9:de:c6:97:7e\",\"log_version\":\"v2\",\"reg_vid\":\"H2V3PXURLT53CR7L7I84XAI8\",\"current_lon\":-83.039008},\"event_name\":\"NAV_EDGES\",\"trigger\":\"FOLLOW_ME\",\"start_engine_id\":\"e7c89150-7fb2-46b6-b5a8-f4149d673bcc\",\"schema_definition\":\"NavEdges\"},\"logshed_app_id\":\"denali_usage_logs\",\"client_address\":\"12.33.222.242, 10.189.1.201,10.190.2.88\",\"type\":1,\"slogtime\":1533092269763}"
        val json2 = "{\"payload\":{\"travelled_edge_id_list\":[{\"actual_speed\":0,\"distance\":64,\"traffic_speed\":11.020627975463867,\"edge_id\":\"753543935\",\"assumed_speed\":11.020627975463867,\"mercator_coord_y\":49658316,\"mercator_coord_x\":36149874}],\"log_context\":{\"log_id\":\"12\",\"current_lat\":42.329645,\"app_version\":\"1.0.12304\",\"utc_timestamp\":1533004810564,\"visitor_id\":\"15434caf-1c29-4644-97ba-a07c1dab0a07\",\"time_zone\":\"Asia/Shanghai\",\"car_id\":\"30:a9:de:c6:97:7e\",\"log_version\":\"v2\",\"reg_vid\":\"H2V3PXURLT53CR7L7I84XAI8\",\"current_lon\":-83.039008},\"event_name\":\"NAV_EDGES\",\"trigger\":\"FOLLOW_ME\",\"start_engine_id\":\"e7c89150-7fb2-46b6-b5a8-f4149d673bcc\",\"schema_definition\":\"NavEdges\"},\"logshed_app_id\":\"denali_usage_logs\",\"client_address\":\"12.33.222.242, 10.189.1.201,10.190.2.88\",\"type\":1,\"slogtime\":1533092269763}"


        val conf = new SparkConf().setAppName("EsJsonWriteDemo1").setMaster("local")
        conf.set("es.index.auto.create", "true")

        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        val ssc = new StreamingContext(sc, Seconds(1))

        val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
        val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

        //        val rdd1 = sc.makeRDD(Seq(numbers, airports))
        //        val microbatch1 = mutable.Queue(rdd1)
        //        ssc.queueStream(microbatch1).saveToEs("spark-streaming/docs")

        val rdd = sc.makeRDD(Seq(json1, json2))
        val microbatch = mutable.Queue(rdd)

        ssc.queueStream(microbatch).saveJsonToEs("spark-json/docs")

        ssc.start()
        ssc.awaitTermination()
    }
}
