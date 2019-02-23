package com.james.spark.kafka.tndpp

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Created by james on 18-8-31.
  */
object KafkaTndppProducer {

    def main(args: Array[String]) {
        var count0 = 0

        if (args.length < 2) {
            System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic>")
            System.exit(1)
        }

        val Array(brokers, topic) = args

        // Zookeeper connection properties
        val props = new util.HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)

        // Send some messages
        for (delta <- 1 to 10) {
            val count = count0 + delta
            val tndppMsg = new ProducerRecord[String, String](topic, null, count + "____" + "{\"payload\":{\"travelled_edge_id_list\":[{\"actual_speed\":0,\"distance\":64,\"traffic_speed\":11.020627975463867,\"edge_id\":\"753543935\",\"assumed_speed\":11.020627975463867,\"mercator_coord_y\":49658316,\"mercator_coord_x\":36149874}],\"log_context\":{\"log_id\":\"754dc23d-e208-40bc-8523-34f9a67a2afd\",\"current_lat\":42.329645,\"app_version\":\"1.0.12304\",\"utc_timestamp\":1533004810564,\"visitor_id\":\"15434caf-1c29-4644-97ba-a07c1dab0a07\",\"time_zone\":\"Asia/Shanghai\",\"car_id\":\"30:a9:de:c6:97:7e\",\"log_version\":\"v2\",\"reg_vid\":\"H2V3PXURLT53CR7L7I84XAI8\",\"current_lon\":-83.039008},\"event_name\":\"NAV_EDGES\",\"trigger\":\"FOLLOW_ME\",\"start_engine_id\":\"e7c89150-7fb2-46b6-b5a8-f4149d673bcc\",\"schema_definition\":\"NavEdges\"},\"logshed_app_id\":\"denali_usage_logs\",\"client_address\":\"12.33.222.242, 10.189.1.201,10.190.2.88\",\"type\":1,\"slogtime\":1533092269763}")

            producer.send(tndppMsg)

            println(s"tndppMsg count: $count")
            Thread.sleep(1000)
        }
    }
}
