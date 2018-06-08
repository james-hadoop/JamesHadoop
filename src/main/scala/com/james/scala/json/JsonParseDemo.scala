package com.james.scala.json

import org.json.JSONObject

object JsonParseDemo {
    def main(args: Array[String]): Unit = {
        var jsonString="{\"payload\":{\"caused_by\":\"responseCode:200\",\"log_context\":{\"log_id\":\"1bba421c-d629-407e-b66b-491baf0f1570\",\"current_lat\":35.099795,\"app_version\":\"2.0.508283-P\",\"utc_timestamp\":1525305595300,\"visitor_id\":\"e484cdf6-d387-405d-92e6-1d7afaeb0e1d\",\"time_zone\":\"America/Anguilla\",\"log_version\":\"v2\",\"reg_vid\":\"OptOut\",\"current_lon\":-77.096395},\"event_name\":\"CLOUD_REQUEST\",\"response_time\":566,\"trigger\":\"SEARCH\",\"schema_definition\":\"CloudRequest\"},\"logshed_app_id\":\"denali_usage_logs\",\"client_address\":\"10.191.1.214, 54.175.4.204, 10.191.102.61,10.191.102.45\",\"type\":1,\"slogtime\":1525305599816}"
        val logs = new JSONObject(jsonString)
        val payload = logs.get("payload").asInstanceOf[JSONObject]
        val eventNameString = payload.getString("event_name")

        println(eventNameString)
    }
}
