package com.james.scala.json

import com.james.udf.validateJsonSchema.RawlogFixer
import org.codehaus.jettison.json.JSONException
import org.json.JSONObject

object JsonParseDemo {
    def main(args: Array[String]): Unit = {
        var jsonString = "{\"payload\":{\"caused_by\":\"responseCode:200\",\"log_context\":{\"log_id\":\"1bba421c-d629-407e-b66b-491baf0f1570\",\"current_lat\":35.099795,\"app_version\":\"2.0.508283-P\",\"utc_timestamp\":1525305595300,\"visitor_id\":\"e484cdf6-d387-405d-92e6-1d7afaeb0e1d\",\"time_zone\":\"America/Anguilla\",\"log_version\":\"v2\",\"reg_vid\":\"OptOut\",\"current_lon\":-77.096395},\"event_name\":\"CLOUD_REQUEST\",\"response_time\":566,\"trigger\":\"SEARCH\",\"schema_definition\":\"CloudRequest\"},\"logshed_app_id\":\"denali_usage_logs\",\"client_address\":\"10.191.1.214, 54.175.4.204, 10.191.102.61,10.191.102.45\",\"type\":1,\"slogtime\":1525305599816}"
        jsonString="{\"payload\":{\"parent_search_id\":\"\",\"distance\":7.3073229600000005,\"iid\":\"\",\"caused_by\":\"\",\"route_position\":1,\"source\":\"ONBOARD\",\"trigger\":\"ETA_CALC\",\"share_eta\":\"OFF\",\"entity_id\":\"\",\"type\":\"FASTEST\",\"schema_definition\":\"Route\",\"number_of_incidents\":-1,\"search_id\":\"\",\"eta\":11,\"dest_type\":\"LABELLED_DEST\",\"log_context\":{\"log_id\":\"6f91046d-022f-4a92-91fe-6de9bbc23044\",\"current_lat\":41.031815,\"app_version\":\"2.0.508283-P\",\"utc_timestamp\":1525305540053,\"visitor_id\":\"a18d0fe1-1d7b-47d2-b9be-1853db0e7bad\",\"time_zone\":\"America/Anguilla\",\"log_version\":\"v2\",\"reg_vid\":\"OptOut\",\"current_lon\":-73.758865},\"event_name\":\"ROUTE\",\"response_time\":316437,\"parent_route_id\":\"\"},\"logshed_app_id\":\"denali_usage_logs\",\"client_address\":\"10.191.1.214, 54.208.95.0, 10.191.102.238,10.191.102.45\",\"type\":1,\"slogtime\":1525305595384}"

        val logs = new JSONObject(jsonString)
        //val payload = logs.get("payload").asInstanceOf[JSONObject]
        //val eventNameString = payload.getString("event_name")
        //println(eventNameString)
        //println

        println(parseEventName(jsonString))
        println(fixRawlog(jsonString))
    }

    def parseEventName(jsonString: String): String = {
        if (null == jsonString || jsonString.isEmpty) {
            return null
        }

        val logs = new JSONObject(jsonString)

        val payload: JSONObject = try {
            logs.get("payload").asInstanceOf[JSONObject]
        } catch {
            case ex: JSONException => null
        }


        val eventName: String = try {
            payload.getString("event_name")
        } catch {
            case ex: JSONException => "a": String
        }

        return eventName
    }

    def fixRawlog(jsonString: String): String = {
        if (null == jsonString || jsonString.isEmpty) {
            return null
        }

        parseEventName(jsonString) match {
            case "CLOUD_REQUEST" => RawlogFixer.fixRawlog(jsonString)
            case "START_ENGINE" => RawlogFixer.fixRawlog(jsonString)
            case "ROUTE" => RawlogFixer.fixRawlogForRoute(jsonString)
        }
    }
}
