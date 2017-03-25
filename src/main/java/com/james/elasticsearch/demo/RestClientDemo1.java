package com.james.elasticsearch.demo;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.util.Collections;

/**
 * Created by james on 17-3-25.
 */
public class RestClientDemo1 {
    public static void main(String[] args) throws Exception {

        RestClient restClient = RestClient.builder(
                new HttpHost("JamesUbuntu", 9200, "http"),
                new HttpHost("JamesUbuntu", 9201, "http")).build();

        Response response = restClient.performRequest("GET", "/",
                Collections.singletonMap("pretty", "true"));
        System.out.println(EntityUtils.toString(response.getEntity()));

//        //index a document
//        HttpEntity entity = new NStringEntity(
//                "{\n" +
//                        "    \"user\" : \"kimchy\",\n" +
//                        "    \"post_date\" : \"2009-11-15T14:12:12\",\n" +
//                        "    \"message\" : \"trying out Elasticsearch\"\n" +
//                        "}", ContentType.APPLICATION_JSON);
//
//        Response indexResponse = restClient.performRequest(
//                "PUT",
//                "/twitter/tweet/1",
//                Collections.<String, String>emptyMap(),
//                entity);

//        //index a document
//        HttpEntity entity = new NStringEntity(
//                "{\n" +
//                        "\"name\":\"james\"\n" +
//                        "}", ContentType.APPLICATION_JSON);
//
//        Response indexResponse = restClient.performRequest(
//                "PUT",
//                "/customer/external/11",
//                Collections.<String, String>emptyMap(),
//                entity);
//
//        restClient.close();
    }
}
