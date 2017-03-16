package com.james.elasticsearch.demo;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.Collections;

/**
 * Created by james on 17-3-14.
 */
public class EsDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello Elasticsearch...");

        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setConnectTimeout(5000).setSocketTimeout(30000);
            }
        }).setMaxRetryTimeoutMillis(30000).build();

//        String request = "{\n" + "\"query\": { \n"
//                + "\"match_all\":{} \n" + "}\n"+ "}";
//        System.out.println(request);

        HttpEntity entity = new NStringEntity("Content-Type", ContentType.APPLICATION_JSON);

        Response res = restClient.performRequest("GET", "/customer/external?pretty&pretty'", Collections.<String, String>emptyMap(), entity);

        restClient.close();
    }
}
