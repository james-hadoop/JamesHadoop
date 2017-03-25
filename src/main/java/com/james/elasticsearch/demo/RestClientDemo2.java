package com.james.elasticsearch.demo;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.Collections;

/**
 * Created by james on 17-3-25.
 */
public class RestClientDemo2 {
    public static void main(String[] args) throws Exception {


        RestClient restClient = RestClient.builder(new HttpHost("JamesUbuntu", 9200))
                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                        return requestConfigBuilder.setConnectTimeout(5000)
                                .setSocketTimeout(60000);
                    }
                })
                .setMaxRetryTimeoutMillis(60000)
                .build();

        Response response = restClient.performRequest("GET", "/customer/external/11",
                Collections.singletonMap("pretty", "true"));
        System.out.println(EntityUtils.toString(response.getEntity()));

        restClient.close();
    }
}
