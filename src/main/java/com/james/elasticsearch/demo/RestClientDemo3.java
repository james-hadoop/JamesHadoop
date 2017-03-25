package com.james.elasticsearch.demo;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

/**
 * Created by james on 17-3-25.
 */
public class RestClientDemo3 {
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

        int numRequests = 10;
        final CountDownLatch latch = new CountDownLatch(numRequests);

        HttpEntity entity = new NStringEntity(
                "{\n" +
                        "    \"user\" : \"kimchy\",\n" +
                        "    \"post_date\" : \"2009-11-15T14:12:12\",\n" +
                        "    \"message\" : \"trying out Elasticsearch\"\n" +
                        "}", ContentType.APPLICATION_JSON);

        HttpEntity[] entities = new HttpEntity[numRequests];

        for (int i = 0; i < entities.length; i++) {
            entities[i] = entity;
        }

        for (int i = 2; i < numRequests; i++) {
            restClient.performRequestAsync(
                    "PUT",
                    "/twitter/tweet/" + i,
                    Collections.<String, String>emptyMap(),
                    //assume that the documents are stored in an entities array
                    entities[i],
                    new ResponseListener() {
                        @Override
                        public void onSuccess(Response response) {
                            System.out.println(response);
                            latch.countDown();
                        }

                        @Override
                        public void onFailure(Exception exception) {
                            latch.countDown();
                        }
                    }
            );
        }

        //wait for all requests to be completed
        latch.await();

        restClient.close();
    }
}
