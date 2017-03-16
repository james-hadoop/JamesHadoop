package com.james.elasticsearch.demo;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Created by james on 17-3-14.
 */
public class EsDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello Elasticsearch...");

        Settings settings = Settings.builder()
                .put("cluster.name", "james-es").build();
        TransportClient client = null;

        client = new PreBuiltTransportClient(settings);

        GetResponse response = client.prepareGet("twitter", "tweet", "1").get();

        if (null != client) {
            client.close();
        }
    }
}
