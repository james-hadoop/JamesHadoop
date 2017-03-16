package com.james.elasticsearch.demo;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * Created by james on 17-3-14.
 */
public class EsDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello Elasticsearch...");

        Settings settings = Settings.builder()
                .put("cluster.name", "james-es").put("client.transport.sniff", true).build();
        TransportClient client;

        client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("JamesUbuntu"), 9300));

        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("customer", "external", "1")
                .get();

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                System.out.println("json=" + json);
            }
        }

        if (null != client) {
            client.close();
        }
    }
}
