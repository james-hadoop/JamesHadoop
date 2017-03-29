package com.james.elasticsearch.demo;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Iterator;

//import org.apache.htrace.fasterxml.jackson.databind.DeserializationFeature;
//import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

public class TransportClientDemo4 {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello Elasticsearch...");

        Settings settings = Settings.builder().put("cluster.name", "data-es").put("client.transport.sniff", true).build();
        TransportClient client;

        client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("JamesUbuntu"), 9300));

        SearchRequestBuilder srb1 = client.prepareSearch().setQuery(QueryBuilders.queryStringQuery("")).setSize(10);
        SearchRequestBuilder srb2 = client.prepareSearch().setQuery(QueryBuilders.matchQuery("name", "Doe")).setSize(10);
        SearchRequestBuilder srb3 = client.prepareSearch().setQuery(QueryBuilders.matchQuery("address", "mill")).setSize(10);
        SearchRequestBuilder srb4 = client.prepareSearch().setQuery(QueryBuilders.matchAllQuery()).setSize(10);

        MultiSearchResponse sr = client.prepareMultiSearch().add(srb4)
                // .add(srb2)
                .get();

        // You will get all individual responses from
        // MultiSearchResponse#getResponses()
        long nbHits = 0;

        MultiSearchResponse.Item[] items = sr.getResponses();
        System.out.println("items.length=" + items.length);

        for (MultiSearchResponse.Item item : items) {
            SearchResponse response = item.getResponse();
            nbHits = response.getHits().getTotalHits();
            System.out.println("nbHits=" + nbHits);

            SearchHits hits = response.getHits();
            Iterator<SearchHit> iterator = hits.iterator();
            while (iterator.hasNext()) {
                String record = iterator.next().getSourceAsString();
                System.out.println("record: " + record);

                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

                try {
                    BankEntity entity = mapper.readValue(record, BankEntity.class);
                    System.out.println(entity);
                    System.out.println();
                } catch (Exception e) {
                    System.out.println("mapper.readValue() exception caught...");
                    throw e;
                }
            }

            System.out.println();
        }

        if (null != client) {
            client.close();
        }

        System.out.println("Bye Elasticsearch...");
    }
}
