package com.james.elasticsearch.demo;

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
import java.util.Map;

/**
 * Created by james on 17-3-14.
 */
public class TransportClientDemo2 {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello Elasticsearch...");

        Settings settings = Settings.builder()
                .put("cluster.name", "james-es").put("client.transport.sniff", true).build();
        TransportClient client;

        client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("JamesUbuntu"), 9300));

        SearchRequestBuilder srb1 = client
                .prepareSearch().setQuery(QueryBuilders.queryStringQuery("Doe")).setSize(10);
        SearchRequestBuilder srb2 = client
                .prepareSearch().setQuery(QueryBuilders.matchQuery("name","Doe")).setSize(10);
        SearchRequestBuilder srb3 = client
                .prepareSearch().setQuery(QueryBuilders.matchQuery("address", "mill")).setSize(10);

        MultiSearchResponse sr = client.prepareMultiSearch()
                //.add(srb1)
                .add(srb2)
                .get();

        // You will get all individual responses from MultiSearchResponse#getResponses()
        long nbHits = 0;

        MultiSearchResponse.Item[] items = sr.getResponses();
        System.out.println("items.length=" + items.length);

        for (MultiSearchResponse.Item item : items) {
            SearchResponse response = item.getResponse();
            nbHits = response.getHits().getTotalHits();
            System.out.println("nbHits=" + nbHits);

            SearchHits hits = response.getHits();
            Iterator<SearchHit> iterator = hits.iterator();
            while(iterator.hasNext()){
                System.out.println(iterator.next().getSourceAsString());
            }


            SearchHit[] searchHits = response.getHits().hits();
            System.out.println("searchHits.length:" + searchHits.length);

            for (SearchHit hit : searchHits) {
                Map<String, Object> source = hit.getSource();

                System.out.println("source.size()=" + source.size());

                for (Map.Entry<String, Object> entry : source.entrySet()) {
                    System.out.println(entry.getKey() + " -> " + entry.getValue());
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
