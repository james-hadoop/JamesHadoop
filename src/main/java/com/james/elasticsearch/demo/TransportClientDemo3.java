package com.james.elasticsearch.demo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * Created by james on 17-3-14.
 */
public class TransportClientDemo3 {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello Elasticsearch...");

        Settings settings = Settings.builder()
                .put("cluster.name", "james-es").put("client.transport.sniff", true).build();
        TransportClient client;

        client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("JamesUbuntu"), 9300));

        SearchResponse sr = client.prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(
                        AggregationBuilders.terms("agg1").field("field")
                )
                .addAggregation(
                        AggregationBuilders.dateHistogram("agg2")
                                .field("birth")
                                .dateHistogramInterval(DateHistogramInterval.YEAR)
                )
                .get();

        // Get your facet results
//        Terms agg1 = sr.getAggregations().get("agg1");
//        DateHistogram agg2 = sr.getAggregations().get("agg2");


        if (null != client) {
            client.close();
        }

        System.out.println("Bye Elasticsearch...");
    }
}
