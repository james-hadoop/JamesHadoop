package com.james.elasticsearch.demo;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by james on 17-3-25.
 */
public class RestClientDemo4 {
    public static void main(String[] args) throws IOException {
        RestClient restClient = RestClient.builder(new HttpHost("JamesUbuntu", 9200)).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setConnectTimeout(5000).setSocketTimeout(60000);
            }
        }).setMaxRetryTimeoutMillis(60000).build();

        Response response = restClient.performRequest("GET", "/bank/_search", Collections.singletonMap("pretty", "true"));
        // System.out.println(EntityUtils.toString(response.getEntity()));

        // parse response
        InputStream in = response.getEntity().getContent();
        String result = IOUtils.toString(in, "UTF-8");
        System.out.println("result: " + result);

        JSONObject jSONObject = JSONObject.fromObject(result);
        System.out.println("jSONObject: " + jSONObject);

        Object hits = jSONObject.getString("hits");
        System.out.println("hits: " + hits);

        Object hitsChildren = JSONObject.fromObject(hits).get("hits");
        System.out.println("hitsChildren: " + hitsChildren);

        JSONArray jsonArray = JSONArray.fromObject(hitsChildren);
        System.out.println("jsonArray: " + jsonArray);
        System.out.println("\n\n--------for--------\n\n");

        List<BankEntity> entities = new ArrayList<BankEntity>();

        for (int i = 0; i < jsonArray.size(); i++) {
            System.out.println(jsonArray.getString(i));

            JSONObject source = JSONObject.fromObject(JSONObject.fromObject(jsonArray.getString(i)).getString("_source"));
            System.out.println("source: " + source);

            BankEntity bankEntity = new BankEntity();
            bankEntity.setAccount_number(source.getInt("account_number"));
            bankEntity.setBalance(source.getInt("balance"));
            bankEntity.setFirstname(source.getString("firstname"));
            bankEntity.setLastname(source.getString("lastname"));
            bankEntity.setAge(source.getInt("age"));
            bankEntity.setGender(source.getString("gender"));
            bankEntity.setAddress(source.getString("address"));
            bankEntity.setEmployer(source.getString("employer"));
            bankEntity.setCity(source.getString("city"));
            bankEntity.setState(source.getString("state"));
            entities.add(bankEntity);
        }

        restClient.close();

        System.out.println("\n\n--------entities--------\n\n");
        for (BankEntity entity : entities) {
            System.out.println("entity: " + entity);
        }
    }
}
