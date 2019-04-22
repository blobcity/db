package com.blobcity.db.adapter;

import com.blobcity.db.Db;
import com.blobcity.db.config.Credentials;
import com.google.gson.JsonObject;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello world");

        Credentials.init("localhost:10111","root", "b0e9990d4d", "test");

        List<JsonObject> list = new ArrayList<>();

        for(int i=0; i<1000 ; i++) {
            JsonObject json = new JsonObject();
            json.addProperty("col1", "" + i);
            list.add(json);
        }

        


        list.parallelStream().forEach(item -> {
            Db.insertJson("test", item);
            System.out.println("Inserting: " + item.get("col1").getAsString());
        });
    }
}
