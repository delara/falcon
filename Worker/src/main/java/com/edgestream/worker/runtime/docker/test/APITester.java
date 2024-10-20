package com.edgestream.worker.runtime.docker.test;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class APITester {

    public static void main(String[] args) throws IOException {

        String dockerContainerID = "1eae13bf1353";

        String URL = "http://10.70.2.224:2375/containers/" + dockerContainerID+"/json";
        JSONObject json = new JSONObject(IOUtils.toString(new URL(URL), StandardCharsets.UTF_8));

        System.out.println(json);

        JSONObject  networkSettings = new JSONObject(json.get("NetworkSettings").toString());
        JSONObject  Networks = new JSONObject(networkSettings.get("Networks").toString());
        JSONObject networkINFO = Networks.getJSONObject("edgestream");
        String ip = networkINFO.getString("IPAddress");

        System.out.println(Networks);
        System.out.println(networkINFO);
        System.out.println(ip);
        //String containerIP = Networks.get("IPAddress").toString();

        //System.out.println("Container IP: " +  containerIP);




    }
}
