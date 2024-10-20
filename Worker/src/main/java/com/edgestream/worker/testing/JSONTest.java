package com.edgestream.worker.testing;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;

public class JSONTest {

    public static void main(String[] args) throws IOException {
        String URL = "c:\\dataset\\6.json";
        JSONObject json = null;


        File file = new File(URL);
        String content = FileUtils.readFileToString(file, "utf-8");


        json = new JSONObject(content);

        System.out.println(json);

    }
}
