package com.edgestream.worker.testing;


import com.edgestream.worker.runtime.docker.DockerAPI;
import org.json.JSONObject;

import java.io.IOException;

public class DockerOrchestrationTesting {

    public static void main(String[] args) throws IOException {

        DockerAPI dockerAPI = new DockerAPI("192.168.86.34");


        String ip = dockerAPI.getContainerIP("fedb484756d008b6cfe9d858d4687b08565bdfbc07f2868171abae4e6885438e");
        System.out.println(ip);

    }
}
