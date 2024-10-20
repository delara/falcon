package com.edgestream.worker.common;

public class DockerHostIDFormatter {

    public static String formatDockerHostID(String dockerHostIP){

        return "docker.host."+ dockerHostIP;

    }
}
