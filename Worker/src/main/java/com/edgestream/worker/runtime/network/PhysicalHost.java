package com.edgestream.worker.runtime.network;

import com.edgestream.worker.runtime.docker.DockerHost;

public class PhysicalHost {

    DockerHost dockerHost;
    String hostName;
    String hostIP;

    public PhysicalHost(String hostName, String hostIP) {

        this.hostName = hostName;
        this.hostIP = hostIP;

    }


    public void setDockerHost(DockerHost dockerHost){

        this.dockerHost = dockerHost;
    }

    public DockerHost getDockerHost(){

        return this.dockerHost;

    }

    public String getHostName() {
        return hostName;
    }


}
