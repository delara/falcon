package com.edgestream.worker.runtime.docker;

public class DockerHostID {


    private final String dockerHostID;


    public DockerHostID(String dockerHostID) {
        this.dockerHostID = dockerHostID;
    }

    public String getDockerHostIDAsString(){
        return dockerHostID;
    }


}
