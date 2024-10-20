package com.edgestream.worker.runtime.container;

public class EdgeStreamContainerID {


    private final String edgeStreamContainerID;
    private String dockerContainerID;
    private String dockerContainerName;


    public EdgeStreamContainerID(String edgeStreamContainerID) {

        this.edgeStreamContainerID = edgeStreamContainerID;

    }



    public String getEdgeStreamContainerID() {
        return edgeStreamContainerID;
    }

    public String getDockerContainerName() {
        return dockerContainerName;
    }


    public String getDockerContainerID(){
        return this.dockerContainerID;

    }

    public String getEdgeStreamContainerIDAsString(){
        return edgeStreamContainerID;
    }

    public void setDockerContainerID(String dockerContainerID) {
        this.dockerContainerID = dockerContainerID;
    }

    public void setDockerContainerName(String dockerContainerName) {
        this.dockerContainerName = dockerContainerName;
    }




}
