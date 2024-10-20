package com.edgestream.worker.runtime.container;

import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.runtime.docker.DockerHost;
import com.edgestream.worker.runtime.task.model.TaskWarmerStrategy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class EdgeStreamContainer {

    private final DockerHost boundDockerHost;
    private final EdgeStreamContainerID edgeStreamContainerID;
    private String DockerContainerLocalIP;
    private String DockerOverlayNetwork;
    private final String operatorInputType;
    private final OperatorID operatorID;
    private final HashMap <String, String> outputTypeToPortMapping = new HashMap<>();
    private int outputPortRange = 8000;
    private TaskWarmerStrategy selectedWarmerStrategy;

    public EdgeStreamContainer(DockerHost boundDockerHost, EdgeStreamContainerID edgeStreamContainerID, String operatorInputType, OperatorID operatorID) {
        this.boundDockerHost = boundDockerHost;
        this.edgeStreamContainerID = edgeStreamContainerID;
        this.operatorInputType = operatorInputType;
        this.operatorID = operatorID;

    }


    public void addOutputTypeToPortMapping(String operatorOutputType){

        //if there aren't more than 1 successors then don't add any elements to the hashmap because they will use the default port of 8000 to connect to this operator container

            outputTypeToPortMapping.put(operatorOutputType, String.valueOf(this.outputPortRange));
            this.outputPortRange++;

    }

    public String getOutputPortNumberByOperatorType(String outputType){

        return outputTypeToPortMapping.get(outputType);

    }

    public String getOperatorInputType() {
        return operatorInputType;
    }

    public String getOperator_id() {
        return operatorID.getOperatorID_as_String();
    }


    public void setDockerContainerLocalIP(String dockerContainerLocalIP) {
        DockerContainerLocalIP = dockerContainerLocalIP;
    }

    public String getDockerContainerLocalIP() {
        return DockerContainerLocalIP;
    }


    public void setDockerOverlayNetwork(String dockerOverlayNetwork) {
        DockerOverlayNetwork = dockerOverlayNetwork;
    }


    public DockerHost getBoundDockerHost() {
        return boundDockerHost;
    }

    public EdgeStreamContainerID getEdgeStreamContainerID() {
        return edgeStreamContainerID;
    }

    public String getOutputTypeAndPortAsPipedList() {

        StringBuilder operatorOutPutTypeAndPortPipedList = new StringBuilder();
        //TODO: convert HashMap <String, String> outputTypeToPortMapping; to a concatenated string ex: "S,8000|T,8001"
        Set<Map.Entry<String, String>> entries = this.outputTypeToPortMapping.entrySet();

        for(Map.Entry<String, String> entry :entries){

            operatorOutPutTypeAndPortPipedList.append(entry.getKey() + "," +  entry.getValue() + "|");
        }


        return operatorOutPutTypeAndPortPipedList.toString();
    }

    public TaskWarmerStrategy getSelectedWarmerStrategy() {
        return selectedWarmerStrategy;
    }

    public void setWarmUpStrategy(TaskWarmerStrategy warmerStrategy) {
        selectedWarmerStrategy = warmerStrategy;
    }


}
