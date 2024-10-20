package com.edgestream.worker.runtime.container;


import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.runtime.docker.DockerHost;

public class EdgeStreamOperatorContainer extends EdgeStreamContainer {


    public EdgeStreamOperatorContainer(DockerHost boundDockerHost, EdgeStreamContainerID edgeStreamContainerID, String operatorInputType, OperatorID operator_id) {
        super(boundDockerHost, edgeStreamContainerID,operatorInputType,operator_id);
    }
}
