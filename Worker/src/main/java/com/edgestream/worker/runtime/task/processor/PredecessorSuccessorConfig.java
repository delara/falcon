package com.edgestream.worker.runtime.task.processor;

import com.edgestream.worker.runtime.container.EdgeStreamContainer;
import com.edgestream.worker.runtime.docker.DockerHost;
import com.edgestream.worker.runtime.docker.DockerHostManager;
import com.edgestream.worker.runtime.reconfiguration.execution.ExecutionType;
import com.edgestream.worker.runtime.task.model.TaskRequestSetupOperator;

import java.util.List;

public class PredecessorSuccessorConfig {

    private String predecessorPort;
    private String predecessorInputMethod;
    private String predecessorIP;
    private final List<String> predecessors;
    private final List<String> successors;
    private final String operatorType;
    private final String operatorID;
    private String successorIP;

    private final String colocatedOperators;
    private final DockerHostManager dockerHostManager;


    protected PredecessorSuccessorConfig(String predecessorPort, String predecessorInputMethod, String predecessorIP
            , String successorIP, TaskRequestSetupOperator taskRequestSetupOperator, DockerHostManager dockerHostManager) {
        this.predecessorPort = predecessorPort;
        this.predecessorInputMethod = predecessorInputMethod;
        this.predecessorIP = predecessorIP;
        this.successorIP = successorIP;
        this.predecessors = taskRequestSetupOperator.getOperatorPredecessors();
        this.operatorType = taskRequestSetupOperator.getOperatorType();
        this.operatorID = taskRequestSetupOperator.getOperatorID();
        this.successors = taskRequestSetupOperator.getOperatorSuccessors();
        this.colocatedOperators = taskRequestSetupOperator.getColocatedOperators();
        this.dockerHostManager = dockerHostManager;

        configurePredecessors();
        configureSuccessors();

    }

    protected String getSuccessorIP() {
        return successorIP;
    }

    protected List<String> getSuccessors() {
        return successors;
    }

    protected String getPredecessorPort() {
        return predecessorPort;
    }

    protected String getPredecessorInputMethod() {
        return predecessorInputMethod;
    }

    protected String getPredecessorIP() {
        return predecessorIP;
    }

    protected List<String> getPredecessors() {
        return predecessors;
    }

    protected boolean hasPredecessors(){
        return !getPredecessors().isEmpty();

    }

    private void configurePredecessors(){

        //TODO: This for loop is not yet setup to actually handle multiple predecessors.
        //TODO contd: Its just that the predecessor arrives a List<String> object but for now we expect that only 1 predecessor will be in that list
        predecessorInputMethod = "activemq";

        for(String predecessor : predecessors ) {
            if (containerOperatorTypeAlreadyExists(predecessor)) {
                predecessorInputMethod = "zeromq";
                EdgeStreamContainer edgeStreamContainer = getCandidatePredecessorSuccessorContainer(predecessor);
                predecessorIP = edgeStreamContainer.getDockerContainerLocalIP();
                predecessorPort = edgeStreamContainer.getOutputPortNumberByOperatorType(operatorType);
                System.out.println("Found a candidate predecessor: " + predecessorIP + " for new operator: " + operatorID);

            }
        }

        if(predecessorInputMethod.equalsIgnoreCase("activemq")) {
            System.out.println("No predecessor for operator: " + operatorID);
        }

    }


   private void configureSuccessors(){

       //TODO: This for loop is not yet setup to actually handle multiple successors.
       // Its just that the successors arrives a List<String> object but for now we expect that only 1 successors will be in that list.
       for (String successor: successors){
           if (containerOperatorTypeAlreadyExists(successor)) {

               //This is used when this operator is being created after the successors are already online.
               // TODO: If this operator is a fanout meaning it produces data for more than one successor,
               //  we need to get all the successor ips and instruct the container setup to make a connection to each one of them.
               //  Operators are always listening on port 7002 for late producers, so we dont't need to know about ports
               successorIP = getCandidatePredecessorSuccessorContainer(successor).getDockerContainerLocalIP();
               System.out.println("Found a candidate successor: " + successorIP + " for new operator: " + operatorID);
               break;
           }

           if (colocatedOperatorExists(successor,colocatedOperators)){ // check to see if the successor operator is part of the same deployment plan
               successorIP = "FUSED";
               System.out.println("Found a operator successor of TYPE: "  + successor + " in this deployment plan, the output is to be: " + successorIP + " for new operator: " + operatorID);
               System.out.println("Will set producer type to ZeroMQ and wait for successor to come online");
           }
           else{
               successorIP = "NONE";
               System.out.println("No successor for operator: " + operatorID + " will use ActiveMQ producer and write output to broker");
           }

       }

   }


    /**
     * This method will check to see if a container of the operatorType exists.
     * There could be many replicas of operatorType running in a given datacenter
     *
     * @param operatorType
     * @return
     */
    protected boolean containerOperatorTypeAlreadyExists(String operatorType){

        boolean found_container = false;
        if (!operatorType.equalsIgnoreCase("NONE")) {
            List<EdgeStreamContainer> containers = dockerHostManager.getActiveEdgeStreamContainers();
            for (EdgeStreamContainer c : containers) {
                if (c.getOperatorInputType().equalsIgnoreCase(operatorType)) {
                    found_container = true;
                }
            }
        }
        return found_container;
    }

    private EdgeStreamContainer getCandidatePredecessorSuccessorContainer(String operatorType){
        EdgeStreamContainer candidateContainer = dockerHostManager.getNextEdgeStreamContainerInSchedule(operatorType);
        return candidateContainer;
    }


    protected EdgeStreamContainer getCandidateReplicaContainer(String operatorType){
        EdgeStreamContainer candidateContainer = dockerHostManager.getEdgeStreamContainerReplica(operatorType);
        return candidateContainer;
    }



    private boolean colocatedOperatorExists(String successorOperatorInputType, String colocatedOperatorList){
        boolean foundColocatedOperator = false;
        for (int i=0; i < colocatedOperatorList.length(  ); i++) {
            if(successorOperatorInputType.equalsIgnoreCase(String.valueOf(colocatedOperatorList.charAt(i)))){
                foundColocatedOperator = true;
            }
        }
        return foundColocatedOperator;
    }



}
