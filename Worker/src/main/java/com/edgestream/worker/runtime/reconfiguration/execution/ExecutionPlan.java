package com.edgestream.worker.runtime.reconfiguration.execution;

import com.edgestream.worker.common.OperatorIDFormatter;
import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.runtime.plan.Host;
import com.edgestream.worker.runtime.plan.OperatorInPPO;
import com.edgestream.worker.runtime.docker.DockerFileType;
import com.edgestream.worker.runtime.reconfiguration.ReconfigurationPlan;
import com.edgestream.worker.runtime.reconfiguration.common.DagElementType;
import com.edgestream.worker.runtime.reconfiguration.common.DagPlan;
import org.apache.commons.lang3.tuple.ImmutablePair;
import java.util.ArrayList;
import java.util.List;

public abstract class ExecutionPlan {

    private ArrayList<ImmutablePair> dag;
    private DagPlan dagPlan;
    private DockerFileType dockerFileType;
    private ReconfigurationPlan reconfigurationPlan;
    private ExecutionType executionType;



    public abstract void createReconfigurationPlan(ExecutionPlanConfig executionPlanConfig);


    public ReconfigurationPlan getReconfigurationPlan() {
        return reconfigurationPlan;
    }

    public void setExecutionType(ExecutionType executionType) {
        this.executionType = executionType;
    }

    public ExecutionType getExecutionType() {
        return executionType;
    }

    public void setReconfigurationPlan(ReconfigurationPlan reconfigurationPlan) {
        this.reconfigurationPlan = reconfigurationPlan;
    }

    public ArrayList<ImmutablePair> getDag() {
        return this.dag;
    }


    void setDag(ArrayList<ImmutablePair> dag) {
        this.dag = dag;
    }


    public DagPlan getDagPlan() {
        return dagPlan;
    }


    void setDagPlan(DagPlan dagPlan) {
        this.dagPlan = dagPlan;
    }

    public DockerFileType getDockerFileType() {
        return dockerFileType;
    }

    public void setDockerFileType(DockerFileType dockerFileType) {
        this.dockerFileType = dockerFileType;
    }


    ArrayList<String> getDestinationTiers(List<Host> destinationDataCenterTaskManagers){

        ArrayList<String> destinationTiers = new ArrayList<>();
        for(Host destinationHost: destinationDataCenterTaskManagers){

            destinationTiers.add(destinationHost.getName());
        }


        return destinationTiers;
    }


    ArrayList<String> getPredecessors(String operatorInputType, ArrayList<ImmutablePair> dag){

        ArrayList<String> predecessors = new ArrayList<>();

        for(ImmutablePair <String,String> dagElement : dag){

            if(dagElement.right.equalsIgnoreCase(operatorInputType)){ // check to see if it is a successor of some operatorID

                predecessors.add(dagElement.left); //if so, get the predecessor
            }

        }

        return predecessors;

    }


    ArrayList<String> getSuccessors(String operatorInputType, ArrayList<ImmutablePair> dag){

        ArrayList<String> successors = new ArrayList<>();

        for(ImmutablePair <String,String> dagElement : dag){

            if(dagElement.left.equalsIgnoreCase(operatorInputType)){ // check to see if it is a predecessor of some operatorID

                successors.add(dagElement.right); //if so, get the successor
            }

        }

        return successors;

    }


    OperatorInPPO createOperator(String inputType, String filePath, ArrayList<String> destinationTiers, ArrayList<String> Predecessor_IDs, String replicaID, ArrayList<String> Successor_IDs, DagElementType dagElementType){

        //String Operator_ID = "OP_" + inputType +"_00" + replicaID;
        String Operator_ID = OperatorIDFormatter.formatOperatorID(inputType,replicaID);
        String Operator_Type = inputType;
        String Input_Type = inputType;
        String Filepath = filePath ;
        String Mode = "container";
        String Routing_Type = "ANYCAST";


        OperatorInPPO operatorInPPO = new OperatorInPPO(Operator_ID,Operator_Type,Input_Type,Filepath,Mode,destinationTiers,Routing_Type,Predecessor_IDs,Successor_IDs,dagElementType);
        OperatorID  operatorID = new OperatorID(Operator_ID);
        this.dagPlan.addOperatorID(operatorID);

        return operatorInPPO;
    }


    OperatorInPPO removeOperator(String inputType, String filePath, ArrayList<String> destinationTiers, ArrayList<String> Predecessor_IDs, String replicaID, ArrayList<String> Successor_IDs, DagElementType dagElementType){

        //String Operator_ID = "OP_" + inputType +"_00" + replicaID;
        String Operator_ID = OperatorIDFormatter.formatOperatorID(inputType,replicaID);
        String Operator_Type = inputType;
        String Input_Type = inputType;
        String Filepath = filePath ;
        String Mode = "remove";
        String Routing_Type = "ANYCAST";


        OperatorInPPO operatorInPPO = new OperatorInPPO(Operator_ID,Operator_Type,Input_Type,Filepath,Mode,destinationTiers,Routing_Type,Predecessor_IDs,Successor_IDs,dagElementType);

        return operatorInPPO;
    }


}
