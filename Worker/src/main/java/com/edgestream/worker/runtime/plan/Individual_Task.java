package com.edgestream.worker.runtime.plan;

import com.edgestream.worker.runtime.reconfiguration.common.DagElementType;
import com.edgestream.worker.runtime.reconfiguration.exception.EdgeStreamReconfigurationException;
import com.edgestream.worker.runtime.reconfiguration.state.StateTransferPlan;

import java.util.List;

public class Individual_Task {

    private final String Topology_ID;
    private final String Individual_Task_ID;
    private final String Task_Manager_ID;
    private final String Host_Name;
    private final String Operator_ID;
    private final String Operator_Type;
    private final String Operator_Input_Type;
    private final String Operator_Path;
    private final String Operator_Mode;
    private final String Host_Address;
    private final String Host_Port;
    private final String Message_Routing_Type;
    private final List<String> Operator_Predecessor_IDs;
    private final List<String> Operator_Successor_IDs;
    private String deploymentMethod; // from the json, we determine if the operator will run external or internal at the destination task manager
    private String prebuiltOperatorJarFileName;
    private final DagElementType dagElementType;
    private StateTransferPlan stateTransferPlan;
    private boolean hasBootStrapState = false;

    // generated timestamp
    private final long Job_Submission_Time;


    public Individual_Task(String topology_ID, String individual_Task_ID, String task_Manager_ID, String host_Name,
                           String operator_ID, String operator_Type, String operator_Input_Type, String operator_Path,
                           String operator_Mode, String host_Address, String host_Port, String message_Routing_Type,
                           List<String> operator_Predecessor_IDs, List<String> operator_Successor_IDs,
                           DagElementType dagElementType, long job_Submission_Time) {
        Topology_ID =  topology_ID;
        Individual_Task_ID = individual_Task_ID;
        Task_Manager_ID = task_Manager_ID;
        Host_Name = host_Name;
        Operator_ID = operator_ID;
        Operator_Type = operator_Type;
        Operator_Input_Type = operator_Input_Type;
        Operator_Path = operator_Path;
        Operator_Mode = operator_Mode;
        Host_Address = host_Address;
        Host_Port = host_Port;
        Message_Routing_Type = message_Routing_Type;
        Operator_Predecessor_IDs = operator_Predecessor_IDs;
        Operator_Successor_IDs = operator_Successor_IDs;
        this.dagElementType = dagElementType;
        Job_Submission_Time = job_Submission_Time;
    }


    public DagElementType getDagElementType() {
        return dagElementType;
    }

    public StateTransferPlan getStateTransferPlan() {
        return stateTransferPlan;
    }

    public void addStateTransferPlan(StateTransferPlan stateTransferPlan){

        if (this.dagElementType == DagElementType.STATEFUL) {

            this.stateTransferPlan = stateTransferPlan;
            this.hasBootStrapState = true;

        }else{
            throw new EdgeStreamReconfigurationException("You are trying to add a StateTransferPlan to a: " + DagElementType.STATELESS + " Individual_Task. This is not allowed, check your logic." , new Throwable());
        }
    }

    public boolean hasBootstrapState(){

        return this.hasBootStrapState;

    }


    public String getTopology_ID() {
        return Topology_ID;
    }

    public String getIndividual_Task_ID() {
        return Individual_Task_ID;
    }

    public String getTask_Manager_ID() {
        return Task_Manager_ID;
    }

    public String getHost_Name() {
        return Host_Name;
    }

    public String getOperator_ID() {
        return Operator_ID;
    }

    public String getOperator_Type() {
        return Operator_Type;
    }

    public String getOperator_Input_Type() {
        return Operator_Input_Type;
    }

    public String getOperator_Path() {
        return Operator_Path;
    }

    public String getOperator_Mode() {
        return Operator_Mode;
    }

    public String getHost_Address() {
        return Host_Address;
    }

    public String getHost_Port() {
        return Host_Port;
    }

    public String getMessage_Routing_Type() {
        return Message_Routing_Type;
    }

    public List<String> getOperator_Predecessor_IDs() {
        return Operator_Predecessor_IDs;
    }

    public List<String> getOperator_Successor_IDs() {
        return Operator_Successor_IDs;
    }

    public long getJob_Submission_Time() {
        return Job_Submission_Time;
    }

    public String getDeploymentMethod() {
        return deploymentMethod;
    }

    public void setDeploymentMethod(String deploymentMethod) {
        this.deploymentMethod = deploymentMethod;
    }

    public String getPrebuiltOperatorJarFileName() {
        return prebuiltOperatorJarFileName;
    }

    public void setPrebuiltOperatorJarFileName(String prebuiltOperatorJarFileName) {
        this.prebuiltOperatorJarFileName = prebuiltOperatorJarFileName;
    }

}
