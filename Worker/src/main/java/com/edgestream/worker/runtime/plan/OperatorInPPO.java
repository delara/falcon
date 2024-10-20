package com.edgestream.worker.runtime.plan;

import com.edgestream.worker.runtime.reconfiguration.common.DagElementType;
import com.edgestream.worker.runtime.reconfiguration.exception.EdgeStreamReconfigurationException;
import com.edgestream.worker.runtime.reconfiguration.state.StateTransferPlan;

import java.util.ArrayList;
import java.util.List;

public class OperatorInPPO {
    private final String Operator_ID;
    private final String Type;
    private final String Input_Type;
    private final String Filepath;
    private final String Mode;
    private final ArrayList<String> Host_names;
    private final String Routing_Type;
    private final List<String> Predecessor_IDs;
    private final List<String> Successor_IDs;
    private final DagElementType dagElementType;
    private StateTransferPlan stateTransferPlan;
    private boolean hasBootStrapState = false;

    public OperatorInPPO(String operator_ID, String type, String input_Type, String filepath, String mode, ArrayList<String> host_names, String routing_Type, List<String> predecessor_IDs, List<String> successor_IDs, DagElementType dagElementType) {
        Operator_ID = operator_ID;
        Type = type;
        Input_Type = input_Type;
        Filepath = filepath;
        Mode = mode;
        Host_names = host_names;
        Routing_Type = routing_Type;
        Predecessor_IDs = predecessor_IDs;
        Successor_IDs = successor_IDs;
        this.dagElementType = dagElementType;
    }


    public StateTransferPlan getStateTransferPlan() {
        return stateTransferPlan;
    }

    public void addStateTransferPlan(StateTransferPlan stateTransferPlan){

        if (this.dagElementType == DagElementType.STATEFUL) {

            this.stateTransferPlan = stateTransferPlan;
            this.hasBootStrapState = true;

        }else{
            throw new EdgeStreamReconfigurationException("You are trying to add a StateTransferPlan to a: " + DagElementType.STATELESS + "OperatorPPO. This is not allowed, check your logic." , new Throwable());
        }
    }


    public boolean hasBootstrapState(){

        return this.hasBootStrapState;

    }


    public boolean isStateful(){
        return dagElementType == DagElementType.STATEFUL;

    }



    public String getOperator_ID() {
        return Operator_ID;
    }

    public String getType() {
        return Type;
    }

    public String getInput_Type() {
        return Input_Type;
    }

    public ArrayList<String> getHost_names() {
        return Host_names;
    }

    public String getRouting_Type() {
        return Routing_Type;
    }

    public List<String> getPredecessor_IDs() {
        return Predecessor_IDs;
    }

    public List<String> getSuccessor_IDs() {
        return Successor_IDs;
    }

    public String getFilepath() {
        return Filepath;
    }

    public String getMode() {
        return Mode;
    }
}
