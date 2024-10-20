package com.edgestream.worker.runtime.plan;

import com.edgestream.worker.runtime.reconfiguration.common.DagElementType;
import com.edgestream.worker.runtime.reconfiguration.exception.EdgeStreamReconfigurationException;
import com.edgestream.worker.runtime.reconfiguration.state.StateTransferPlan;

import java.util.List;

public class Operator_Src {
    private final String Operator_ID;
    private final String Operator_Type;
    private final String Operator_Input_Type;
    private final String Message_Routing_Type;
    private final String Operator_Path;
    private final String Operator_Mode;
    private final List<String> Predecessor_IDs;
    private final List<String> Successor_IDs;
    private DagElementType dagElementType;
    private StateTransferPlan stateTransferPlan;
    private boolean hasBootStrapState = false;


    public Operator_Src(String operator_ID, String operator_Type, String operator_Input_Type,
                        String message_Routing_Type, String operator_Path, String operator_Mode,
                        List<String> predecessor_IDs, List<String> successor_IDs, DagElementType dagElementType) {
        Operator_ID = operator_ID;
        Operator_Type = operator_Type;
        Operator_Input_Type = operator_Input_Type;
        Message_Routing_Type = message_Routing_Type;
        Operator_Path = operator_Path;
        Operator_Mode = operator_Mode;
        Predecessor_IDs = predecessor_IDs;
        Successor_IDs = successor_IDs;
        this.dagElementType = dagElementType;
    }

    public StateTransferPlan getStateTransferPlan() {
        return stateTransferPlan;
    }


    public void addStateTransferPlan(StateTransferPlan stateTransferPlan){

            this.stateTransferPlan = stateTransferPlan;
            this.dagElementType = DagElementType.STATEFUL;
            this.hasBootStrapState = true;
    }

    public DagElementType getDagElementType() {
        return dagElementType;
    }

    public boolean hasBootstrapState(){

        return this.hasBootStrapState;

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

    public String getMessage_Routing_Type() {
        return Message_Routing_Type;
    }

    public String getOperator_Path() {
        return Operator_Path;
    }

    public String getOperator_Mode() {
        return Operator_Mode;
    }

    public List<String> getPredecessor_IDs() {
        return Predecessor_IDs;
    }

    public List<String> getSuccessor_IDs() {
        return Successor_IDs;
    }
}
