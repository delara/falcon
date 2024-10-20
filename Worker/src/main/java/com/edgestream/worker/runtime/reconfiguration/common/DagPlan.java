package com.edgestream.worker.runtime.reconfiguration.common;

import com.edgestream.worker.common.OperatorIDFormatter;
import com.edgestream.worker.operator.OperatorID;
import com.edgestream.worker.runtime.reconfiguration.state.StateTransferPlan;
import com.edgestream.worker.runtime.scheduler.ScheduledSlot;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;

public class DagPlan implements Serializable {

    private final ArrayList<ImmutablePair> dagElements = new ArrayList<>();
    private final HashMap<ImmutablePair,DagElementType> elementTypeMapping = new HashMap<>();
    private final HashMap<ImmutablePair, StateTransferPlan> elementStateTransferPlan = new HashMap<>();
    private final ArrayDeque<OperatorID> operatorIdInDagPlan = new ArrayDeque<>();
    private final HashMap<String, ScheduledSlot> slotMapping = new HashMap<>();


    public void addDagPlanElement(String operatorInputType, int replicaID,ScheduledSlot scheduledSlot){

        ImmutablePair immutablePair = new ImmutablePair(operatorInputType,replicaID);
        dagElements.add(immutablePair);
        elementTypeMapping.put(immutablePair,DagElementType.STATELESS);
        OperatorID operatorID = new OperatorID(OperatorIDFormatter.formatOperatorID(operatorInputType,String.valueOf(replicaID)));
        slotMapping.put(operatorID.getOperatorID_as_String(),scheduledSlot);

    }

    public void addStatefulDagPlanElement(String operatorInputType, int replicaID , StateTransferPlan stateTransferPlan,ScheduledSlot scheduledSlot){

        ImmutablePair immutablePair = new ImmutablePair(operatorInputType,replicaID);
        dagElements.add(immutablePair);
        elementTypeMapping.put(immutablePair,DagElementType.STATEFUL);
        elementStateTransferPlan.put(immutablePair,stateTransferPlan);
        OperatorID operatorID = new OperatorID(OperatorIDFormatter.formatOperatorID(operatorInputType,String.valueOf(replicaID)));
        slotMapping.put(operatorID.getOperatorID_as_String(),scheduledSlot);

    }


    public ScheduledSlot getScheduledSlot(OperatorID operatorID){
        return slotMapping.get(operatorID.getOperatorID_as_String());
    }

    public void addOperatorID(OperatorID operatorID){

        operatorIdInDagPlan.add(operatorID);
    }

    public ArrayDeque<OperatorID> getOperatorIDsInDagPlan() {
        return operatorIdInDagPlan;
    }

    public boolean isDagElementStateful(ImmutablePair immutablePair){

        return elementTypeMapping.get(immutablePair) == DagElementType.STATEFUL;

    }

    public StateTransferPlan getElementStateTransferPlan(ImmutablePair immutablePair){

        return elementStateTransferPlan.get(immutablePair);
    }




    public ArrayList<ImmutablePair> getDagElements(){

        return  dagElements;
    }

    public int getDagPlanSize(){

        return  this.dagElements.size();
    }

    public String getLastOperatorInDagPlan(){

        String lastOperatorInPlan = null;

        int size = this.dagElements.size();

        if(size > 1) {
            lastOperatorInPlan = String.valueOf(this.dagElements.get(size - 1).left);
        }else{
            lastOperatorInPlan = String.valueOf(this.dagElements.get(0).left);
        }

        return lastOperatorInPlan;

    }
}
