package com.edgestream.worker.runtime.task.model;

import com.edgestream.worker.common.Tuple;
import com.edgestream.worker.operator.Operator;

import java.io.IOException;

public class Task {

    String taskID;
    Operator operator;

    public Task(String taskID, Operator operator) {

        this.taskID = taskID;
        this.operator = operator;

    }


    /***
     * For use when you don't yet know the {@link Operator} you want to run yet.
     * @param taskID
     */
    public Task(String taskID) {
        this.taskID = taskID;
    }

    public void setOperator(Operator operator) {

        this.operator = operator;
    }

    public void resetBoundOperator(){
        this.operator.reset();
    }

    public void enableBoundOperatorWarmupPhase(){
        this.operator.enableWarmUpPhase();
    }

    public void disableBoundOperatorWarmupPhase(){
        this.operator.disableWarmUpPhase();

    }

    public void enableBoundOperatorEmitter(){
        this.operator.enableEmitter();
    }


    public void disableBoundOperatorEmitter(){
        this.operator.disableEmitter();
    }

    public boolean isOperatorInWarmUpPhase(){
        return this.operator.isInWarmUpPhase();
    }

    public void processIncomingElement(Tuple tuple, String tupleID, String tupleInternalID, String producerId, String timeStamp, String tupleOrigin, long inputEventSize, String inputKey) throws IOException {
        //System.out.println("[Task] : Received a tuple, forwarding to the operator!");
        operator.processElement(tuple, tupleID, tupleInternalID, producerId, timeStamp, tupleOrigin, inputKey);
    }
}
