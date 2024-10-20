package com.edgestream.worker.runtime.reconfiguration.data;

import com.edgestream.worker.metrics.common.StateCollection;

public class OperatorPartitionKeyStats {

    private String taskManagerID;
    private String topologyID;
    private String operatorID;
    private String timestamp;
    private final StateCollection stateCollection;

    public OperatorPartitionKeyStats(String taskManagerID, String topologyID, String operatorID, String timestamp, StateCollection stateCollection) {

        this.taskManagerID = taskManagerID;
        this.topologyID = topologyID;
        this.operatorID = operatorID;
        this.timestamp = timestamp;
        this.stateCollection = stateCollection;

    }


    public StateCollection getStateCollectionObject() {
        return stateCollection;
    }

    public String getTaskManagerID() {
        return taskManagerID;
    }

    public void setTaskManagerID(String taskManagerID) {
        this.taskManagerID = taskManagerID;
    }

    public String getTopologyID() {
        return topologyID;
    }

    public void setTopologyID(String topologyID) {
        this.topologyID = topologyID;
    }

    public String getOperatorID() {
        return operatorID;
    }

    public void setOperatorID(String operatorID) {
        this.operatorID = operatorID;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String toTuple() {
        String tuple =
                        taskManagerID
                        + ";"
                        + topologyID
                        + ";"
                        + operatorID;


        return tuple;
    }


}
