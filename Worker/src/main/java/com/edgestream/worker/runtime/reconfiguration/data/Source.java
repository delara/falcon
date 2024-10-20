package com.edgestream.worker.runtime.reconfiguration.data;

public class Source {
    private String resourceId;
    private String operatorId;
    private long tuplesToProcess;

    public Source(String resourceId, String operatorId, long tuplesToProcess) {
        this.resourceId = resourceId;
        this.operatorId = operatorId;
        this.tuplesToProcess = tuplesToProcess;
    }

    public Source(String resourceId, String operatorId) {
        this.resourceId = resourceId;
        this.operatorId = operatorId;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public long getTuplesToProcess() {
        return tuplesToProcess;
    }

    public void setTuplesToProcess(long tuplesToProcess) {
        this.tuplesToProcess = tuplesToProcess;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public void setOperatorId(String operatorId) {
        this.operatorId = operatorId;
    }


}
