package com.edgestream.worker.runtime.reconfiguration.data;

public class OperatorMap{
    private String resourceId;
    private String operatorId;
    private double tupleSecond;
    private String srcResourceId;
    private int replicaId;

    public OperatorMap(String[] args) {
        this.setResourceId(args[0].trim());
        this.setOperatorId(args[1].trim());
    }

    public OperatorMap(String resourceId, String operatorId) {
        this.resourceId = resourceId;
        this.operatorId = operatorId;
    }

    public OperatorMap(String resourceId, String operatorId, double tupleSecond, String srcResourceId, int replicaId) {
        this.resourceId = resourceId;
        this.operatorId = operatorId;
        this.tupleSecond = tupleSecond;
        this.srcResourceId = srcResourceId;
        this.replicaId = replicaId;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public double getTupleSecond() {
        return tupleSecond;
    }

    public void setTupleSecond(double tupleSecond) {
        this.tupleSecond = tupleSecond;
    }

    public String getSrcResourceId() {
        return srcResourceId;
    }

    public void setSrcResourceId(String srcResourceId) {
        this.srcResourceId = srcResourceId;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public void setReplicaId(int replicaId) {
        this.replicaId = replicaId;
    }


    public void setOperatorId(String operatorId) {
        this.operatorId = operatorId;
    }

    public String toTuple() {
        String tuple =
                        resourceId
                        + ";"
                        + operatorId;

        return tuple;
    }

}
