package com.edgestream.worker.runtime.reconfiguration.data;

public class Edge {
    private String operator_id;
    private String child_id;

    public Edge(String[] args) {
        this.setOperator_id(args[0].trim());
        this.setChild_id(args[1].trim());
    }

    public String getOperator_id() {
        return operator_id;
    }

    public void setOperator_id(String operator_id) {
        this.operator_id = operator_id;
    }

    public String getChild_id() {
        return child_id;
    }

    public void setChild_id(String child_id) {
        this.child_id = child_id;
    }

    public String toTuple() {
        String tuple =
                        operator_id
                        + ";"
                        + child_id;

        return tuple;
    }

}
