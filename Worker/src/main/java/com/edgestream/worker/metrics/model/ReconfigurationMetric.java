package com.edgestream.worker.metrics.model;


import java.lang.reflect.Field;

public class ReconfigurationMetric implements Metric {

    private final String timeStamp;
    private final String sequence_id;
    private final String topology_id;
    private final String node_id;
    private final String operator_id;
    private String operation;
    private String sourceCreationTimestamp;


    public ReconfigurationMetric(String timeStamp, String sequence_id, String topology_id, String node_id, String operator_id, String operation, String sourceCreationTimestamp) {
        this.timeStamp = timeStamp;
        this.sequence_id = sequence_id;
        this.topology_id = topology_id;
        this.node_id = node_id;
        this.operator_id = operator_id;
        this.operation = operation;
        this.sourceCreationTimestamp = sourceCreationTimestamp;

    }


    public String toString() {

        System.out.println("------------Reconfiguration Metric Details---------------");

        StringBuilder result = new StringBuilder();
        String newLine = System.getProperty("line.separator");

        result.append(this.getClass().getName());
        result.append(" Object {");
        result.append(newLine);

        //determine fields declared in this class only (no fields of superclass)
        Field[] fields = this.getClass().getDeclaredFields();

        //print field names paired with their values
        for (Field field : fields) {
            result.append("  ");
            try {
                result.append(field.getName());
                result.append(": ");
                //requires access to private field:
                result.append(field.get(this));
            } catch (IllegalAccessException ex) {
                System.out.println(ex);
            }
            result.append(newLine);
        }
        result.append("}");

        return result.toString();
    }


    public String getTimeStamp() {
        return timeStamp;
    }

    public String getSequence_id() {
        return sequence_id;
    }

    public String getTopology_id() {
        return topology_id;
    }

    public String getNode_id() {
        return node_id;
    }

    public String getOperator_id() {
        return operator_id;
    }


    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getSourceCreationTimestamp() {
        return sourceCreationTimestamp;
    }

    public void setSourceCreationTimestamp(String sourceCreationTimestamp) {
        this.sourceCreationTimestamp = sourceCreationTimestamp;
    }

    @Override
    public String toTuple() {
        String tuple =
                timeStamp
                        + ";"
                        + sequence_id
                        + ";"
                        + topology_id
                        + ";"
                        + node_id
                        + ";"
                        + operator_id
                        + ";"
                        + operation
                        + ";"
                        + sourceCreationTimestamp
                        ;
        return tuple;


    }
}
