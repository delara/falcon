package com.edgestream.worker.metrics.model;


import java.lang.reflect.Field;

public class StateManagementMetric implements Metric {

    private final String timeStamp;
    private final String sequence_id;
    private final String topology_id;
    private final String node_id;
    private final String operator_id;

    private String operationType;
    private String keys;
    private String sizeBytes;

    private String items;
    private String duration;


    public StateManagementMetric(String timeStamp, String sequence_id, String topology_id, String node_id, String operator_id, String operationType, String keys, String sizeBytes, String items, String duration) {
        this.timeStamp = timeStamp;
        this.sequence_id = sequence_id;
        this.topology_id = topology_id;
        this.node_id = node_id;
        this.operator_id = operator_id;
        this.operationType = operationType;
        this.keys = keys;
        this.sizeBytes = sizeBytes;
        this.items = items;
        this.duration = duration;
    }


    public String toString() {

        System.out.println("------------State Management Metric Details---------------");

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

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public String getKeys() {
        return keys;
    }

    public void setKeys(String keys) {
        this.keys = keys;
    }

    public String getSizeBytes() {
        return sizeBytes;
    }

    public void setSizeBytes(String sizeBytes) {
        this.sizeBytes = sizeBytes;
    }

    public String getItems() {
        return items;
    }

    public void setItems(String items) {
        this.items = items;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
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
                        + operationType
                        + ";"
                        + keys
                        + ";"
                        + sizeBytes
                        + ";"
                        + items
                        + ";"
                        + duration;

        return tuple;


    }
}
