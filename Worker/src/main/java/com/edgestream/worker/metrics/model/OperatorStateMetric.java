package com.edgestream.worker.metrics.model;

import com.edgestream.worker.metrics.common.StateCollection;

import java.lang.reflect.Field;


public class OperatorStateMetric implements Metric  {


    private final String timeStamp;
    private final String sequence_id;
    private final String message_type;
    private final String topology_id;
    private final String node_id;
    private final String operator_id;
    private final String tupleType;
    private final String batch_size;
    private final String buffer_consumer_size;
    private final String buffer_producer_size;
    private final StateCollection state_collection;

    public OperatorStateMetric(String timeStamp, String sequence_id, String message_type, String topology_id, String node_id, String operator_id
            , String tupleType, String batch_size, String buffer_consumer_size, String buffer_producer_size, StateCollection state_collection) {
        this.timeStamp = timeStamp;
        this.sequence_id = sequence_id;
        this.message_type = message_type;
        this.topology_id = topology_id;
        this.node_id = node_id;
        this.operator_id = operator_id;
        this.tupleType = tupleType;
        this.batch_size = batch_size;
        this.buffer_consumer_size = buffer_consumer_size;
        this.buffer_producer_size = buffer_producer_size;
        this.state_collection = state_collection;
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

    public String getBatch_size() {
        return batch_size;
    }

    public String getBuffer_consumer_size() {
        return buffer_consumer_size;
    }

    public String getBuffer_producer_size() {
        return buffer_producer_size;
    }

    public StateCollection getState_collection() {
        return state_collection;
    }

    public String getMessage_type() {
        return message_type;
    }

    public String getTupleType() {
        return tupleType;
    }

    public String toString() {

        System.out.println("------------Application Metric Details---------------");

        StringBuilder result = new StringBuilder();
        String newLine = System.getProperty("line.separator");

        result.append( this.getClass().getName() );
        result.append( " Object {" );
        result.append(newLine);

        //determine fields declared in this class only (no fields of superclass)
        Field[] fields = this.getClass().getDeclaredFields();

        //print field names paired with their values
        for ( Field field : fields  ) {
            result.append("  ");
            try {
                result.append( field.getName() );
                result.append(": ");
                //requires access to private field:
                result.append( field.get(this) );
            } catch ( IllegalAccessException ex ) {
                System.out.println(ex);
            }
            result.append(newLine);
        }
        result.append("}");

        return result.toString();
    }

    @Override
    public String toTuple() {
        String tuple =  timeStamp
                +";"
                +node_id
                +";"
                +operator_id
                +";"
                +sequence_id
                +";"
                +topology_id
                +";"
                +batch_size
                +";"
                +buffer_consumer_size
                +";"
                +buffer_producer_size
                ;

        return tuple;

    }
}
