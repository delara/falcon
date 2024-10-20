package com.edgestream.worker.metrics.model;


import java.lang.reflect.Field;

public class ApplicationEdgeMetric implements Metric {

    private final String timeStamp;
    private final String sequence_id;
    private final String topology_id;
    private final String node_id;
    private final String avg_transferring_time;
    private final String min_transferring_time;
    private final String max_transferring_time;
    private final String std_transferring_time;
    private final String median_transferring_time;
    private final String transferring_size;
    private final String input_rate;
    private final String operator_id;
    private final String previous_operator_id;


    public ApplicationEdgeMetric(String timeStamp,String sequence_id, String topology_id, String node_id, String avg_transferring_time, String min_transferring_time, String max_transferring_time, String std_transferring_time, String median_transferring_time, String transferring_size, String input_rate, String operator_id, String previous_operator_id) {
        this.timeStamp =timeStamp;
        this.sequence_id = sequence_id;
        this.topology_id = topology_id;
        this.node_id = node_id;
        this.avg_transferring_time = avg_transferring_time;

        this.min_transferring_time = min_transferring_time;
        this.max_transferring_time = max_transferring_time;
        this.std_transferring_time = std_transferring_time;
        this.median_transferring_time = median_transferring_time;

        this.transferring_size = transferring_size;
        this.input_rate = input_rate;
        this.operator_id = operator_id;
        this.previous_operator_id = previous_operator_id;

    }





    public String toString() {

        System.out.println("------------Application Edge Metric Details---------------");

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

    public String getAvgTransferring_time() {
        return avg_transferring_time;
    }

    public String getMinTransferring_time() {
        return min_transferring_time;
    }

    public String getMaxTransferring_time() {
        return max_transferring_time;
    }

    public String getStdTransferring_time() {
        return std_transferring_time;
    }

    public String getMedianTransferring_time() {
        return median_transferring_time;
    }

    public String getTransferring_size() {
        return transferring_size;
    }

    public String getInput_rate() {
        return input_rate;
    }

    public String getOperator_id() {
        return operator_id;
    }

    public String getPrevious_operator_id() {
        return previous_operator_id;
    }

    @Override
    public String toTuple() {
        String tuple =
                timeStamp
                +";"
                +sequence_id
                +";"
                +topology_id
                +";"
                +node_id
                +";"
                +avg_transferring_time
                +";"
                        +min_transferring_time
                        +";"
                        +max_transferring_time
                        +";"
                        +std_transferring_time
                        +";"
                        +median_transferring_time
                        +";"
                +transferring_size
                +";"
                +input_rate
                +";"
                +operator_id
                +";"
                +previous_operator_id
                ;
        return tuple;


    }
}
