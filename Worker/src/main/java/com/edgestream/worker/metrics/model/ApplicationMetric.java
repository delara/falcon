package com.edgestream.worker.metrics.model;

import java.lang.reflect.Field;
import java.util.ArrayList;

public class ApplicationMetric implements Metric {

    private final String timeStamp;
    private final String node_id;
    private final String avg_latency_in_ms;
    private final String min_latency_in_ms;
    private final String max_latency_in_ms;
    private final String std_latency_in_ms;
    private final String median_latency_in_ms;

    private final String operator_id;
    private final String sequence_id;
    private final String throughput;
    private final String topology_id;

    private final String input_rate;
    private final String input_msg_size;
    private final String output_msg_size;
    private final String processing_time_ns;

    private final String batch_size;
    private final String buffer_consumer_size;
    private final String buffer_producer_size;

    private final ArrayList<String> tupleOrigin;




    public ApplicationMetric(String timeStamp, String node_id, String avg_latency_in_ms, String min_latency_in_ms, String max_latency_in_ms, String std_latency_in_ms,  String median_latency_in_ms, String operator_id, String sequence_id , String throughput, String topology_id, String input_rate,String input_msg_size,String output_msg_size ,String processing_time_ns, String batch_size, String buffer_consumer_size, String buffer_producer_size, ArrayList<String> tupleOrigin) {

        this.timeStamp = timeStamp;
        this.node_id = node_id;
        this.avg_latency_in_ms = avg_latency_in_ms;
        this.min_latency_in_ms=min_latency_in_ms;
        this.max_latency_in_ms=max_latency_in_ms;
        this.std_latency_in_ms=std_latency_in_ms;
        this.median_latency_in_ms=median_latency_in_ms;

        this.operator_id = operator_id;
        this.sequence_id = sequence_id;
        this.throughput = throughput;
        this.topology_id = topology_id;
        this.input_rate = input_rate;
        this.input_msg_size = input_msg_size;
        this.output_msg_size =output_msg_size;
        this.processing_time_ns = processing_time_ns;
        this.batch_size = batch_size;
        this.buffer_consumer_size = buffer_consumer_size;
        this.buffer_producer_size = buffer_producer_size;
        this.tupleOrigin = tupleOrigin;
    }





    public String getTimeStamp() {
        return timeStamp;
    }

    public String getNode_id() {
        return node_id;
    }

    public String getAvgLatency_in_ms() {
        return avg_latency_in_ms;
    }

    public String getMinLatency_in_ms() {
        return min_latency_in_ms;
    }

    public String getMaxLatency_in_ms() {
        return max_latency_in_ms;
    }

    public String getStdLatency_in_ms() {
        return std_latency_in_ms;
    }

    public String getMedianLatency_in_ms() {
        return median_latency_in_ms;
    }

    public String getOperator_id() {
        return operator_id;
    }

    public String getSequence_id() {
        return sequence_id;
    }

    public String getThroughput() {
        return throughput;
    }

    public String getTopology_id() {
        return topology_id;
    }


    public String getInput_rate() {
        return input_rate;
    }

    public String getInput_msg_size() {
        return input_msg_size;
    }

    public String getOutput_msg_size() {
        return output_msg_size;
    }

    public String getProcessing_time_ns() {
        return processing_time_ns;
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

    public ArrayList<String> getTupleOrigin() {
        return tupleOrigin;
    }

    public String getTupleOriginListAsString(){

        StringBuffer sb = new StringBuffer();

        for (String s : getTupleOrigin()) {
            sb.append(s);
            sb.append(" ");
        }
        return sb.toString();

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
                +avg_latency_in_ms
                +";"
                +min_latency_in_ms
                +";"
                +max_latency_in_ms
                +";"
                +std_latency_in_ms
                +";"
                +median_latency_in_ms
                +";"
                +operator_id
                +";"
                +sequence_id
                +";"
                +throughput
                +";"
                +topology_id
                +";"
                +input_rate
                +";"
                +input_msg_size
                +";"
                +output_msg_size
                +";"
                +processing_time_ns
                +";"
                +batch_size
                +";"
                +buffer_consumer_size
                +";"
                +buffer_producer_size
                +";"
                +getTupleOriginListAsString()
                ;

        return tuple;

    }
}
