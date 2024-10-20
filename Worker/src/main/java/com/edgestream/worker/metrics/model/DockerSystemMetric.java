package com.edgestream.worker.metrics.model;

import java.lang.reflect.Field;

public class DockerSystemMetric implements Metric {

    private final String timeStamp;
    private final String taskManagerID;
    private final String operator_ID;
    private final String container_id;
    private final String container_name;
    private final String container_cpu_utilization;
    private final String container_allocated_cpu_core_count;
    private final String container_memory_usage;
    private final String container_memory_limit;



    public DockerSystemMetric(String timeStamp, String taskManagerID, String operator_ID, String container_id, String container_name, String container_cpu_utilization ,String container_allocated_cpu_core_count, String container_memory_usage, String container_memory_limit) {
        this.timeStamp = timeStamp;
        this.taskManagerID = taskManagerID;
        this.operator_ID = operator_ID;
        this.container_id = container_id;
        this.container_name = container_name;
        this.container_cpu_utilization = container_cpu_utilization;
        this.container_allocated_cpu_core_count = container_allocated_cpu_core_count;
        this.container_memory_usage = container_memory_usage;
        this.container_memory_limit = container_memory_limit;

    }

    public String getCpu_core_count() {
        return container_allocated_cpu_core_count;
    }


    public String getTaskManagerID() {
        return taskManagerID;
    }

    public String getOperator_ID() {
        return operator_ID;
    }


    public String getTimeStamp() {
        return timeStamp;
    }

    public String getDockerContainer_id() {
        return container_id;
    }

    public String getDockerContainer_name() {
        return container_name;
    }



    public String getCpu_utilization() {
        return container_cpu_utilization;
    }

    public String getMemory_usage() {
        return container_memory_usage;
    }

    public String getMemory_limit() {
        return container_memory_limit;
    }


    public String toString() {

        System.out.println("------------Docker Metric Details---------------");

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
                +taskManagerID
                +";"
                +operator_ID
                +";"
                +container_id
                +";"
                +container_name
                +";"
                +container_cpu_utilization
                +";"
                +container_allocated_cpu_core_count
                +";"
                +container_memory_usage
                +";"
                +container_memory_limit
                ;

        return tuple;
    }
}
