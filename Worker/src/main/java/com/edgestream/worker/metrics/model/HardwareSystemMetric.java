package com.edgestream.worker.metrics.model;


import java.lang.reflect.Field;

public class HardwareSystemMetric implements Metric {

    private final String timeStamp;
    private final String taskManagerID;
    private final String host_cpu_model;
    private final String host_cpu_number_of_cores;
    private final String MemAvailable;
    private final String MemTotal;
    private final String MemFree;
    private final String platform;
    private final String hostName;


    public HardwareSystemMetric(String timeStamp, String taskManagerID, String cpu_model, String cpu_number_of_cores, String MemAvailable,String MemTotal, String MemFree, String platform, String hostName) {
        this.timeStamp = timeStamp;
        this.taskManagerID = taskManagerID;
        this.host_cpu_model = cpu_model;
        this.host_cpu_number_of_cores = cpu_number_of_cores;
        this.MemAvailable = MemAvailable;
        this.MemTotal = MemTotal;
        this.MemFree = MemFree;
        this.platform = platform;
        this.hostName = hostName;
    }

    public String getHostName() {
        return hostName;
    }

    public String getPlatform() {
        return platform;
    }


    public String getTimeStamp() {
        return timeStamp;
    }

    public String getTaskManagerID() {
        return taskManagerID;
    }

    public String getCpu_model() {
        return host_cpu_model;
    }

    public String getCpu_number_of_cores() {
        return host_cpu_number_of_cores;
    }

    public String getMemAvailable() {
        return MemAvailable;
    }

    public String getMemTotal() {
        return MemTotal;
    }

    public String getMemFree() {
        return MemFree;
    }

    public boolean anyFieldIsEmpty(){

        return getMemAvailable() == null || getMemTotal() == null || getMemFree() == null || getCpu_number_of_cores() == null || getCpu_model() == null;
    }


    public String toString() {

        System.out.println("------------Hardware System Metric Details----------------");

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
                +host_cpu_model
                +";"
                +host_cpu_number_of_cores
                +";"
                +MemAvailable
                +";"
                +MemTotal
                +";"
                +MemFree
                +";"
                +platform
                +";"
                +hostName
                ;

        return tuple;


    }
}
